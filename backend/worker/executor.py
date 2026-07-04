import asyncio
import logging
import traceback
from datetime import datetime, timezone, timedelta
import uuid

from sqlalchemy import select, update, insert
from sqlalchemy.orm import joinedload
from croniter import croniter

from app.database import AsyncSessionLocal
from app.models.job import Job, JobStatus, JobType
from app.models.job_execution import JobExecution, ExecutionStatus
from app.models.job_log import JobLog, LogLevel
from app.models.queue import Queue
from app.models.retry_policy import RetryPolicy, RetryStrategy
from app.models.dead_letter_queue import DeadLetterQueue
from app.models.scheduled_job import ScheduledJob

logger = logging.getLogger("worker.executor")


async def log_to_db(session, execution_id: uuid.UUID, level: LogLevel, message: str):
    """Utility to insert a log entry for a job execution."""
    try:
        log_entry = JobLog(
            execution_id=execution_id,
            level=level,
            message=message
        )
        session.add(log_entry)
        await session.flush()
    except Exception as e:
        logger.error(f"Failed to write log to DB: {e}")


async def run_executor(job_id: uuid.UUID, execution_id: uuid.UUID, worker_id: uuid.UUID):
    """Executes the job, manages execution lifecycle, retries, and DLQ."""
    async with AsyncSessionLocal() as session:
        # Fetch job and its queue (along with retry policy)
        result = await session.execute(
            select(Job)
            .options(
                joinedload(Job.queue).joinedload(Queue.retry_policy),
                joinedload(Job.scheduled_job)
            )
            .where(Job.id == job_id)
        )
        job = result.scalar_one_or_none()

        if not job:
            logger.error(f"Job {job_id} not found in database.")
            return

        # Ensure execution entry exists and is fetched
        exec_result = await session.execute(
            select(JobExecution).where(JobExecution.id == execution_id)
        )
        execution = exec_result.scalar_one_or_none()
        if not execution:
            logger.error(f"Job execution {execution_id} not found.")
            return

        logger.info(f"Starting execution of job {job_id} (Attempt {execution.attempt_number})")
        await log_to_db(session, execution_id, LogLevel.INFO, f"Started job execution. Attempt {execution.attempt_number}")
        await session.commit()

        # Simulate execution based on payload
        payload = job.payload or {}
        duration = payload.get("duration", 0)
        should_fail = payload.get("fail", False) or (payload.get("task") == "fail")

        try:
            if duration > 0:
                await log_to_db(session, execution_id, LogLevel.INFO, f"Simulating work for {duration} seconds...")
                await session.commit()
                await asyncio.sleep(duration)

            if should_fail:
                raise Exception(payload.get("error_message", "Simulated job execution failure."))

            # Success path
            await log_to_db(session, execution_id, LogLevel.INFO, "Job completed successfully.")
            
            # Update execution
            execution.status = ExecutionStatus.COMPLETED
            execution.finished_at = datetime.now(timezone.utc)

            # Update job status
            if job.job_type == JobType.RECURRING and job.scheduled_job:
                # Re-schedule recurring job
                cron_expr = job.scheduled_job.cron_expr
                now = datetime.now(timezone.utc)
                iter = croniter(cron_expr, now)
                next_run = iter.get_next(datetime)

                job.status = JobStatus.SCHEDULED
                job.run_at = next_run
                job.retry_count = 0  # reset retries for next natural run
                job.scheduled_job.next_run_at = next_run
                
                await log_to_db(
                    session, 
                    execution_id, 
                    LogLevel.INFO, 
                    f"Recurring job rescheduled for {next_run.isoformat()} based on cron expression '{cron_expr}'."
                )
            else:
                # Optimistic concurrency check: make sure job status is still 'running'
                # (i.e. it hasn't been requeued by the reaper)
                update_result = await session.execute(
                    update(Job)
                    .where(Job.id == job_id, Job.status == JobStatus.RUNNING)
                    .values(status=JobStatus.COMPLETED, updated_at=datetime.now(timezone.utc))
                )
                if update_result.rowcount == 0:
                    logger.warning(f"Job {job_id} status was not 'running' (possibly requeued by reaper). Completion skipped.")
                    await log_to_db(session, execution_id, LogLevel.WARNING, "Job was requeued by reaper while executing.")

            await session.commit()
            logger.info(f"Job {job_id} finished successfully.")

        except Exception as exc:
            # Failure path
            error_msg = str(exc)
            tb = traceback.format_exc()
            logger.error(f"Job {job_id} failed: {error_msg}\n{tb}")
            
            await log_to_db(session, execution_id, LogLevel.ERROR, f"Job failed: {error_msg}")
            await log_to_db(session, execution_id, LogLevel.ERROR, f"Traceback:\n{tb}")

            # Update execution
            execution.status = ExecutionStatus.FAILED
            execution.finished_at = datetime.now(timezone.utc)

            # Check retry policy
            max_retries = job.max_retries
            if job.queue and job.queue.retry_policy:
                max_retries = job.queue.retry_policy.max_retries
                policy = job.queue.retry_policy
            else:
                policy = None

            # Incremented retry count
            new_retry_count = job.retry_count + 1
            job.retry_count = new_retry_count

            # Guarded optimistic update for jobs status
            if new_retry_count <= max_retries:
                # Calculate backoff delay
                delay_sec = 1.0
                if policy:
                    if policy.strategy == RetryStrategy.FIXED:
                        delay_sec = policy.base_delay_ms / 1000.0
                    elif policy.strategy == RetryStrategy.LINEAR:
                        delay_sec = (policy.base_delay_ms * new_retry_count) / 1000.0
                    elif policy.strategy == RetryStrategy.EXPONENTIAL:
                        delay_sec = (policy.base_delay_ms * (2 ** (new_retry_count - 1))) / 1000.0

                next_run_at = datetime.now(timezone.utc) + timedelta(seconds=delay_sec)
                
                # Check status was 'running' before update
                update_result = await session.execute(
                    update(Job)
                    .where(Job.id == job_id, Job.status == JobStatus.RUNNING)
                    .values(
                        status=JobStatus.SCHEDULED,
                        run_at=next_run_at,
                        retry_count=new_retry_count,
                        updated_at=datetime.now(timezone.utc)
                    )
                )
                if update_result.rowcount > 0:
                    await log_to_db(
                        session, 
                        execution_id, 
                        LogLevel.WARNING, 
                        f"Job failed. Retrying (Attempt {new_retry_count}/{max_retries}) in {delay_sec}s at {next_run_at.isoformat()}."
                    )
                else:
                    logger.warning(f"Job {job_id} was not in 'running' state during retry update.")
            else:
                # Move to Dead Letter Queue
                update_result = await session.execute(
                    update(Job)
                    .where(Job.id == job_id, Job.status == JobStatus.RUNNING)
                    .values(status=JobStatus.DEAD_LETTER, updated_at=datetime.now(timezone.utc))
                )
                if update_result.rowcount > 0:
                    dlq_entry = DeadLetterQueue(
                        job_id=job_id,
                        failure_reason=error_msg,
                        moved_at=datetime.now(timezone.utc)
                    )
                    session.add(dlq_entry)
                    await log_to_db(
                        session, 
                        execution_id, 
                        LogLevel.ERROR, 
                        f"Retries exhausted ({new_retry_count - 1}/{max_retries}). Moved to Dead Letter Queue."
                    )
                else:
                    logger.warning(f"Job {job_id} was not in 'running' state when moving to DLQ.")

            await session.commit()
