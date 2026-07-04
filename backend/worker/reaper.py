import asyncio
import logging
from datetime import datetime, timezone, timedelta
import uuid

from sqlalchemy import select, update
from sqlalchemy.orm import selectinload

from app.database import AsyncSessionLocal
from app.models.worker import Worker, WorkerStatus
from app.models.worker_heartbeat import WorkerHeartbeat
from app.models.job import Job, JobStatus
from app.models.job_execution import JobExecution, ExecutionStatus

logger = logging.getLogger("reaper")

import os

# Configurable constants (typically overridden via env variables)
REAPER_INTERVAL_SECONDS = int(os.getenv("REAPER_INTERVAL_SECONDS", 30))
STALE_THRESHOLD_SECONDS = int(os.getenv("STALE_THRESHOLD_SECONDS", 60))


from sqlalchemy.orm import joinedload
from app.models.queue import Queue
from app.models.retry_policy import RetryPolicy, RetryStrategy
from app.models.dead_letter_queue import DeadLetterQueue


async def reclaim_stalled_worker_jobs(session, worker_id: uuid.UUID):
    """Requeues running jobs for a stalled worker and marks their executions failed, applying retry limits & backoff."""
    # Find all running job executions for this worker along with the job and its queue/retry policy
    execs_result = await session.execute(
        select(JobExecution, Job)
        .join(Job, JobExecution.job_id == Job.id)
        .options(
            joinedload(Job.queue).joinedload(Queue.retry_policy)
        )
        .where(
            JobExecution.worker_id == worker_id,
            JobExecution.status == ExecutionStatus.RUNNING
        )
    )
    stalled_pairs = execs_result.all()

    for execution, job in stalled_pairs:
        new_retry_count = job.retry_count + 1
        
        # 1. Update the execution record to failed
        execution.status = ExecutionStatus.FAILED
        execution.finished_at = datetime.now(timezone.utc)

        # 2. Determine max retries & retry policy
        max_retries = job.max_retries
        policy = None
        if job.queue and job.queue.retry_policy:
            max_retries = job.queue.retry_policy.max_retries
            policy = job.queue.retry_policy

        # 3. Route to either Scheduled/Queued (retry) or Dead Letter Queue (exhausted)
        if new_retry_count <= max_retries:
            delay_sec = 1.0
            if policy:
                if policy.strategy == RetryStrategy.FIXED:
                    delay_sec = policy.base_delay_ms / 1000.0
                elif policy.strategy == RetryStrategy.LINEAR:
                    delay_sec = (policy.base_delay_ms * new_retry_count) / 1000.0
                elif policy.strategy == RetryStrategy.EXPONENTIAL:
                    delay_sec = (policy.base_delay_ms * (2 ** (new_retry_count - 1))) / 1000.0

            next_run_at = datetime.now(timezone.utc) + timedelta(seconds=delay_sec)
            job.status = JobStatus.SCHEDULED
            job.retry_count = new_retry_count
            job.run_at = next_run_at
            job.updated_at = datetime.now(timezone.utc)
            logger.info(f"Requeued job {job.id} (attempt {new_retry_count}/{max_retries}) with backoff delay of {delay_sec}s from stalled worker {worker_id}")
        else:
            job.status = JobStatus.DEAD_LETTER
            job.retry_count = new_retry_count
            job.updated_at = datetime.now(timezone.utc)

            # Move to Dead Letter Queue
            dlq_entry = DeadLetterQueue(
                job_id=job.id,
                failure_reason="Worker stalled or crashed during execution (max retries exceeded)",
                moved_at=datetime.now(timezone.utc)
            )
            session.add(dlq_entry)
            logger.warning(f"Moved job {job.id} to Dead Letter Queue (attempts exhausted: {new_retry_count}/{max_retries})")

    # 4. Mark worker as offline
    await session.execute(
        update(Worker)
        .where(Worker.id == worker_id)
        .values(status=WorkerStatus.OFFLINE, updated_at=datetime.now(timezone.utc))
    )


async def reap_once():
    """Runs a single pass of the reaper to identify and clean up stalled workers."""
    threshold_time = datetime.now(timezone.utc) - timedelta(seconds=STALE_THRESHOLD_SECONDS)

    async with AsyncSessionLocal() as session:
        # Find all workers marked online whose heartbeat is stale (or missing)
        stalled_workers_query = (
            select(Worker.id)
            .outerjoin(WorkerHeartbeat, Worker.id == WorkerHeartbeat.worker_id)
            .where(
                Worker.status == WorkerStatus.ONLINE,
                (WorkerHeartbeat.last_seen == None) | (WorkerHeartbeat.last_seen < threshold_time)
            )
        )
        result = await session.execute(stalled_workers_query)
        stalled_worker_ids = result.scalars().all()

        for worker_id in stalled_worker_ids:
            logger.warning(f"Worker {worker_id} identified as stalled (threshold: {STALE_THRESHOLD_SECONDS}s). Recovering jobs.")
            try:
                # Wrap all recovery updates for the worker in a single transaction
                async with session.begin_nested():
                    await reclaim_stalled_worker_jobs(session, worker_id)
                await session.commit()
                logger.info(f"Successfully recovered jobs and set worker {worker_id} offline.")
            except Exception as e:
                logger.exception(f"Error recovering stalled worker {worker_id}: {e}")
                await session.rollback()


async def run_reaper_loop():
    """Background loop that executes the reaper pass periodically."""
    logger.info("Reaper background task started.")
    while True:
        try:
            await reap_once()
        except Exception as e:
            logger.exception(f"Unexpected error in reaper loop: {e}")
        await asyncio.sleep(REAPER_INTERVAL_SECONDS)
