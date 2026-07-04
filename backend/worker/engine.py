import asyncio
import logging
import socket
import sys
import uuid
import signal
from datetime import datetime, timezone

from sqlalchemy import select, update, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.database import AsyncSessionLocal
from app.models.worker import Worker, WorkerStatus
from app.models.worker_heartbeat import WorkerHeartbeat
from app.models.queue import Queue
from app.models.job import Job, JobStatus
from app.models.job_execution import JobExecution, ExecutionStatus
from worker.executor import run_executor

# Setup standard logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("worker.engine")

import os

# Configuration Constants
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", 2))
HEARTBEAT_INTERVAL_SECONDS = int(os.getenv("HEARTBEAT_INTERVAL_SECONDS", 10))
SHUTDOWN_TIMEOUT_SECONDS = int(os.getenv("SHUTDOWN_TIMEOUT_SECONDS", 15))


class WorkerEngine:
    def __init__(self):
        self.worker_id = uuid.uuid4()
        self.hostname = socket.gethostname()
        self.shutting_down = False
        self.in_flight = set()
        self.loop = None

    def handle_signal(self, sig):
        logger.info(f"Received signal {sig}. Initiating graceful shutdown...")
        self.shutting_down = True
        if self.loop:
            # Schedule the shutdown coroutine on the running loop
            self.loop.create_task(self.shutdown())

    async def register_worker(self):
        """Registers the worker as ONLINE in the database."""
        async with AsyncSessionLocal() as session:
            async with session.begin():
                worker = Worker(
                    id=self.worker_id,
                    hostname=self.hostname,
                    status=WorkerStatus.ONLINE
                )
                session.add(worker)
            logger.info(f"Registered worker {self.worker_id} on host {self.hostname}")

    async def update_status(self, status: WorkerStatus):
        """Helper to update worker status in the database."""
        try:
            async with AsyncSessionLocal() as session:
                async with session.begin():
                    await session.execute(
                        update(Worker)
                        .where(Worker.id == self.worker_id)
                        .values(status=status, updated_at=datetime.now(timezone.utc))
                    )
            logger.info(f"Worker status updated to {status}")
        except Exception as e:
            logger.error(f"Failed to update worker status to {status}: {e}")

    async def send_heartbeat(self) -> bool:
        """Sends heartbeat. Returns False if the worker status is no longer ONLINE."""
        async with AsyncSessionLocal() as session:
            # 1. Verify worker status is still ONLINE
            worker_result = await session.execute(
                select(Worker.status).where(Worker.id == self.worker_id)
            )
            status = worker_result.scalar_one_or_none()
            if status != WorkerStatus.ONLINE:
                logger.warning(f"Heartbeat detected status is {status} (not ONLINE). Draining worker.")
                return False

            # 2. Upsert heartbeat
            stmt = pg_insert(WorkerHeartbeat).values(
                id=uuid.uuid4(),
                worker_id=self.worker_id,
                last_seen=datetime.now(timezone.utc)
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=[WorkerHeartbeat.worker_id],
                set_={"last_seen": datetime.now(timezone.utc)}
            )
            await session.execute(stmt)
            await session.commit()
            return True

    async def claim_job(self, queue: Queue) -> tuple[uuid.UUID, uuid.UUID] | None:
        """Atomically locks, claims, and returns a job along with its execution ID."""
        claim_sql = text("""
            WITH running_count AS (
                SELECT COUNT(*) AS cnt
                FROM jobs
                WHERE queue_id = :queue_id
                  AND status = 'running'
            ),
            claimable AS (
                SELECT j.id, j.retry_count
                FROM jobs j, running_count rc
                WHERE j.queue_id = :queue_id
                  AND j.status IN ('queued', 'scheduled')
                  AND j.run_at <= NOW()
                  AND rc.cnt < :concurrency_limit
                  AND NOT EXISTS (
                      SELECT 1
                      FROM job_dependencies jd
                      JOIN jobs dep ON jd.depends_on_job_id = dep.id
                      WHERE jd.job_id = j.id
                        AND dep.status != 'completed'
                  )
                ORDER BY j.run_at ASC
                LIMIT 1
                FOR UPDATE OF j SKIP LOCKED
            )
            UPDATE jobs
            SET status = 'running', updated_at = NOW()
            FROM claimable
            WHERE jobs.id = claimable.id
            RETURNING jobs.id, claimable.retry_count;
        """)

        async with AsyncSessionLocal() as session:
            # We run the claim and the execution creation in the SAME transaction
            async with session.begin():
                result = await session.execute(
                    claim_sql,
                    {
                        "queue_id": queue.id,
                        "concurrency_limit": queue.concurrency_limit
                    }
                )
                row = result.fetchone()
                if not row:
                    return None

                job_id, retry_count = row
                
                # Create corresponding JobExecution entry
                execution_id = uuid.uuid4()
                execution = JobExecution(
                    id=execution_id,
                    job_id=job_id,
                    worker_id=self.worker_id,
                    status=ExecutionStatus.RUNNING,
                    started_at=datetime.now(timezone.utc),
                    attempt_number=retry_count + 1
                )
                session.add(execution)
                
                return job_id, execution_id

    async def heartbeat_loop(self):
        """Periodically runs worker heartbeats."""
        logger.info("Heartbeat loop started.")
        while not self.shutting_down:
            try:
                alive = await self.send_heartbeat()
                if not alive:
                    self.shutting_down = True
                    await self.shutdown()
                    break
            except Exception as e:
                logger.error(f"Error in heartbeat loop: {e}")
            await asyncio.sleep(HEARTBEAT_INTERVAL_SECONDS)

    async def poll_loop(self):
        """Periodically polls active queues and triggers executions."""
        logger.info("Polling loop started.")
        while not self.shutting_down:
            try:
                # Fetch active (non-paused) queues sorted by priority (DESC)
                async with AsyncSessionLocal() as session:
                    queues_result = await session.execute(
                        select(Queue)
                        .where(Queue.paused == False)
                        .order_by(Queue.priority.desc(), Queue.created_at.asc())
                    )
                    active_queues = queues_result.scalars().all()

                for queue in active_queues:
                    if self.shutting_down:
                        break
                    
                    # Try claiming a job
                    claim = await self.claim_job(queue)
                    if claim:
                        job_id, execution_id = claim
                        # Run execution as a background task
                        task = asyncio.create_task(run_executor(job_id, execution_id, self.worker_id))
                        self.in_flight.add(task)
                        task.add_done_callback(self.in_flight.discard)

            except Exception as e:
                logger.error(f"Error in polling loop: {e}")
            
            await asyncio.sleep(POLL_INTERVAL_SECONDS)

    async def shutdown(self):
        """Gracefully shuts down the worker process."""
        if self.shutting_down:
            # Status update to draining
            await self.update_status(WorkerStatus.DRAINING)

            if self.in_flight:
                logger.info(f"Waiting for {len(self.in_flight)} in-flight jobs to complete (timeout: {SHUTDOWN_TIMEOUT_SECONDS}s)")
                # Wait for in-flight tasks to finish
                done, pending = await asyncio.wait(
                    self.in_flight,
                    timeout=SHUTDOWN_TIMEOUT_SECONDS
                )
                if pending:
                    logger.warning(f"{len(pending)} jobs did not finish within timeout. Cancelling.")
                    for task in pending:
                        task.cancel()
            
            # Status update to offline
            await self.update_status(WorkerStatus.OFFLINE)
            logger.info("Worker exit clean.")
            # Stop the loop and exit
            sys.exit(0)

    async def run(self):
        """Starts the worker engine loops."""
        self.loop = asyncio.get_running_loop()
        
        # Register SIGTERM / SIGINT handlers
        try:
            for sig in (signal.SIGTERM, signal.SIGINT):
                self.loop.add_signal_handler(sig, lambda s=sig: self.handle_signal(s))
        except NotImplementedError:
            # Fallback for Windows
            pass

        await self.register_worker()
        
        # Run poll and heartbeat loops concurrently
        try:
            await asyncio.gather(
                self.heartbeat_loop(),
                self.poll_loop()
            )
        except asyncio.CancelledError:
            pass
        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt received. Initiating graceful shutdown...")
            self.shutting_down = True
            await self.shutdown()


if __name__ == "__main__":
    engine = WorkerEngine()
    try:
        asyncio.run(engine.run())
    except KeyboardInterrupt:
        pass
