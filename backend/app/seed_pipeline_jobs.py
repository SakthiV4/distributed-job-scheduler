import asyncio
import logging
import uuid
import datetime
from sqlalchemy import select
from app.database import AsyncSessionLocal
from app.models.organization import Organization
from app.models.user import User
from app.models.project import Project
from app.models.queue import Queue
from app.models.job import Job, JobStatus
from app.models.worker import Worker, WorkerStatus
from app.models.worker_heartbeat import WorkerHeartbeat
from app.models.job_execution import JobExecution
from app.models.dead_letter_queue import DeadLetterQueue

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("scheduler.seed_pipeline")

async def seed_pipeline_jobs():
    logger.info("Starting pipeline jobs seeding...")
    async with AsyncSessionLocal() as session:
        async with session.begin():
            # Get the default project
            project_res = await session.execute(
                select(Project).where(Project.name == "Video Processing Cluster")
            )
            project = project_res.scalar_one_or_none()
            if not project:
                logger.error("Video Processing Cluster not found. Run base seed first.")
                return

            # Get the default queue
            queue_res = await session.execute(
                select(Queue).where(Queue.project_id == project.id).where(Queue.name == "default-queue")
            )
            queue = queue_res.scalar_one_or_none()
            if not queue:
                logger.error("default-queue not found. Run base seed first.")
                return

            # Ensure we have an active worker in the DB
            worker_res = await session.execute(select(Worker))
            worker = worker_res.scalars().first()
            if not worker:
                worker = Worker(
                    id=uuid.uuid4(),
                    hostname="seeded-worker-node-1",
                    status=WorkerStatus.ONLINE
                )
                session.add(worker)
                await session.flush()
                
            # Create or update worker heartbeat to keep it alive/online
            hb_res = await session.execute(select(WorkerHeartbeat).where(WorkerHeartbeat.worker_id == worker.id))
            hb = hb_res.scalar_one_or_none()
            if not hb:
                hb = WorkerHeartbeat(
                    worker_id=worker.id,
                    last_seen=datetime.datetime.now(datetime.timezone.utc)
                )
                session.add(hb)
            else:
                hb.last_seen = datetime.datetime.now(datetime.timezone.utc)
            await session.flush()

            # Clean existing jobs in this queue to start fresh
            jobs_to_delete_res = await session.execute(select(Job).where(Job.queue_id == queue.id))
            jobs_to_delete = jobs_to_delete_res.scalars().all()
            for j in jobs_to_delete:
                await session.delete(j)
            await session.flush()

            # 1. Seed QUEUED Job
            queued_job = Job(
                id=uuid.uuid4(),
                queue_id=queue.id,
                status=JobStatus.QUEUED,
                job_type="immediate",
                payload={"action": "reindex_search_catalog", "limit": 1000},
                retry_count=0
            )
            session.add(queued_job)

            # 2. Seed RUNNING Job
            running_job = Job(
                id=uuid.uuid4(),
                queue_id=queue.id,
                status=JobStatus.RUNNING,
                job_type="immediate",
                payload={"action": "compress_s3_backups", "archive_format": "tar.gz"},
                retry_count=1
            )
            session.add(running_job)
            await session.flush()

            running_execution = JobExecution(
                id=uuid.uuid4(),
                job_id=running_job.id,
                worker_id=worker.id,
                status="running",
                started_at=datetime.datetime.now(datetime.timezone.utc),
                attempt_number=2
            )
            session.add(running_execution)

            # 3. Seed COMPLETED Job
            completed_job = Job(
                id=uuid.uuid4(),
                queue_id=queue.id,
                status=JobStatus.COMPLETED,
                job_type="immediate",
                payload={"action": "generate_monthly_invoice", "billing_period": "2026-06"},
                retry_count=0
            )
            session.add(completed_job)
            await session.flush()

            completed_execution = JobExecution(
                id=uuid.uuid4(),
                job_id=completed_job.id,
                worker_id=worker.id,
                status="completed",
                started_at=datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=5),
                finished_at=datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=4),
                attempt_number=1
            )
            session.add(completed_execution)

            # 4. Seed FAILED Job
            failed_job = Job(
                id=uuid.uuid4(),
                queue_id=queue.id,
                status=JobStatus.FAILED,
                job_type="immediate",
                payload={"action": "sync_stripe_webhooks", "event_id": "evt_99812A"},
                retry_count=3
            )
            session.add(failed_job)
            await session.flush()

            failed_execution = JobExecution(
                id=uuid.uuid4(),
                job_id=failed_job.id,
                worker_id=worker.id,
                status="failed",
                started_at=datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=10),
                finished_at=datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=9),
                attempt_number=3
            )
            session.add(failed_execution)

            # 5. Seed DEAD LETTER Job
            dlq_job = Job(
                id=uuid.uuid4(),
                queue_id=queue.id,
                status=JobStatus.DEAD_LETTER,
                job_type="immediate",
                payload={"action": "transcode_4k_hls_streams", "bitrate_bps": 8500000},
                retry_count=5
            )
            session.add(dlq_job)
            await session.flush()

            dlq_execution = JobExecution(
                id=uuid.uuid4(),
                job_id=dlq_job.id,
                worker_id=worker.id,
                status="failed",
                started_at=datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=15),
                finished_at=datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=14),
                attempt_number=5
            )
            session.add(dlq_execution)

            dlq_entry = DeadLetterQueue(
                id=uuid.uuid4(),
                job_id=dlq_job.id,
                failure_reason="OutOfMemoryError: Subprocess transcoder process terminated by OS killer (attempts exhausted: 5/5)",
                moved_at=datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=14)
            )
            session.add(dlq_entry)

            logger.info("Pipeline test jobs seeded successfully!")

if __name__ == "__main__":
    asyncio.run(seed_pipeline_jobs())
