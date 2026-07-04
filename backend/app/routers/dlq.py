import logging
import uuid
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status, Query, Response
from sqlalchemy import select, delete, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload
from pydantic import BaseModel

from app.dependencies import get_db, get_current_user
from app.models.user import User
from app.models.project import Project
from app.models.queue import Queue
from app.models.job import Job, JobStatus
from app.models.dead_letter_queue import DeadLetterQueue
from app.routers.jobs import _verify_queue_access, JobResponse
from app.services.crud import paginate
from app.schemas.dlq import DLQJobResponse
from app.schemas.paginated import PaginatedDLQResponse

router = APIRouter()
logger = logging.getLogger("scheduler.dlq")


async def _verify_job_dlq_access(job_id: uuid.UUID, user: User, db: AsyncSession) -> tuple[Job, DeadLetterQueue]:
    """Helper to verify a job's existence, DLQ status, and caller organization access."""
    # Fetch job along with its DLQ entry
    job_result = await db.execute(
        select(Job)
        .options(joinedload(Job.dead_letter_entry))
        .where(Job.id == job_id)
    )
    job = job_result.scalar_one_or_none()
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")

    # Check that it actually has a DLQ entry
    dlq_entry = job.dead_letter_entry
    if dlq_entry is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, 
            detail="Job is not in the Dead Letter Queue"
        )

    # Scoped auth: verify parent queue and project organization
    await _verify_queue_access(job.queue_id, user, db)

    return job, dlq_entry


@router.get(
    "/queues/{queue_id}/dlq",
    response_model=PaginatedDLQResponse,
)
async def list_dlq_jobs(
    queue_id: uuid.UUID,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Retrieves a paginated list of dead-lettered jobs for a given queue."""
    # Verify organization access to the queue
    await _verify_queue_access(queue_id, current_user, db)

    # Query dead letter queue joined with the job
    query = (
        select(DeadLetterQueue)
        .options(joinedload(DeadLetterQueue.job).joinedload(Job.scheduled_job))
        .join(Job, DeadLetterQueue.job_id == Job.id)
        .where(Job.queue_id == queue_id)
        .order_by(DeadLetterQueue.moved_at.desc())
    )

    result = await paginate(db, query, page, page_size)
    return result.to_dict(lambda item: DLQJobResponse.model_validate(item).model_dump(mode="json"))


@router.post(
    "/dlq/{job_id}/requeue",
    response_model=JobResponse,
)
async def requeue_dlq_job(
    job_id: uuid.UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Manually requeues a job from the DLQ. Resets attempt counter and schedules immediately."""
    job, dlq_entry = await _verify_job_dlq_access(job_id, current_user, db)

    try:
        # Wrap DLQ delete, job update in a transaction block
        async with db.begin_nested():
            # Delete the DLQ entry
            await db.delete(dlq_entry)
            
            # Reset retry counts, change status to queued, schedule run immediately
            job.status = JobStatus.QUEUED
            job.retry_count = 0
            job.run_at = datetime.now(timezone.utc)
            job.updated_at = datetime.now(timezone.utc)

        await db.commit()
        logger.info(f"User {current_user.email} manually requeued job {job_id} from DLQ.")
    except Exception as e:
        await db.rollback()
        logger.exception(f"Failed to requeue job {job_id} from DLQ: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to requeue job"
        ) from e

    # Return refreshed job details
    refreshed_result = await db.execute(
        select(Job).options(joinedload(Job.scheduled_job)).where(Job.id == job_id)
    )
    return refreshed_result.scalar_one()


@router.delete(
    "/dlq/{job_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def discard_dlq_job(
    job_id: uuid.UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Manually discards/removes a job from the DLQ, cascading deletions to logs."""
    job, dlq_entry = await _verify_job_dlq_access(job_id, current_user, db)

    try:
        async with db.begin_nested():
            await db.delete(dlq_entry)
            job.status = JobStatus.FAILED
            job.updated_at = datetime.now(timezone.utc)
        await db.commit()
        logger.info(f"User {current_user.email} soft-discarded job {job_id} from DLQ (status updated to failed).")
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    except Exception as e:
        await db.rollback()
        logger.exception(f"Failed to soft-discard job {job_id} from DLQ: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to discard job"
        ) from e
