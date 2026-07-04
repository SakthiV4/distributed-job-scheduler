import logging
import uuid
from datetime import datetime, timezone, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status, Query, Response
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import joinedload, selectinload
from croniter import croniter

from app.dependencies import get_db, get_current_user
from app.models.user import User
from app.models.project import Project
from app.models.queue import Queue
from app.models.job import Job, JobStatus, JobType
from app.models.scheduled_job import ScheduledJob
from app.models.job_dependency import JobDependency
from app.schemas.job import JobCreate, BatchJobCreate, JobResponse
from app.services.crud import paginate

router = APIRouter()
logger = logging.getLogger("scheduler.jobs")


async def _verify_queue_access(queue_id: uuid.UUID, user: User, db: AsyncSession) -> Queue:
    # Fetch queue with retry policy loaded
    result = await db.execute(
        select(Queue).options(joinedload(Queue.retry_policy)).where(Queue.id == queue_id)
    )
    queue = result.scalar_one_or_none()
    if queue is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Queue not found")

    # Verify project and organization
    project_result = await db.execute(select(Project).where(Project.id == queue.project_id))
    project = project_result.scalar_one_or_none()
    if project is None or project.org_id != user.org_id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Access denied")

    return queue


async def _get_existing_job_by_idempotency_key(
    db: AsyncSession, queue_id: uuid.UUID, idempotency_key: str
) -> Optional[Job]:
    result = await db.execute(
        select(Job)
        .options(joinedload(Job.scheduled_job))
        .where(Job.queue_id == queue_id, Job.idempotency_key == idempotency_key)
    )
    return result.scalar_one_or_none()


def _calculate_run_at(body: JobCreate) -> datetime:
    now = datetime.now(timezone.utc)
    if body.job_type == JobType.IMMEDIATE:
        return now
    elif body.job_type == JobType.DELAYED:
        return now + timedelta(seconds=body.delay_seconds)
    elif body.job_type == JobType.SCHEDULED:
        return body.run_at
    elif body.job_type == JobType.RECURRING:
        iter = croniter(body.cron_expr, now)
        return iter.get_next(datetime)
    return now


@router.post(
    "/queues/{queue_id}/jobs",
    response_model=JobResponse,
    status_code=status.HTTP_201_CREATED,
)
async def submit_job(
    queue_id: uuid.UUID,
    body: JobCreate,
    response: Response,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    queue = await _verify_queue_access(queue_id, current_user, db)

    # 1. Fast idempotency check (pre-transaction check)
    if body.idempotency_key:
        existing = await _get_existing_job_by_idempotency_key(db, queue_id, body.idempotency_key)
        if existing:
            response.status_code = status.HTTP_200_OK
            response.headers["X-Idempotent-Replay"] = "true"
            logger.info("Idempotent replay triggered for key: %s in queue %s", body.idempotency_key, queue_id)
            return existing

    # 2. Setup job parameters
    run_at = _calculate_run_at(body)
    
    # 3. Validate dependencies if declared
    if body.depends_on:
        dep_jobs_res = await db.execute(
            select(Job.id, Queue.project_id)
            .join(Queue, Job.queue_id == Queue.id)
            .where(Job.id.in_(body.depends_on))
        )
        dep_jobs = dep_jobs_res.all()
        dep_map = {row[0]: row[1] for row in dep_jobs}

        for d_id in body.depends_on:
            if d_id not in dep_map:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Referenced dependency job ID {d_id} does not exist."
                )
            if dep_map[d_id] != queue.project_id:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Dependency job ID {d_id} belongs to a different project."
                )

    # State mapping: immediate is queued; delayed/scheduled/recurring starts as scheduled
    job_status = JobStatus.QUEUED if body.job_type == JobType.IMMEDIATE else JobStatus.SCHEDULED

    # Resolve default max_retries
    max_retries = body.max_retries
    if max_retries is None:
        if queue.retry_policy:
            max_retries = queue.retry_policy.max_retries
        else:
            max_retries = 3

    # Create job object
    job = Job(
        queue_id=queue_id,
        status=job_status,
        run_at=run_at,
        job_type=body.job_type,
        payload=body.payload or {},
        idempotency_key=body.idempotency_key,
        max_retries=max_retries,
        retry_count=0,
    )

    try:
        db.add(job)
        await db.flush()  # Generate job.id

        # Insert JobDependency entries
        if body.depends_on:
            for dep_job_id in body.depends_on:
                dep_row = JobDependency(job_id=job.id, depends_on_job_id=dep_job_id)
                db.add(dep_row)

        # If recurring, create the child ScheduledJob entry
        if body.job_type == JobType.RECURRING:
            scheduled = ScheduledJob(
                job_id=job.id,
                cron_expr=body.cron_expr,
                next_run_at=run_at,
            )
            db.add(scheduled)

        await db.commit()
    except IntegrityError as e:
        # Handle concurrent race condition on duplicate insert
        await db.rollback()
        if body.idempotency_key:
            existing = await _get_existing_job_by_idempotency_key(db, queue_id, body.idempotency_key)
            if existing:
                response.status_code = status.HTTP_200_OK
                response.headers["X-Idempotent-Replay"] = "true"
                logger.info("Idempotent replay (race recovery) triggered for key: %s in queue %s", body.idempotency_key, queue_id)
                return existing
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Database integrity violation or duplicate key",
        ) from e

    # Refresh and load relationships
    result = await db.execute(
        select(Job)
        .options(
            joinedload(Job.scheduled_job),
            selectinload(Job.dependencies_left).joinedload(JobDependency.depends_on)
        )
        .where(Job.id == job.id)
    )
    job = result.scalar_one()

    logger.info("Job submitted: %s (type: %s, status: %s)", job.id, job.job_type, job.status)
    return job


@router.post(
    "/queues/{queue_id}/batches",
    response_model=list[JobResponse],
    status_code=status.HTTP_201_CREATED,
)
async def submit_batch(
    queue_id: uuid.UUID,
    body: BatchJobCreate,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    queue = await _verify_queue_access(queue_id, current_user, db)

    batch_id = uuid.uuid4()
    created_jobs = []

    # Resolve default max_retries
    default_max_retries = 3
    if queue.retry_policy:
        default_max_retries = queue.retry_policy.max_retries

    try:
        for job_def in body.jobs:
            # Enforce batch job type
            run_at = _calculate_run_at(job_def)
            job_status = JobStatus.QUEUED if job_def.job_type == JobType.IMMEDIATE else JobStatus.SCHEDULED
            
            payload = job_def.payload or {}
            payload["_batch_id"] = str(batch_id)

            max_retries = job_def.max_retries if job_def.max_retries is not None else default_max_retries

            # Check idempotency key within the batch loop (fast check)
            if job_def.idempotency_key:
                existing = await _get_existing_job_by_idempotency_key(db, queue_id, job_def.idempotency_key)
                if existing:
                    # Fail batch on collision
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail=f"Idempotency key collision on job inside batch: {job_def.idempotency_key}",
                    )

            job = Job(
                queue_id=queue_id,
                status=job_status,
                run_at=run_at,
                job_type=JobType.BATCH,  # Mark as batch job in DB
                payload=payload,
                idempotency_key=job_def.idempotency_key,
                max_retries=max_retries,
                retry_count=0,
            )
            db.add(job)
            created_jobs.append(job)

        await db.commit()
    except IntegrityError as e:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Batch job submission failed due to duplicate key or integrity constraint",
        ) from e

    # Reload all jobs with relationships
    job_ids = [j.id for j in created_jobs]
    result = await db.execute(
        select(Job)
        .options(
            joinedload(Job.scheduled_job),
            selectinload(Job.dependencies_left).joinedload(JobDependency.depends_on)
        )
        .where(Job.id.in_(job_ids))
        .order_by(Job.created_at)
    )
    jobs = result.scalars().all()

    logger.info("Batch submitted: %s (jobs count: %d)", batch_id, len(jobs))
    return jobs


from app.schemas.paginated import PaginatedJobsResponse


@router.get("/queues/{queue_id}/jobs", response_model=PaginatedJobsResponse)
async def list_jobs_in_queue(
    queue_id: uuid.UUID,
    status: Optional[JobStatus] = Query(default=None),
    job_type: Optional[JobType] = Query(default=None),
    date_start: Optional[datetime] = Query(default=None),
    date_end: Optional[datetime] = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    await _verify_queue_access(queue_id, current_user, db)

    query = (
        select(Job)
        .options(
            joinedload(Job.scheduled_job),
            selectinload(Job.dependencies_left).joinedload(JobDependency.depends_on)
        )
        .where(Job.queue_id == queue_id)
        .order_by(Job.created_at.desc())
    )

    if status:
        query = query.where(Job.status == status)
    if job_type:
        query = query.where(Job.job_type == job_type)
    if date_start:
        query = query.where(Job.run_at >= date_start)
    if date_end:
        query = query.where(Job.run_at <= date_end)

    result = await paginate(db, query, page, page_size)
    return result.to_dict(lambda j: JobResponse.model_validate(j).model_dump(mode="json"))


@router.get("/jobs/{job_id}", response_model=JobResponse)
async def get_job_by_id(
    job_id: uuid.UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(Job)
        .options(
            joinedload(Job.scheduled_job),
            selectinload(Job.dependencies_left).joinedload(JobDependency.depends_on)
        )
        .where(Job.id == job_id)
    )
    job = result.scalar_one_or_none()
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")

    # Check access to the project owning the queue
    await _verify_queue_access(job.queue_id, current_user, db)
    return job
