import logging
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from app.dependencies import get_db, get_current_user
from app.models.user import User
from app.models.project import Project
from app.models.queue import Queue
from app.schemas.queue import QueueCreate, QueueUpdate, QueueResponse
from app.services.crud import paginate

router = APIRouter()
logger = logging.getLogger("scheduler.queues")


async def _verify_project_access(project_id: UUID, user: User, db: AsyncSession) -> Project:
    result = await db.execute(select(Project).where(Project.id == project_id))
    project = result.scalar_one_or_none()
    if project is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Project not found")
    if project.org_id != user.org_id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Access denied")
    return project


async def _get_queue_with_access(queue_id: UUID, user: User, db: AsyncSession) -> Queue:
    result = await db.execute(
        select(Queue).options(joinedload(Queue.retry_policy)).where(Queue.id == queue_id)
    )
    queue = result.scalar_one_or_none()
    if queue is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Queue not found")

    # Verify the queue belongs to a project in the user's org
    project_result = await db.execute(select(Project).where(Project.id == queue.project_id))
    project = project_result.scalar_one_or_none()
    if project is None or project.org_id != user.org_id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Access denied")

    return queue


from sqlalchemy.exc import IntegrityError
from app.schemas.paginated import PaginatedQueuesResponse


@router.post(
    "/projects/{project_id}/queues",
    response_model=QueueResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_queue(
    project_id: UUID,
    body: QueueCreate,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    await _verify_project_access(project_id, current_user, db)

    queue = Queue(
        project_id=project_id,
        name=body.name,
        priority=body.priority,
        concurrency_limit=body.concurrency_limit,
        retry_policy_id=body.retry_policy_id,
    )
    db.add(queue)
    try:
        await db.commit()
    except IntegrityError as e:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Database integrity violation or invalid retry policy"
        ) from e

    await db.refresh(queue)

    # Reload with retry_policy eager-loaded
    result = await db.execute(
        select(Queue).options(joinedload(Queue.retry_policy)).where(Queue.id == queue.id)
    )
    queue = result.scalar_one()

    logger.info("Queue created: %s (project: %s)", queue.name, project_id)
    return queue


@router.get("/projects/{project_id}/queues", response_model=PaginatedQueuesResponse)
async def list_queues(
    project_id: UUID,
    priority: int | None = Query(default=None, description="Filter by exact priority"),
    paused: bool | None = Query(default=None, description="Filter by paused status"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    await _verify_project_access(project_id, current_user, db)

    query = (
        select(Queue)
        .options(joinedload(Queue.retry_policy))
        .where(Queue.project_id == project_id)
        .order_by(Queue.priority.desc(), Queue.created_at)
    )
    if priority is not None:
        query = query.where(Queue.priority == priority)
    if paused is not None:
        query = query.where(Queue.paused == paused)

    result = await paginate(db, query, page, page_size)
    return result.to_dict(lambda q: QueueResponse.model_validate(q).model_dump(mode="json"))


@router.get("/queues/{queue_id}", response_model=QueueResponse)
async def get_queue(
    queue_id: UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    return await _get_queue_with_access(queue_id, current_user, db)


@router.put("/queues/{queue_id}", response_model=QueueResponse)
async def update_queue(
    queue_id: UUID,
    body: QueueUpdate,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    queue = await _get_queue_with_access(queue_id, current_user, db)

    update_data = body.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(queue, field, value)

    try:
        await db.commit()
    except IntegrityError as e:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Database integrity violation or invalid retry policy"
        ) from e

    await db.refresh(queue)

    # Reload with retry_policy
    result = await db.execute(
        select(Queue).options(joinedload(Queue.retry_policy)).where(Queue.id == queue.id)
    )
    queue = result.scalar_one()

    logger.info("Queue updated: %s", queue_id)
    return queue


@router.delete("/queues/{queue_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_queue(
    queue_id: UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    queue = await _get_queue_with_access(queue_id, current_user, db)
    await db.delete(queue)
    await db.commit()
    logger.info("Queue deleted: %s", queue_id)
