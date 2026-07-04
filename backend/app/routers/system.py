import logging
import uuid
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List

from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy import select, func, and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.dependencies import get_db, get_current_user
from app.models.user import User, UserRole
from app.models.project import Project
from app.models.queue import Queue
from app.models.job import Job, JobStatus
from app.models.worker import Worker, WorkerStatus
from app.models.worker_heartbeat import WorkerHeartbeat

router = APIRouter()
logger = logging.getLogger("scheduler.system")


@router.get("/system/summary")
async def get_system_summary(
    project_id: uuid.UUID = Query(..., description="Project ID to fetch summary for"),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Returns aggregated job status counts and online worker counts for a project."""
    # 1. Enforce org boundary
    project_res = await db.execute(select(Project).where(Project.id == project_id))
    project = project_res.scalar_one_or_none()
    if not project:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Project not found")
    
    if project.org_id != current_user.org_id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Access denied")

    # 2. Get queues in project
    queues_res = await db.execute(select(Queue.id).where(Queue.project_id == project_id))
    queue_ids = [row[0] for row in queues_res.all()]

    job_counts = {s.value: 0 for s in JobStatus}
    if queue_ids:
        # Group and count by status
        counts_res = await db.execute(
            select(Job.status, func.count(Job.id))
            .where(Job.queue_id.in_(queue_ids))
            .group_by(Job.status)
        )
        for row in counts_res.all():
            job_counts[row[0].value] = row[1]

    # 3. Get count of active online workers (last heartbeat within 60s)
    threshold = datetime.now(timezone.utc) - timedelta(seconds=60)
    workers_res = await db.execute(
        select(func.count(Worker.id))
        .join(WorkerHeartbeat, Worker.id == WorkerHeartbeat.worker_id)
        .where(Worker.status == WorkerStatus.ONLINE)
        .where(WorkerHeartbeat.last_seen >= threshold)
    )
    active_workers_count = workers_res.scalar() or 0

    return {
        "job_counts": job_counts,
        "active_workers_count": active_workers_count,
    }


@router.get("/system/workers")
async def get_worker_fleet(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Lists registered worker fleet statuses. Restricted to admin users."""
    # Enforce admin privilege to prevent cross-tenant leakage of node names
    if current_user.role != UserRole.ADMIN:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Access denied")

    threshold = datetime.now(timezone.utc) - timedelta(seconds=60)
    workers_res = await db.execute(
        select(Worker, WorkerHeartbeat.last_seen)
        .join(WorkerHeartbeat, Worker.id == WorkerHeartbeat.worker_id)
        .where(Worker.status == WorkerStatus.ONLINE)
        .where(WorkerHeartbeat.last_seen >= threshold)
        .order_by(WorkerHeartbeat.last_seen.desc())
    )
    
    fleet = []
    for worker, last_seen in workers_res.all():
        fleet.append({
            "id": str(worker.id),
            "hostname": worker.hostname,
            "status": worker.status.value,
            "last_seen": last_seen.isoformat() if last_seen else None,
            "created_at": worker.created_at.isoformat(),
        })

    return fleet
