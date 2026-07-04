import logging
from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel
from uuid import UUID

from app.dependencies import get_db, get_current_user
from app.models.user import User
from app.models.retry_policy import RetryPolicy

router = APIRouter()
logger = logging.getLogger("scheduler.retry_policies")


class RetryPolicyResponse(BaseModel):
    id: UUID
    name: str
    strategy: str
    max_retries: int
    base_delay_ms: int

    model_config = {"from_attributes": True}


@router.get(
    "/retry-policies",
    response_model=list[RetryPolicyResponse],
)
async def list_retry_policies(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Retrieves all seeded retry policies in the system."""
    result = await db.execute(select(RetryPolicy).order_by(RetryPolicy.name))
    policies = result.scalars().all()
    logger.info(f"User {current_user.email} listed {len(policies)} retry policies.")
    return policies
