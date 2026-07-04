from pydantic import BaseModel, Field
from uuid import UUID
from datetime import datetime
from typing import Optional


class QueueCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    priority: int = Field(default=0, ge=0, le=100)
    concurrency_limit: int = Field(default=5, ge=1, le=1000)
    retry_policy_id: Optional[UUID] = None


class QueueUpdate(BaseModel):
    name: Optional[str] = Field(default=None, min_length=1, max_length=255)
    priority: Optional[int] = Field(default=None, ge=0, le=100)
    concurrency_limit: Optional[int] = Field(default=None, ge=1, le=1000)
    paused: Optional[bool] = None
    retry_policy_id: Optional[UUID] = None


class RetryPolicyResponse(BaseModel):
    id: UUID
    name: str
    strategy: str
    max_retries: int
    base_delay_ms: int

    model_config = {"from_attributes": True}


class QueueResponse(BaseModel):
    id: UUID
    project_id: UUID
    name: str
    priority: int
    concurrency_limit: int
    paused: bool
    retry_policy_id: Optional[UUID] = None
    retry_policy: Optional[RetryPolicyResponse] = None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}
