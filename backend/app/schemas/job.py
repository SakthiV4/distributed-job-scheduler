from pydantic import BaseModel, Field, field_validator, model_validator
from uuid import UUID
from datetime import datetime, timezone
from typing import Optional, Any
from croniter import croniter

from app.models.job import JobType, JobStatus


class JobCreate(BaseModel):
    job_type: JobType
    payload: Optional[dict[str, Any]] = Field(default=None)
    idempotency_key: Optional[str] = Field(default=None, min_length=1, max_length=255)
    delay_seconds: Optional[int] = Field(default=None, ge=1)
    run_at: Optional[datetime] = Field(default=None)
    cron_expr: Optional[str] = Field(default=None, min_length=1, max_length=100)
    max_retries: Optional[int] = Field(default=None, ge=0, le=100)
    depends_on: Optional[list[UUID]] = Field(default=None)

    @model_validator(mode="after")
    def validate_job_parameters(self) -> "JobCreate":
        # 1. Delayed job validation
        if self.job_type == JobType.DELAYED:
            if self.delay_seconds is None:
                raise ValueError("delay_seconds is required when job_type is 'delayed'")
            if self.run_at is not None:
                raise ValueError("run_at cannot be set when job_type is 'delayed' (use delay_seconds instead)")

        # 2. Scheduled job validation
        elif self.job_type == JobType.SCHEDULED:
            if self.run_at is None:
                raise ValueError("run_at is required when job_type is 'scheduled'")
            if self.delay_seconds is not None:
                raise ValueError("delay_seconds cannot be set when job_type is 'scheduled'")
            # Check run_at is in the future
            if self.run_at < datetime.now(timezone.utc):
                raise ValueError("run_at must be a future datetime")

        # 3. Recurring job validation
        elif self.job_type == JobType.RECURRING:
            if self.cron_expr is None:
                raise ValueError("cron_expr is required when job_type is 'recurring'")
            if not croniter.is_valid(self.cron_expr):
                raise ValueError(f"cron_expr '{self.cron_expr}' is not a valid 5-field cron expression")
            if self.delay_seconds is not None or self.run_at is not None:
                raise ValueError("Neither delay_seconds nor run_at can be set when job_type is 'recurring'")

        # 4. Immediate job validation
        elif self.job_type == JobType.IMMEDIATE:
            if self.delay_seconds is not None or self.run_at is not None or self.cron_expr is not None:
                raise ValueError("delay_seconds, run_at, and cron_expr must not be set for 'immediate' jobs")

        return self


class BatchJobCreate(BaseModel):
    jobs: list[JobCreate] = Field(..., min_length=1, max_length=100)


class ScheduledJobResponse(BaseModel):
    id: UUID
    job_id: UUID
    cron_expr: str
    next_run_at: datetime
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class JobDependencyDetail(BaseModel):
    id: UUID
    status: JobStatus

    model_config = {"from_attributes": True}


class JobResponse(BaseModel):
    id: UUID
    queue_id: UUID
    status: JobStatus
    run_at: Optional[datetime]
    job_type: JobType
    payload: Optional[dict[str, Any]]
    idempotency_key: Optional[str]
    max_retries: int
    retry_count: int
    created_at: datetime
    updated_at: datetime
    scheduled_job: Optional[ScheduledJobResponse] = None
    depends_on: list[JobDependencyDetail] = Field(default=[])

    model_config = {"from_attributes": True}
