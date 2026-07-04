import uuid
import enum

from sqlalchemy import String, Integer, ForeignKey, Enum, DateTime, Index, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin


class JobStatus(str, enum.Enum):
    QUEUED = "queued"
    SCHEDULED = "scheduled"
    CLAIMED = "claimed"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    DEAD_LETTER = "dead_letter"


class JobType(str, enum.Enum):
    IMMEDIATE = "immediate"
    DELAYED = "delayed"
    SCHEDULED = "scheduled"
    RECURRING = "recurring"
    BATCH = "batch"


class Job(Base, TimestampMixin):
    __tablename__ = "jobs"
    __table_args__ = (
        Index(
            "ix_jobs_claim_lookup",
            "queue_id",
            "status",
            "run_at",
            postgresql_where=text("status IN ('queued', 'scheduled')"),
        ),
        UniqueConstraint("queue_id", "idempotency_key", name="uq_jobs_queue_idempotency_key"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    queue_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("queues.id", ondelete="CASCADE"), nullable=False
    )
    status: Mapped[JobStatus] = mapped_column(
        Enum(JobStatus, name="job_status", create_constraint=True, values_callable=lambda e: [x.value for x in e]),
        default=JobStatus.QUEUED,
        nullable=False,
        index=True,
    )
    run_at: Mapped[str | None] = mapped_column(DateTime(timezone=True), nullable=True)
    job_type: Mapped[JobType] = mapped_column(
        Enum(JobType, name="job_type", create_constraint=True, values_callable=lambda e: [x.value for x in e]),
        nullable=False,
    )
    payload: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    idempotency_key: Mapped[str | None] = mapped_column(
        String(255), nullable=True, index=True
    )
    max_retries: Mapped[int] = mapped_column(Integer, default=3, nullable=False)
    retry_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    # Relationships
    queue = relationship("Queue", back_populates="jobs")
    executions = relationship("JobExecution", back_populates="job", cascade="all, delete-orphan")
    scheduled_job = relationship("ScheduledJob", back_populates="job", uselist=False, cascade="all, delete-orphan")
    dead_letter_entry = relationship("DeadLetterQueue", back_populates="job", uselist=False, cascade="all, delete-orphan")
    dependencies_left = relationship("JobDependency", foreign_keys="[JobDependency.job_id]", back_populates="job", cascade="all, delete-orphan")
    dependencies_right = relationship("JobDependency", foreign_keys="[JobDependency.depends_on_job_id]", back_populates="depends_on", cascade="all, delete-orphan")

    @property
    def depends_on(self):
        if "dependencies_left" in self.__dict__:
            return [dep.depends_on for dep in self.dependencies_left if dep.depends_on]
        return []
