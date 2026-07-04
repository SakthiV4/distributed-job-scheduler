import uuid
import enum

from sqlalchemy import String, Enum
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin


class WorkerStatus(str, enum.Enum):
    ONLINE = "online"
    OFFLINE = "offline"
    DRAINING = "draining"


class Worker(Base, TimestampMixin):
    __tablename__ = "workers"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    hostname: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[WorkerStatus] = mapped_column(
        Enum(WorkerStatus, name="worker_status", create_constraint=True, values_callable=lambda e: [x.value for x in e]),
        default=WorkerStatus.ONLINE,
        nullable=False,
    )

    # Relationships
    heartbeats = relationship("WorkerHeartbeat", back_populates="worker", cascade="all, delete-orphan")
    executions = relationship("JobExecution", back_populates="worker")
