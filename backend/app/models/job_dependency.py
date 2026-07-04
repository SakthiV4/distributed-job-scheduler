import uuid
from sqlalchemy import ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.models.base import Base

class JobDependency(Base):
    __tablename__ = "job_dependencies"

    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"), primary_key=True, nullable=False
    )
    depends_on_job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"), primary_key=True, nullable=False
    )

    # Relationships
    job = relationship("Job", foreign_keys=[job_id], back_populates="dependencies_left")
    depends_on = relationship("Job", foreign_keys=[depends_on_job_id], back_populates="dependencies_right")
