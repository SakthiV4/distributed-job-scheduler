import uuid
import enum

from sqlalchemy import String, Integer, Enum
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, TimestampMixin


class RetryStrategy(str, enum.Enum):
    FIXED = "fixed"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"


class RetryPolicy(Base, TimestampMixin):
    __tablename__ = "retry_policies"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    name: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    strategy: Mapped[RetryStrategy] = mapped_column(
        Enum(RetryStrategy, name="retry_strategy", create_constraint=True, values_callable=lambda e: [x.value for x in e]),
        nullable=False,
    )
    max_retries: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    base_delay_ms: Mapped[int] = mapped_column(Integer, nullable=False, default=1000)
