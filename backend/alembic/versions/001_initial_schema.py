"""initial schema

Revision ID: 001_initial
Revises:
Create Date: 2026-07-04

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB

revision: str = "001_initial"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # --- Organizations ---
    op.create_table(
        "organizations",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("name", sa.String(255), unique=True, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )

    # --- Users ---
    op.create_table(
        "users",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("org_id", UUID(as_uuid=True), sa.ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False),
        sa.Column("email", sa.String(320), unique=True, nullable=False),
        sa.Column("hashed_password", sa.String(255), nullable=False),
        sa.Column("role", sa.Enum("admin", "member", name="user_role", create_constraint=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_users_email", "users", ["email"])

    # --- Projects ---
    op.create_table(
        "projects",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("org_id", UUID(as_uuid=True), sa.ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("org_id", "name", name="uq_project_org_name"),
    )

    # --- Retry Policies ---
    op.create_table(
        "retry_policies",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("name", sa.String(100), unique=True, nullable=False),
        sa.Column("strategy", sa.Enum("fixed", "linear", "exponential", name="retry_strategy", create_constraint=True), nullable=False),
        sa.Column("max_retries", sa.Integer, nullable=False),
        sa.Column("base_delay_ms", sa.Integer, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )

    # --- Seed default retry policies ---
    op.execute(
        """
        INSERT INTO retry_policies (id, name, strategy, max_retries, base_delay_ms, created_at, updated_at) VALUES
        ('a0000000-0000-0000-0000-000000000001', 'Default Fixed (3 retries, 1s)', 'fixed', 3, 1000, NOW(), NOW()),
        ('a0000000-0000-0000-0000-000000000002', 'Default Linear (5 retries, 2s)', 'linear', 5, 2000, NOW(), NOW()),
        ('a0000000-0000-0000-0000-000000000003', 'Default Exponential (5 retries, 1s)', 'exponential', 5, 1000, NOW(), NOW())
        """
    )

    # --- Queues ---
    op.create_table(
        "queues",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("project_id", UUID(as_uuid=True), sa.ForeignKey("projects.id", ondelete="CASCADE"), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("priority", sa.Integer, default=0, nullable=False),
        sa.Column("concurrency_limit", sa.Integer, default=5, nullable=False),
        sa.Column("paused", sa.Boolean, default=False, nullable=False),
        sa.Column("retry_policy_id", UUID(as_uuid=True), sa.ForeignKey("retry_policies.id", ondelete="SET NULL"), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )

    # --- Jobs ---
    op.create_table(
        "jobs",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("queue_id", UUID(as_uuid=True), sa.ForeignKey("queues.id", ondelete="CASCADE"), nullable=False),
        sa.Column("status", sa.Enum("queued", "scheduled", "claimed", "running", "completed", "failed", "dead_letter", name="job_status", create_constraint=True), default="queued", nullable=False),
        sa.Column("run_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("job_type", sa.Enum("immediate", "delayed", "scheduled", "recurring", "batch", name="job_type", create_constraint=True), nullable=False),
        sa.Column("payload", JSONB, nullable=True),
        sa.Column("idempotency_key", sa.String(255), unique=True, nullable=True),
        sa.Column("max_retries", sa.Integer, default=3, nullable=False),
        sa.Column("retry_count", sa.Integer, default=0, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    # Critical composite index for atomic claim query
    op.create_index("ix_jobs_claim_lookup", "jobs", ["queue_id", "status", "run_at"])
    op.create_index("ix_jobs_status", "jobs", ["status"])
    op.create_index("ix_jobs_idempotency_key", "jobs", ["idempotency_key"])

    # --- Scheduled Jobs ---
    op.create_table(
        "scheduled_jobs",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("job_id", UUID(as_uuid=True), sa.ForeignKey("jobs.id", ondelete="CASCADE"), unique=True, nullable=False),
        sa.Column("cron_expr", sa.String(100), nullable=False),
        sa.Column("next_run_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )

    # --- Workers ---
    op.create_table(
        "workers",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("hostname", sa.String(255), nullable=False),
        sa.Column("status", sa.Enum("online", "offline", "draining", name="worker_status", create_constraint=True), default="online", nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )

    # --- Worker Heartbeats ---
    op.create_table(
        "worker_heartbeats",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("worker_id", UUID(as_uuid=True), sa.ForeignKey("workers.id", ondelete="CASCADE"), nullable=False),
        sa.Column("last_seen", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )

    # --- Job Executions ---
    op.create_table(
        "job_executions",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("job_id", UUID(as_uuid=True), sa.ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("worker_id", UUID(as_uuid=True), sa.ForeignKey("workers.id", ondelete="SET NULL"), nullable=True),
        sa.Column("status", sa.Enum("running", "completed", "failed", name="execution_status", create_constraint=True), default="running", nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempt_number", sa.Integer, default=1, nullable=False),
    )
    op.create_index("ix_job_executions_job_id", "job_executions", ["job_id"])

    # --- Job Logs ---
    op.create_table(
        "job_logs",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("execution_id", UUID(as_uuid=True), sa.ForeignKey("job_executions.id", ondelete="CASCADE"), nullable=False),
        sa.Column("level", sa.Enum("info", "warning", "error", name="log_level", create_constraint=True), default="info", nullable=False),
        sa.Column("message", sa.Text, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_job_logs_execution_id", "job_logs", ["execution_id"])

    # --- Dead Letter Queue ---
    op.create_table(
        "dead_letter_queue",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("job_id", UUID(as_uuid=True), sa.ForeignKey("jobs.id", ondelete="CASCADE"), unique=True, nullable=False),
        sa.Column("failure_reason", sa.Text, nullable=False),
        sa.Column("moved_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("dead_letter_queue")
    op.drop_table("job_logs")
    op.drop_table("job_executions")
    op.drop_table("worker_heartbeats")
    op.drop_table("workers")
    op.drop_table("scheduled_jobs")
    op.drop_table("jobs")
    op.drop_table("queues")
    op.drop_table("retry_policies")
    op.drop_table("projects")
    op.drop_table("users")
    op.drop_table("organizations")

    # Drop enums
    op.execute("DROP TYPE IF EXISTS user_role")
    op.execute("DROP TYPE IF EXISTS retry_strategy")
    op.execute("DROP TYPE IF EXISTS job_status")
    op.execute("DROP TYPE IF EXISTS job_type")
    op.execute("DROP TYPE IF EXISTS worker_status")
    op.execute("DROP TYPE IF EXISTS execution_status")
    op.execute("DROP TYPE IF EXISTS log_level")
