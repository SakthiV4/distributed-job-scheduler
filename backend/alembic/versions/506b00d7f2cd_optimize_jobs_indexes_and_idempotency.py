"""optimize_jobs_indexes_and_idempotency

Revision ID: 506b00d7f2cd
Revises: dac0f4bb02eb
Create Date: 2026-07-03 21:23:37.025112

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '506b00d7f2cd'
down_revision: Union[str, None] = 'dac0f4bb02eb'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Drop existing unique index on idempotency_key
    op.drop_index('ix_jobs_idempotency_key', table_name='jobs')
    # Recreate it as non-unique
    op.create_index('ix_jobs_idempotency_key', 'jobs', ['idempotency_key'], unique=False)
    
    # 2. Add composite unique constraint
    op.create_unique_constraint('uq_jobs_queue_idempotency_key', 'jobs', ['queue_id', 'idempotency_key'])
    
    # 3. Drop existing full index for claim lookup
    op.drop_index('ix_jobs_claim_lookup', table_name='jobs')
    # Recreate it as partial index
    op.create_index(
        'ix_jobs_claim_lookup', 
        'jobs', 
        ['queue_id', 'status', 'run_at'], 
        unique=False, 
        postgresql_where="status IN ('queued', 'scheduled')"
    )


def downgrade() -> None:
    # 1. Drop partial index
    op.drop_index('ix_jobs_claim_lookup', table_name='jobs', postgresql_where="status IN ('queued', 'scheduled')")
    # Recreate full index
    op.create_index('ix_jobs_claim_lookup', 'jobs', ['queue_id', 'status', 'run_at'], unique=False)
    
    # 2. Drop composite unique constraint
    op.drop_constraint('uq_jobs_queue_idempotency_key', 'jobs', type_='unique')
    
    # 3. Drop non-unique index
    op.drop_index('ix_jobs_idempotency_key', table_name='jobs')
    # Recreate unique index
    op.create_index('ix_jobs_idempotency_key', 'jobs', ['idempotency_key'], unique=True)
