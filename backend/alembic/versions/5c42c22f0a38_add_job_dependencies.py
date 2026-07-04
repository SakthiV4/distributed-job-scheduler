"""add_job_dependencies

Revision ID: 5c42c22f0a38
Revises: 506b00d7f2cd
Create Date: 2026-07-04 08:08:19.868713

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '5c42c22f0a38'
down_revision: Union[str, None] = '506b00d7f2cd'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'job_dependencies',
        sa.Column('job_id', sa.UUID(), nullable=False),
        sa.Column('depends_on_job_id', sa.UUID(), nullable=False),
        sa.ForeignKeyConstraint(['job_id'], ['jobs.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['depends_on_job_id'], ['jobs.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('job_id', 'depends_on_job_id')
    )


def downgrade() -> None:
    op.drop_table('job_dependencies')
