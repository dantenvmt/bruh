"""add normalize columns

Revision ID: 14cbf66cf41d
Revises: 014_add_ai_summaries
Create Date: 2026-03-15 16:43:38.207041

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '14cbf66cf41d'
down_revision = '014_add_ai_summaries'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('jobs', sa.Column('salary_min', sa.Integer(), nullable=True))
    op.add_column('jobs', sa.Column('salary_max', sa.Integer(), nullable=True))
    op.add_column('jobs', sa.Column('seniority', sa.String(length=32), nullable=True))
    op.add_column('jobs', sa.Column('visa_sponsorship', sa.Boolean(), nullable=True))
    op.add_column('jobs', sa.Column('ai_summary_bullets', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.add_column('jobs', sa.Column('normalized_at', sa.DateTime(), nullable=True))


def downgrade() -> None:
    op.drop_column('jobs', 'normalized_at')
    op.drop_column('jobs', 'ai_summary_bullets')
    op.drop_column('jobs', 'visa_sponsorship')
    op.drop_column('jobs', 'seniority')
    op.drop_column('jobs', 'salary_max')
    op.drop_column('jobs', 'salary_min')
