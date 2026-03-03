"""Add missing composite indexes for cursor pagination and analytics.

Revision ID: 010_add_composite_indexes
Revises: 009_add_trgm_indexes
Create Date: 2026-02-23
"""
from alembic import op


revision = "010_add_composite_indexes"
down_revision = "009_add_trgm_indexes"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_jobs_cursor_pagination
        ON jobs (updated_at DESC, id)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_user_job_events_job_occurred
        ON user_job_events (job_id, occurred_at)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_user_job_events_user_occurred
        ON user_job_events (user_id, occurred_at)
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_user_job_events_user_occurred")
    op.execute("DROP INDEX IF EXISTS idx_user_job_events_job_occurred")
    op.execute("DROP INDEX IF EXISTS idx_jobs_cursor_pagination")
