"""Add run_sources table for per-source tracking

Revision ID: 004_add_run_sources_table
Revises: 003_scrub_raw_payloads
Create Date: 2026-02-10

This migration adds the run_sources table to track per-source, per-target
results for each ingestion run. This enables granular observability into
which ATS boards are working, failing, or returning no results.

Schema:
- run_id: FK to runs table
- source: Source name (e.g., "greenhouse", "lever")
- source_target: Target identifier (e.g., "stripe", "airbnb" for ATS boards)
- jobs_fetched: Count of jobs fetched from this source/target
- jobs_after_dedupe: Count after deduplication
- error_message: Error message if fetch failed
- error_code: Error category (e.g., "rate_limited", "not_found")
- request_duration_ms: Request duration for performance analysis
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '004_add_run_sources_table'
down_revision = '003_scrub_raw_payloads'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create run_sources table with indexes."""
    op.create_table(
        'run_sources',
        sa.Column('id', sa.UUID(), nullable=False),
        sa.Column('run_id', sa.UUID(), nullable=False),
        sa.Column('source', sa.String(length=64), nullable=False),
        sa.Column('source_target', sa.String(length=256), nullable=True),
        sa.Column('jobs_fetched', sa.Integer(), nullable=True),
        sa.Column('jobs_after_dedupe', sa.Integer(), nullable=True),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('error_code', sa.String(length=32), nullable=True),
        sa.Column('request_duration_ms', sa.Integer(), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['run_id'], ['runs.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id')
    )

    # Index on run_id for querying all sources for a given run
    op.create_index('ix_run_sources_run_id', 'run_sources', ['run_id'], unique=False)

    # Composite index on source + source_target for historical board health queries
    op.create_index('idx_run_sources_source_target', 'run_sources', ['source', 'source_target'], unique=False)


def downgrade() -> None:
    """Drop run_sources table and indexes."""
    op.drop_index('idx_run_sources_source_target', table_name='run_sources')
    op.drop_index('ix_run_sources_run_id', table_name='run_sources')
    op.drop_table('run_sources')
