"""Initial schema with runs, jobs, job_seen, and source_errors tables

Revision ID: 001_initial_schema
Revises:
Create Date: 2026-01-25

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '001_initial_schema'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create initial database schema."""

    # Create runs table
    op.create_table(
        'runs',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('started_at', sa.DateTime(timezone=False), nullable=False),
        sa.Column('ended_at', sa.DateTime(timezone=False), nullable=True),
        sa.Column('status', sa.String(length=32), nullable=False),
        sa.Column('sources', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('total_jobs', sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )

    # Create jobs table
    op.create_table(
        'jobs',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('dedupe_key', sa.Text(), nullable=False),
        sa.Column('source', sa.String(length=64), nullable=True),
        sa.Column('source_job_id', sa.String(length=128), nullable=True),
        sa.Column('title', sa.Text(), nullable=False),
        sa.Column('company', sa.Text(), nullable=True),
        sa.Column('location', sa.Text(), nullable=True),
        sa.Column('url', sa.Text(), nullable=True),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('salary', sa.Text(), nullable=True),
        sa.Column('employment_type', sa.Text(), nullable=True),
        sa.Column('posted_date', sa.Text(), nullable=True),
        sa.Column('remote', sa.Boolean(), nullable=True),
        sa.Column('category', sa.Text(), nullable=True),
        sa.Column('tags', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('skills', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('raw_payload', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=False), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=False), server_default=sa.text('now()'), nullable=False),
        sa.Column('last_seen_at', sa.DateTime(timezone=False), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('dedupe_key')
    )

    # Create indexes on jobs table
    op.create_index('idx_jobs_dedupe_key', 'jobs', ['dedupe_key'], unique=True)
    op.create_index('idx_jobs_source', 'jobs', ['source'], unique=False)
    op.create_index('idx_jobs_created_at', 'jobs', ['created_at'], unique=False)
    op.create_index('idx_jobs_last_seen_at', 'jobs', ['last_seen_at'], unique=False)
    op.create_index('idx_jobs_updated_at_desc', 'jobs', [sa.text('updated_at DESC')], unique=False)

    # Create partial index for remote jobs (only index where remote = true)
    op.execute(
        "CREATE INDEX idx_jobs_remote_partial ON jobs (remote) WHERE remote = true"
    )

    # Create job_seen junction table
    op.create_table(
        'job_seen',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('run_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('job_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('seen_at', sa.DateTime(timezone=False), nullable=False),
        sa.ForeignKeyConstraint(['job_id'], ['jobs.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['run_id'], ['runs.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('run_id', 'job_id', name='uq_run_job_seen')
    )

    # Create indexes on job_seen table
    op.create_index('ix_job_seen_run_id', 'job_seen', ['run_id'], unique=False)
    op.create_index('ix_job_seen_job_id', 'job_seen', ['job_id'], unique=False)

    # Create source_errors table
    op.create_table(
        'source_errors',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('run_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('source', sa.String(length=64), nullable=True),
        sa.Column('message', sa.Text(), nullable=False),
        sa.Column('payload', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=False), nullable=False),
        sa.ForeignKeyConstraint(['run_id'], ['runs.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id')
    )

    # Create index on source_errors table
    op.create_index('ix_source_errors_run_id', 'source_errors', ['run_id'], unique=False)


def downgrade() -> None:
    """Drop all tables."""
    op.drop_table('source_errors')
    op.drop_table('job_seen')
    op.drop_table('jobs')
    op.drop_table('runs')
