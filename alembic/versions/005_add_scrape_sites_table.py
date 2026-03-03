"""Add scrape_sites table for career site scraping

Revision ID: 005_add_scrape_sites_table
Revises: 004_add_run_sources_table
Create Date: 2026-02-15

This migration adds the scrape_sites table to store configuration for
direct career site scraping. Each row represents a company careers page
with its scraping configuration (CSS selectors, JS requirements, etc.).

Features:
- Per-site CSS selectors stored as JSONB
- JavaScript rendering flag for Playwright vs httpx
- Anti-bot level for routing to appropriate extraction strategy
- Auto-disable after consecutive failures
- Interval-based scheduling
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB


revision = '005_add_scrape_sites_table'
down_revision = '004_add_run_sources_table'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create scrape_sites table with indexes."""
    op.create_table(
        'scrape_sites',
        sa.Column('id', UUID(as_uuid=True), nullable=False, server_default=sa.text('gen_random_uuid()')),
        sa.Column('company_name', sa.String(256), nullable=False),
        sa.Column('careers_url', sa.Text(), nullable=False),
        sa.Column('site_type', sa.String(32), server_default='custom'),
        sa.Column('requires_js', sa.Boolean(), server_default='false'),
        sa.Column('anti_bot_level', sa.String(16), server_default='none'),
        sa.Column('selectors', JSONB(), server_default='{}'),
        sa.Column('scrape_interval_hours', sa.Integer(), server_default='24'),
        sa.Column('enabled', sa.Boolean(), server_default='true'),
        sa.Column('last_scraped_at', sa.DateTime(), nullable=True),
        sa.Column('last_success_at', sa.DateTime(), nullable=True),
        sa.Column('consecutive_failures', sa.Integer(), server_default='0'),
        sa.Column('last_error', sa.Text(), nullable=True),
        sa.Column('notes', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('careers_url', name='uq_scrape_sites_careers_url'),
    )

    # Index for querying enabled sites
    op.create_index(
        'idx_scrape_sites_enabled',
        'scrape_sites',
        ['enabled'],
        postgresql_where=sa.text('enabled = TRUE'),
    )

    # Index for scheduling (find sites due for scraping)
    op.create_index(
        'idx_scrape_sites_next_scrape',
        'scrape_sites',
        ['last_scraped_at'],
        postgresql_where=sa.text('enabled = TRUE'),
    )


def downgrade() -> None:
    """Drop scrape_sites table and indexes."""
    op.drop_index('idx_scrape_sites_next_scrape', table_name='scrape_sites')
    op.drop_index('idx_scrape_sites_enabled', table_name='scrape_sites')
    op.drop_table('scrape_sites')
