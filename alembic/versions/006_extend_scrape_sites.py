"""Extend scrape_sites table for discovery workflow

Revision ID: 006_extend_scrape_sites
Revises: 005_add_scrape_sites_table
Create Date: 2026-02-17

This migration extends the scrape_sites table with discovery and compliance fields.
Phase 1 (discovery) fields are actively used; Phase 2 (scraping) fields are added
to avoid a second migration later but are NOT used in Phase 1.

Discovery fields:
- source: where company was discovered (seed_csv, hardcoded, etc.)
- detected_ats: ATS platform classification
- detection_probed_at: when ATS probe ran
- selector_hints: auto-detected hints (not production)
- selector_confidence: confidence score 0-1
- discovery_notes: human review notes
- robots_allowed: robots.txt compliance check result
- priority: company priority for weighted calculations

Phase 2 fields (not used in Phase 1):
- fetch_mode: 'static' or 'browser' (replaces requires_js)
- next_scrape_at: explicit scheduling timestamp
- max_failures: auto-disable threshold
- last_error_code: typed error code

Legacy columns (requires_js, site_type, anti_bot_level) are kept for now;
drop in migration 007 after 2 weeks stable production usage.
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB


revision = '006_extend_scrape_sites'
down_revision = '005_add_scrape_sites_table'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add discovery and Phase 2 columns to scrape_sites."""

    # Make careers_url nullable for discovery workflow
    # (companies are added without URLs, then resolved later)
    op.alter_column('scrape_sites', 'careers_url', nullable=True)

    # Discovery fields (Phase 1)
    op.add_column('scrape_sites', sa.Column('source', sa.String(32), nullable=True))
    op.add_column('scrape_sites', sa.Column('detected_ats', sa.String(32), nullable=True))
    op.add_column('scrape_sites', sa.Column('detection_probed_at', sa.DateTime(), nullable=True))
    op.add_column('scrape_sites', sa.Column('selector_hints', JSONB(), nullable=True))
    op.add_column('scrape_sites', sa.Column('selector_confidence', sa.Float(), nullable=True))
    op.add_column('scrape_sites', sa.Column('discovery_notes', sa.Text(), nullable=True))
    op.add_column('scrape_sites', sa.Column('robots_allowed', sa.Boolean(), nullable=True))
    op.add_column('scrape_sites', sa.Column('priority', sa.Integer(), nullable=True))

    # Phase 2 fields (not used in Phase 1, added to avoid second migration)
    op.add_column('scrape_sites', sa.Column('fetch_mode', sa.String(16), server_default='static'))
    op.add_column('scrape_sites', sa.Column('next_scrape_at', sa.DateTime(), nullable=True))
    op.add_column('scrape_sites', sa.Column('max_failures', sa.Integer(), server_default='5'))
    op.add_column('scrape_sites', sa.Column('last_error_code', sa.String(32), nullable=True))

    # Backfill fetch_mode from requires_js for existing rows
    op.execute("""
        UPDATE scrape_sites
        SET fetch_mode = CASE WHEN requires_js THEN 'browser' ELSE 'static' END
        WHERE fetch_mode IS NULL OR fetch_mode = 'static'
    """)

    # Backfill next_scrape_at for existing rows
    op.execute("""
        UPDATE scrape_sites
        SET next_scrape_at = COALESCE(
            last_scraped_at + make_interval(hours => scrape_interval_hours),
            now()
        )
        WHERE next_scrape_at IS NULL
    """)

    # Add constraint for scrape_interval_hours
    op.create_check_constraint(
        'chk_scrape_interval_hours_range',
        'scrape_sites',
        'scrape_interval_hours >= 1 AND scrape_interval_hours <= 168'
    )

    # Add unique constraint for idempotent build-list upserts
    op.create_unique_constraint(
        'uq_scrape_sites_company_source',
        'scrape_sites',
        ['company_name', 'source']
    )

    # Add index for due-query (Phase 2, but added now)
    op.create_index(
        'idx_scrape_sites_due',
        'scrape_sites',
        ['next_scrape_at'],
        postgresql_where=sa.text('enabled = TRUE')
    )

    # Add index for discovery queries
    op.create_index(
        'idx_scrape_sites_detected_ats',
        'scrape_sites',
        ['detected_ats'],
        postgresql_where=sa.text('detected_ats IS NOT NULL')
    )


def downgrade() -> None:
    """Remove discovery and Phase 2 columns."""
    op.drop_index('idx_scrape_sites_detected_ats', table_name='scrape_sites')
    op.drop_index('idx_scrape_sites_due', table_name='scrape_sites')
    op.drop_constraint('uq_scrape_sites_company_source', 'scrape_sites', type_='unique')
    op.drop_constraint('chk_scrape_interval_hours_range', 'scrape_sites', type_='check')

    # Restore NOT NULL on careers_url
    op.alter_column('scrape_sites', 'careers_url', nullable=False)

    op.drop_column('scrape_sites', 'last_error_code')
    op.drop_column('scrape_sites', 'max_failures')
    op.drop_column('scrape_sites', 'next_scrape_at')
    op.drop_column('scrape_sites', 'fetch_mode')
    op.drop_column('scrape_sites', 'priority')
    op.drop_column('scrape_sites', 'robots_allowed')
    op.drop_column('scrape_sites', 'discovery_notes')
    op.drop_column('scrape_sites', 'selector_confidence')
    op.drop_column('scrape_sites', 'selector_hints')
    op.drop_column('scrape_sites', 'detection_probed_at')
    op.drop_column('scrape_sites', 'detected_ats')
    op.drop_column('scrape_sites', 'source')
