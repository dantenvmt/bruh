"""Add pg_trgm GIN indexes for full-text search on title, company, and description.

Revision ID: 009_add_trgm_indexes
Revises: 008_enable_rls_for_new_tables
Create Date: 2026-02-22
"""
from alembic import op


revision = "009_add_trgm_indexes"
down_revision = "008_enable_rls_for_new_tables"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Enable the extension inside a normal transaction — idempotent.
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")

    # CREATE INDEX CONCURRENTLY cannot run inside a transaction block.
    # We drop down to the raw DBAPI connection and set autocommit.
    connection = op.get_bind()
    raw_conn = connection.connection

    # psycopg2 exposes autocommit as a simple attribute.
    prev_autocommit = getattr(raw_conn, "autocommit", False)
    try:
        raw_conn.autocommit = True
        cur = raw_conn.cursor()
        cur.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_jobs_title_trgm "
            "ON jobs USING GIN (title gin_trgm_ops)"
        )
        cur.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_jobs_company_trgm "
            "ON jobs USING GIN (company gin_trgm_ops)"
        )
        cur.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_jobs_description_trgm "
            "ON jobs USING GIN (description gin_trgm_ops)"
        )
        cur.close()
    finally:
        raw_conn.autocommit = prev_autocommit


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_jobs_description_trgm")
    op.execute("DROP INDEX IF EXISTS idx_jobs_company_trgm")
    op.execute("DROP INDEX IF EXISTS idx_jobs_title_trgm")
    op.execute("DROP EXTENSION IF EXISTS pg_trgm")
