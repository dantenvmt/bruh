"""Enable RLS on new analytics/saved-job tables for Supabase.

Revision ID: 008_enable_rls_for_new_tables
Revises: 007_add_job_enrichment_and_user_analytics
Create Date: 2026-02-22
"""
from alembic import op
import sqlalchemy as sa
import os


revision = "008_enable_rls_for_new_tables"
down_revision = "007_add_job_enrichment_and_user_analytics"
branch_labels = None
depends_on = None


def should_apply_rls(connection) -> bool:
    if os.environ.get("JOB_SCRAPER_FORCE_RLS", "").lower() in ("1", "true", "yes"):
        return True

    postgres_role = connection.execute(sa.text("SELECT 1 FROM pg_roles WHERE rolname = 'postgres'")).fetchone()
    if not postgres_role:
        return False
    service_role = connection.execute(
        sa.text("SELECT 1 FROM pg_roles WHERE rolname = 'service_role'")
    ).fetchone()
    return service_role is not None


def upgrade() -> None:
    connection = op.get_bind()
    if not should_apply_rls(connection):
        print("Skipping RLS migration for new tables: not a Supabase environment")
        return

    new_tables = ["user_job_events", "user_saved_jobs"]
    for table in new_tables:
        op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
        op.execute(
            f"""
            CREATE POLICY "{table}_postgres_all" ON {table}
            FOR ALL
            TO postgres
            USING (true)
            WITH CHECK (true)
            """
        )
        op.execute(
            f"""
            CREATE POLICY "{table}_service_role_all" ON {table}
            FOR ALL
            TO service_role
            USING (true)
            WITH CHECK (true)
            """
        )


def downgrade() -> None:
    connection = op.get_bind()
    if not should_apply_rls(connection):
        print("Skipping RLS downgrade for new tables: not a Supabase environment")
        return

    new_tables = ["user_job_events", "user_saved_jobs"]
    for table in new_tables:
        op.execute(f'DROP POLICY IF EXISTS "{table}_postgres_all" ON {table}')
        op.execute(f'DROP POLICY IF EXISTS "{table}_service_role_all" ON {table}')
        op.execute(f"ALTER TABLE {table} DISABLE ROW LEVEL SECURITY")
