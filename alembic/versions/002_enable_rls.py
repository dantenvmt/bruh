"""Enable Row Level Security (RLS) on all tables for Supabase

Revision ID: 002_enable_rls
Revises: 001_initial_schema
Create Date: 2026-01-31

This migration:
1. Enables RLS on all public tables (runs, jobs, job_seen, source_errors, alembic_version)
2. Creates policies allowing full access for the postgres/service_role
3. This prevents public PostgREST access while allowing your backend to operate normally

Note: Supabase has two key roles:
- 'anon': Used by unauthenticated API requests
- 'authenticated': Used by authenticated API requests
- 'service_role': Has full access, bypasses RLS (used by your backend)
- 'postgres': The superuser role

For a backend-only application (no direct client access), we grant access to
postgres and service_role only. The anon role gets no access.

IMPORTANT: This migration only applies to Supabase environments.
For local/Docker PostgreSQL (without Supabase roles), this migration is skipped.

Override: Set JOB_SCRAPER_FORCE_RLS=1 to enable RLS on non-Supabase databases.
"""
from alembic import op
import sqlalchemy as sa
import os


# revision identifiers, used by Alembic.
revision = '002_enable_rls'
down_revision = '001_initial_schema'
branch_labels = None
depends_on = None


def should_apply_rls(connection) -> bool:
    """Determine if RLS should be applied based on environment."""
    # Allow forcing RLS via environment variable
    if os.environ.get('JOB_SCRAPER_FORCE_RLS', '').lower() in ('1', 'true', 'yes'):
        print("JOB_SCRAPER_FORCE_RLS is set: Forcing RLS migration")
        return True

    # Check if we're in a Supabase environment (has postgres and service_role roles)
    result = connection.execute(sa.text(
        "SELECT 1 FROM pg_roles WHERE rolname = 'postgres'"
    )).fetchone()
    if not result:
        return False

    result = connection.execute(sa.text(
        "SELECT 1 FROM pg_roles WHERE rolname = 'service_role'"
    )).fetchone()
    return result is not None


def upgrade() -> None:
    """Enable RLS on all tables and create service role policies (Supabase only)."""

    # Get the current connection
    connection = op.get_bind()

    # Check if RLS should be applied
    if not should_apply_rls(connection):
        print("Skipping RLS migration: Not a Supabase environment (postgres/service_role roles not found)")
        print("Set JOB_SCRAPER_FORCE_RLS=1 to enable RLS on non-Supabase databases")
        return

    print("Enabling RLS policies")

    # List of application tables
    app_tables = ['runs', 'jobs', 'job_seen', 'source_errors']

    for table in app_tables:
        # Enable RLS on the table
        op.execute(f'ALTER TABLE {table} ENABLE ROW LEVEL SECURITY')

        # Create policy for postgres role (superuser - always has access)
        # Note: postgres role bypasses RLS by default, but explicit policy is cleaner
        op.execute(f'''
            CREATE POLICY "{table}_postgres_all" ON {table}
            FOR ALL
            TO postgres
            USING (true)
            WITH CHECK (true)
        ''')

        # Create policy for service_role (used by backend connections)
        op.execute(f'''
            CREATE POLICY "{table}_service_role_all" ON {table}
            FOR ALL
            TO service_role
            USING (true)
            WITH CHECK (true)
        ''')

    # Handle alembic_version table separately
    # This table is created by Alembic itself, not our migrations
    op.execute('ALTER TABLE alembic_version ENABLE ROW LEVEL SECURITY')
    op.execute('''
        CREATE POLICY "alembic_version_postgres_all" ON alembic_version
        FOR ALL
        TO postgres
        USING (true)
        WITH CHECK (true)
    ''')
    op.execute('''
        CREATE POLICY "alembic_version_service_role_all" ON alembic_version
        FOR ALL
        TO service_role
        USING (true)
        WITH CHECK (true)
    ''')


def downgrade() -> None:
    """Disable RLS and drop policies (Supabase only)."""

    # Get the current connection
    connection = op.get_bind()

    # Check if RLS was applied
    if not should_apply_rls(connection):
        print("Skipping RLS downgrade: RLS was not applied in this environment")
        return

    all_tables = ['runs', 'jobs', 'job_seen', 'source_errors', 'alembic_version']

    for table in all_tables:
        # Drop policies first
        op.execute(f'DROP POLICY IF EXISTS "{table}_postgres_all" ON {table}')
        op.execute(f'DROP POLICY IF EXISTS "{table}_service_role_all" ON {table}')

        # Disable RLS
        op.execute(f'ALTER TABLE {table} DISABLE ROW LEVEL SECURITY')
