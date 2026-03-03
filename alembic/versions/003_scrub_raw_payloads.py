"""Scrub existing raw_payload data for security

Revision ID: 003_scrub_raw_payloads
Revises: 002_enable_rls
Create Date: 2026-01-31

This migration scrubs all existing raw_payload data from the jobs table.
This is a one-way migration - the data cannot be recovered without a backup.

WARNING: Back up your database before running this migration!

Context:
- The raw_payload field contains complete API responses from job boards
- This data may include PII, internal IDs, or other sensitive information
- We no longer need this data as we extract relevant fields during ingestion
- Setting these to NULL reduces storage and eliminates security/privacy risks
"""
from alembic import op
from sqlalchemy import text


# revision identifiers, used by Alembic.
revision = '003_scrub_raw_payloads'
down_revision = '002_enable_rls'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Scrub all raw_payload data from jobs table."""

    # Count affected rows for logging
    conn = op.get_bind()
    result = conn.execute(text("SELECT COUNT(*) FROM jobs WHERE raw_payload IS NOT NULL"))
    count = result.scalar()

    if count > 0:
        print(f"Scrubbing raw_payload from {count} job records...")

        # Update in batches to avoid locking the entire table on large datasets
        # Process 1000 rows at a time
        batch_size = 1000
        total_updated = 0

        while True:
            result = conn.execute(
                text("""
                    UPDATE jobs
                    SET raw_payload = NULL
                    WHERE id IN (
                        SELECT id
                        FROM jobs
                        WHERE raw_payload IS NOT NULL
                        LIMIT :batch_size
                    )
                """),
                {"batch_size": batch_size},
            )

            rows_updated = result.rowcount
            total_updated += rows_updated

            if rows_updated == 0:
                break

            print(f"Progress: {total_updated}/{count} rows scrubbed...")

        print(f"Successfully scrubbed {total_updated} raw_payload entries.")
    else:
        print("No raw_payload data to scrub - all jobs already have NULL payloads.")


def downgrade() -> None:
    """Cannot restore scrubbed data - this is intentional for security.

    If you need to restore the data, you must use a database backup
    taken before this migration was applied.
    """
    print("=" * 70)
    print("WARNING: raw_payload data has been permanently deleted.")
    print("This migration cannot be reversed without a database backup.")
    print("If you need to restore this data, restore from a backup snapshot.")
    print("=" * 70)
    pass
