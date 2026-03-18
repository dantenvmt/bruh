"""Add job enrichment columns and user analytics tables.

Revision ID: 007_job_enrichment
Revises: 006_extend_scrape_sites
Create Date: 2026-02-22
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "007_job_enrichment"
down_revision = "006_extend_scrape_sites"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # jobs table enrichment fields
    op.add_column("jobs", sa.Column("experience_level", sa.String(length=32), nullable=True))
    op.add_column("jobs", sa.Column("experience_min_years", sa.Integer(), nullable=True))
    op.add_column("jobs", sa.Column("experience_max_years", sa.Integer(), nullable=True))
    op.add_column("jobs", sa.Column("required_skills", postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.add_column("jobs", sa.Column("industry", sa.String(length=64), nullable=True))
    op.add_column("jobs", sa.Column("industry_confidence", sa.Float(), nullable=True))
    op.add_column("jobs", sa.Column("work_mode", sa.String(length=32), nullable=True))
    op.add_column("jobs", sa.Column("role_pop_reasons", postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.add_column("jobs", sa.Column("enrichment_version", sa.Integer(), nullable=True))
    op.add_column("jobs", sa.Column("enrichment_updated_at", sa.DateTime(timezone=False), nullable=True))

    op.create_index("idx_jobs_experience_level", "jobs", ["experience_level"], unique=False)
    op.create_index("idx_jobs_industry", "jobs", ["industry"], unique=False)
    op.create_index("idx_jobs_work_mode", "jobs", ["work_mode"], unique=False)
    op.create_index(
        "idx_jobs_required_skills_gin",
        "jobs",
        ["required_skills"],
        unique=False,
        postgresql_using="gin",
    )
    op.create_index(
        "idx_jobs_role_pop_reasons_gin",
        "jobs",
        ["role_pop_reasons"],
        unique=False,
        postgresql_using="gin",
    )

    op.create_table(
        "user_job_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("user_id", sa.String(length=128), nullable=True),
        sa.Column("guest_session_id", sa.String(length=128), nullable=True),
        sa.Column("event_type", sa.String(length=50), nullable=False),
        sa.Column("surface", sa.String(length=50), nullable=True),
        sa.Column("event_metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("occurred_at", sa.DateTime(timezone=False), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"], ondelete="CASCADE"),
        sa.CheckConstraint(
            "(user_id IS NOT NULL) OR (guest_session_id IS NOT NULL)",
            name="ck_user_job_events_identity_required",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_user_job_events_job_id", "user_job_events", ["job_id"], unique=False)
    op.create_index("idx_user_job_events_user_id", "user_job_events", ["user_id"], unique=False)
    op.create_index(
        "idx_user_job_events_guest_session_id",
        "user_job_events",
        ["guest_session_id"],
        unique=False,
    )
    op.create_index("idx_user_job_events_occurred_at", "user_job_events", ["occurred_at"], unique=False)

    op.create_table(
        "user_saved_jobs",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("user_id", sa.String(length=128), nullable=True),
        sa.Column("guest_session_id", sa.String(length=128), nullable=True),
        sa.Column("saved_at", sa.DateTime(timezone=False), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=False), nullable=False, server_default=sa.text("now()")),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"], ondelete="CASCADE"),
        sa.CheckConstraint(
            "(user_id IS NOT NULL) OR (guest_session_id IS NOT NULL)",
            name="ck_user_saved_jobs_identity_required",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_user_saved_jobs_job_id", "user_saved_jobs", ["job_id"], unique=False)
    op.create_index("idx_user_saved_jobs_user_id", "user_saved_jobs", ["user_id"], unique=False)
    op.create_index(
        "idx_user_saved_jobs_guest_session_id",
        "user_saved_jobs",
        ["guest_session_id"],
        unique=False,
    )
    op.create_index("idx_user_saved_jobs_is_active", "user_saved_jobs", ["is_active"], unique=False)

    op.execute(
        """
        CREATE UNIQUE INDEX uq_user_saved_jobs_user_job
        ON user_saved_jobs (user_id, job_id)
        WHERE user_id IS NOT NULL
        """
    )
    op.execute(
        """
        CREATE UNIQUE INDEX uq_user_saved_jobs_guest_job
        ON user_saved_jobs (guest_session_id, job_id)
        WHERE guest_session_id IS NOT NULL
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS uq_user_saved_jobs_guest_job")
    op.execute("DROP INDEX IF EXISTS uq_user_saved_jobs_user_job")

    op.drop_index("idx_user_saved_jobs_is_active", table_name="user_saved_jobs")
    op.drop_index("idx_user_saved_jobs_guest_session_id", table_name="user_saved_jobs")
    op.drop_index("idx_user_saved_jobs_user_id", table_name="user_saved_jobs")
    op.drop_index("idx_user_saved_jobs_job_id", table_name="user_saved_jobs")
    op.drop_table("user_saved_jobs")

    op.drop_index("idx_user_job_events_occurred_at", table_name="user_job_events")
    op.drop_index("idx_user_job_events_guest_session_id", table_name="user_job_events")
    op.drop_index("idx_user_job_events_user_id", table_name="user_job_events")
    op.drop_index("idx_user_job_events_job_id", table_name="user_job_events")
    op.drop_table("user_job_events")

    op.drop_index("idx_jobs_role_pop_reasons_gin", table_name="jobs")
    op.drop_index("idx_jobs_required_skills_gin", table_name="jobs")
    op.drop_index("idx_jobs_work_mode", table_name="jobs")
    op.drop_index("idx_jobs_industry", table_name="jobs")
    op.drop_index("idx_jobs_experience_level", table_name="jobs")

    op.drop_column("jobs", "enrichment_updated_at")
    op.drop_column("jobs", "enrichment_version")
    op.drop_column("jobs", "role_pop_reasons")
    op.drop_column("jobs", "work_mode")
    op.drop_column("jobs", "industry_confidence")
    op.drop_column("jobs", "industry")
    op.drop_column("jobs", "required_skills")
    op.drop_column("jobs", "experience_max_years")
    op.drop_column("jobs", "experience_min_years")
    op.drop_column("jobs", "experience_level")
