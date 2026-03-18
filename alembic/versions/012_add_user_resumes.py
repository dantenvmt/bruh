"""Add user_resumes table for resume upload and LLM optimization.

Revision ID: 012_add_user_resumes
Revises: 011_api_endpoint_scrape_sites
Create Date: 2026-02-26
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "012_add_user_resumes"
down_revision = "011_api_endpoint_scrape_sites"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "user_resumes",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("user_id", sa.String(length=128), nullable=False),
        sa.Column("raw_text", sa.Text(), nullable=False),
        sa.Column("filename", sa.String(length=256), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=False), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=False), nullable=False, server_default=sa.text("now()")),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_user_resumes_user_id", "user_resumes", ["user_id"], unique=False)
    op.create_index("idx_user_resumes_user_id_created", "user_resumes", ["user_id", "created_at"], unique=False)


def downgrade() -> None:
    op.drop_index("idx_user_resumes_user_id_created", table_name="user_resumes")
    op.drop_index("idx_user_resumes_user_id", table_name="user_resumes")
    op.drop_table("user_resumes")
