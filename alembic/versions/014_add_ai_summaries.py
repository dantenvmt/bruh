"""Add ai_summary_card, ai_summary_detail, ai_summarized_at to jobs table.

Revision ID: 014_add_ai_summaries
Revises: 013_resume_skill_extract
Create Date: 2026-03-04
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB


revision = "014_add_ai_summaries"
down_revision = "013_resume_skill_extract"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("jobs", sa.Column("ai_summary_card", sa.Text(), nullable=True))
    op.add_column("jobs", sa.Column("ai_summary_detail", JSONB(), nullable=True))
    op.add_column("jobs", sa.Column("ai_summarized_at", sa.DateTime(timezone=False), nullable=True))


def downgrade() -> None:
    op.drop_column("jobs", "ai_summarized_at")
    op.drop_column("jobs", "ai_summary_detail")
    op.drop_column("jobs", "ai_summary_card")
