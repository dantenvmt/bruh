"""Add extracted_skills and extracted_experience_years to user_resumes.

Revision ID: 013_resume_skill_extract
Revises: 012_add_user_resumes
Create Date: 2026-02-26
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB


revision = "013_resume_skill_extract"
down_revision = "012_add_user_resumes"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("user_resumes", sa.Column("extracted_skills", JSONB, nullable=True))
    op.add_column("user_resumes", sa.Column("extracted_experience_years", sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column("user_resumes", "extracted_experience_years")
    op.drop_column("user_resumes", "extracted_skills")
