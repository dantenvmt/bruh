"""Add api_endpoint JSONB column to scrape_sites.

Stores the NetworkSpy-discovered JSON API endpoint config for sites where
fetch_mode='api_spy'.  The column holds: url, method, replay_headers,
request_post_data, pagination (style/param_name/current_value/in_body),
and confidence.

Revision ID: 011_add_api_endpoint_to_scrape_sites
Revises: 010_add_composite_indexes
Create Date: 2026-02-26
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB


revision = "011_add_api_endpoint_to_scrape_sites"
down_revision = "010_add_composite_indexes"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "scrape_sites",
        sa.Column("api_endpoint", JSONB, nullable=True),
    )


def downgrade() -> None:
    op.drop_column("scrape_sites", "api_endpoint")
