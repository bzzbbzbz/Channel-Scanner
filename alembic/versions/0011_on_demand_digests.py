"""on-demand digests

Revision ID: 0011_on_demand_digests
Revises: 0010_digest_processing_logs
Create Date: 2026-07-18 00:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "0011_on_demand_digests"
down_revision: Union[str, Sequence[str], None] = "0010_digest_processing_logs"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "on_demand_digests",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("subscription_id", sa.Integer(), nullable=False),
        sa.Column("period_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("period_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("prompt_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=16), server_default="generating", nullable=False),
        sa.Column("rendered_messages", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["subscription_id"], ["subscriptions.id"]),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("subscription_id", "period_start", "period_end", "prompt_fingerprint", name="uq_on_demand_digests_request"),
    )
    op.create_index("ix_on_demand_digests_user_subscription", "on_demand_digests", ["user_id", "subscription_id"])


def downgrade() -> None:
    op.drop_index("ix_on_demand_digests_user_subscription", table_name="on_demand_digests")
    op.drop_table("on_demand_digests")
