"""digest processing logs

Revision ID: 0010_digest_processing_logs
Revises: 0009_custom_filter_prompt
Create Date: 2026-07-18 00:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0010_digest_processing_logs"
down_revision: Union[str, Sequence[str], None] = "0009_custom_filter_prompt"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "digest_processing_logs",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("subscription_id", sa.Integer(), nullable=False),
        sa.Column("found_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("filtered_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("included_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.ForeignKeyConstraint(["subscription_id"], ["subscriptions.id"]),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_digest_processing_logs_user_id", "digest_processing_logs", ["user_id"])
    op.create_index(
        "ix_digest_processing_logs_subscription_completed_at",
        "digest_processing_logs",
        ["subscription_id", "completed_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_digest_processing_logs_subscription_completed_at", table_name="digest_processing_logs")
    op.drop_index("ix_digest_processing_logs_user_id", table_name="digest_processing_logs")
    op.drop_table("digest_processing_logs")
