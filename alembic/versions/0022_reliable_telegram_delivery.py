"""link reliable Telegram delivery outcomes to runs and messages

Revision ID: 0022_reliable_telegram_delivery
Revises: 0021_reliable_digest_stage3
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "0022_reliable_telegram_delivery"
down_revision = "0021_reliable_digest_stage3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "digest_deliveries",
        sa.Column("digest_run_id", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.add_column(
        "digest_deliveries",
        sa.Column("digest_message_id", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.create_foreign_key(
        "fk_digest_deliveries_digest_run_id_digest_runs",
        "digest_deliveries",
        "digest_runs",
        ["digest_run_id"],
        ["id"],
    )
    op.create_foreign_key(
        "fk_digest_deliveries_digest_message_id_digest_outbox_messages",
        "digest_deliveries",
        "digest_outbox_messages",
        ["digest_message_id"],
        ["id"],
    )
    op.create_index("ix_digest_deliveries_digest_run_id", "digest_deliveries", ["digest_run_id"])
    op.create_index("ix_digest_deliveries_digest_message_id", "digest_deliveries", ["digest_message_id"])

    op.add_column(
        "digest_processing_logs",
        sa.Column("digest_run_id", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.create_foreign_key(
        "fk_digest_processing_logs_digest_run_id_digest_runs",
        "digest_processing_logs",
        "digest_runs",
        ["digest_run_id"],
        ["id"],
    )
    op.create_unique_constraint(
        "uq_digest_processing_logs_digest_run_id",
        "digest_processing_logs",
        ["digest_run_id"],
    )


def downgrade() -> None:
    op.drop_constraint(
        "uq_digest_processing_logs_digest_run_id",
        "digest_processing_logs",
        type_="unique",
    )
    op.drop_constraint(
        "fk_digest_processing_logs_digest_run_id_digest_runs",
        "digest_processing_logs",
        type_="foreignkey",
    )
    op.drop_column("digest_processing_logs", "digest_run_id")

    op.drop_index("ix_digest_deliveries_digest_message_id", table_name="digest_deliveries")
    op.drop_index("ix_digest_deliveries_digest_run_id", table_name="digest_deliveries")
    op.drop_constraint(
        "fk_digest_deliveries_digest_message_id_digest_outbox_messages",
        "digest_deliveries",
        type_="foreignkey",
    )
    op.drop_constraint(
        "fk_digest_deliveries_digest_run_id_digest_runs",
        "digest_deliveries",
        type_="foreignkey",
    )
    op.drop_column("digest_deliveries", "digest_message_id")
    op.drop_column("digest_deliveries", "digest_run_id")
