"""add content-free dead letters and replay audit

Revision ID: 0023_dead_letters_and_replay
Revises: 0022_reliable_telegram_delivery
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "0023_dead_letters_and_replay"
down_revision = "0022_reliable_telegram_delivery"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "dead_letter_records",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("source_topic", sa.String(255), nullable=False),
        sa.Column("source_event_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("source_partition", sa.Integer(), nullable=True),
        sa.Column("source_offset", sa.BigInteger(), nullable=True),
        sa.Column("work_type", sa.String(32), nullable=False),
        sa.Column("entity_ref", sa.String(512), nullable=False),
        sa.Column("run_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("message_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("subscription_id", sa.Integer(), nullable=True),
        sa.Column("correlation_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("terminal_reason", sa.String(64), nullable=False),
        sa.Column("error_code", sa.String(128), nullable=False),
        sa.Column("attempt_summary", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("first_failed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_failed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.String(24), server_default="open", nullable=False),
        sa.Column("generation", sa.Integer(), nullable=False),
        sa.Column("dlq_outbox_event_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.CheckConstraint("work_type IN ('digest_run', 'digest_message', 'unreadable_event')", name="ck_dead_letters_work_type"),
        sa.CheckConstraint("status IN ('open', 'replayed', 'replay_rejected')", name="ck_dead_letters_status"),
        sa.CheckConstraint("generation >= 1", name="ck_dead_letters_generation"),
        sa.CheckConstraint("length(terminal_reason) BETWEEN 1 AND 64", name="ck_dead_letters_reason_length"),
        sa.CheckConstraint("length(error_code) BETWEEN 1 AND 128", name="ck_dead_letters_error_length"),
        sa.CheckConstraint("source_partition IS NULL OR source_partition >= 0", name="ck_dead_letters_partition"),
        sa.CheckConstraint("source_offset IS NULL OR source_offset >= 0", name="ck_dead_letters_offset"),
        sa.CheckConstraint("work_type <> 'unreadable_event' OR (source_partition IS NOT NULL AND source_offset IS NOT NULL AND dlq_outbox_event_id IS NULL)", name="ck_dead_letters_unreadable_source"),
        sa.ForeignKeyConstraint(["run_id"], ["digest_runs.id"], name="fk_dead_letters_run", ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["message_id"], ["digest_outbox_messages.id"], name="fk_dead_letters_message", ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["subscription_id"], ["subscriptions.id"], name="fk_dead_letters_subscription", ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["dlq_outbox_event_id"], ["outbox_events.event_id"], name="fk_dead_letters_dlq_outbox"),
        sa.PrimaryKeyConstraint("id", name="pk_dead_letter_records"),
        sa.UniqueConstraint("work_type", "entity_ref", "generation", name="uq_dead_letters_work_generation"),
        sa.UniqueConstraint("source_topic", "source_partition", "source_offset", name="uq_dead_letters_source_offset"),
        sa.UniqueConstraint("dlq_outbox_event_id", name="uq_dead_letter_records_dlq_outbox_event_id"),
    )
    op.create_index("ix_dead_letters_list", "dead_letter_records", ["status", "last_failed_at", "id"])
    op.create_index("ix_dead_letters_correlation", "dead_letter_records", ["correlation_id"])
    op.create_index("ix_dead_letters_run", "dead_letter_records", ["run_id"])
    op.create_index("ix_dead_letters_message", "dead_letter_records", ["message_id"])

    op.create_table(
        "dead_letter_replays",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("dead_letter_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("idempotency_key", sa.String(128), nullable=False),
        sa.Column("actor", sa.String(128), nullable=False),
        sa.Column("requested_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("result", sa.String(24), nullable=False),
        sa.Column("generation", sa.Integer(), nullable=False),
        sa.Column("error_code", sa.String(128), nullable=True),
        sa.Column("outbox_event_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.CheckConstraint("result IN ('replayed', 'replay_rejected')", name="ck_dead_letter_replays_result"),
        sa.CheckConstraint("generation >= 1", name="ck_dead_letter_replays_generation"),
        sa.CheckConstraint("error_code IS NULL OR length(error_code) BETWEEN 1 AND 128", name="ck_dead_letter_replays_error_length"),
        sa.CheckConstraint("(result = 'replayed' AND outbox_event_id IS NOT NULL AND error_code IS NULL) OR (result = 'replay_rejected' AND outbox_event_id IS NULL AND error_code IS NOT NULL)", name="ck_dead_letter_replays_outcome"),
        sa.ForeignKeyConstraint(["dead_letter_id"], ["dead_letter_records.id"], name="fk_dead_letter_replays_record"),
        sa.ForeignKeyConstraint(["outbox_event_id"], ["outbox_events.event_id"], name="fk_dead_letter_replays_outbox"),
        sa.PrimaryKeyConstraint("id", name="pk_dead_letter_replays"),
        sa.UniqueConstraint("dead_letter_id", "idempotency_key", name="uq_dead_letter_replays_key"),
        sa.UniqueConstraint("outbox_event_id", name="uq_dead_letter_replays_outbox_event_id"),
    )
    op.create_index("ix_dead_letter_replays_record_time", "dead_letter_replays", ["dead_letter_id", "requested_at"])


def downgrade() -> None:
    op.drop_index("ix_dead_letter_replays_record_time", table_name="dead_letter_replays")
    op.drop_table("dead_letter_replays")
    op.drop_index("ix_dead_letters_message", table_name="dead_letter_records")
    op.drop_index("ix_dead_letters_run", table_name="dead_letter_records")
    op.drop_index("ix_dead_letters_correlation", table_name="dead_letter_records")
    op.drop_index("ix_dead_letters_list", table_name="dead_letter_records")
    op.drop_table("dead_letter_records")
