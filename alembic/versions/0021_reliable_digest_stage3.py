"""add reliable digest inbox, runs, and persisted messages

Revision ID: 0021_reliable_digest_stage3
Revises: 0020_outbox_events
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "0021_reliable_digest_stage3"
down_revision = "0020_outbox_events"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "inbox_events",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("consumer_name", sa.String(128), nullable=False),
        sa.Column("event_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("generation", sa.Integer(), nullable=False),
        sa.Column("state", sa.String(16), server_default="pending", nullable=False),
        sa.Column("lease_owner", sa.String(128), nullable=True),
        sa.Column("lease_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("processing_attempt_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("last_error", sa.String(128), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.CheckConstraint("state IN ('pending', 'processing', 'completed')", name="ck_inbox_events_state"),
        sa.CheckConstraint("attempt >= 1", name="ck_inbox_events_attempt"),
        sa.CheckConstraint("generation >= 1", name="ck_inbox_events_generation"),
        sa.CheckConstraint("processing_attempt_count >= 0", name="ck_inbox_events_processing_attempt_count"),
        sa.CheckConstraint("last_error IS NULL OR length(last_error) <= 128", name="ck_inbox_events_error_length"),
        sa.CheckConstraint("(state = 'processing' AND lease_owner IS NOT NULL AND lease_until IS NOT NULL) OR (state <> 'processing' AND lease_owner IS NULL AND lease_until IS NULL)", name="ck_inbox_events_processing_lease"),
        sa.CheckConstraint("(state = 'completed' AND completed_at IS NOT NULL) OR (state <> 'completed' AND completed_at IS NULL)", name="ck_inbox_events_completion"),
        sa.PrimaryKeyConstraint("id", name="pk_inbox_events"),
        sa.UniqueConstraint("consumer_name", "event_id", name="uq_inbox_events_consumer_event"),
    )
    op.create_index("ix_inbox_events_recovery", "inbox_events", ["consumer_name", "state", "lease_until"])

    op.create_table(
        "digest_runs",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("subscription_id", sa.Integer(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("logical_schedule_slot", sa.DateTime(timezone=True), nullable=False),
        sa.Column("correlation_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("generation", sa.Integer(), server_default="1", nullable=False),
        sa.Column("state", sa.String(24), server_default="pending", nullable=False),
        sa.Column("lease_owner", sa.String(128), nullable=True),
        sa.Column("lease_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("render_attempt_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_error", sa.String(128), nullable=True),
        sa.Column("rendered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.CheckConstraint("state IN ('pending', 'rendering', 'render_retry_wait', 'ready', 'delivering', 'completed', 'failed')", name="ck_digest_runs_state"),
        sa.CheckConstraint("render_attempt_count >= 0", name="ck_digest_runs_render_attempt_count"),
        sa.CheckConstraint("generation >= 1", name="ck_digest_runs_generation"),
        sa.CheckConstraint("last_error IS NULL OR length(last_error) <= 128", name="ck_digest_runs_error_length"),
        sa.CheckConstraint("(state = 'rendering' AND lease_owner IS NOT NULL AND lease_until IS NOT NULL) OR (state <> 'rendering' AND lease_owner IS NULL AND lease_until IS NULL)", name="ck_digest_runs_rendering_lease"),
        sa.CheckConstraint("(state = 'completed' AND completed_at IS NOT NULL) OR (state <> 'completed' AND completed_at IS NULL)", name="ck_digest_runs_completion"),
        sa.CheckConstraint("state NOT IN ('ready', 'delivering', 'completed') OR rendered_at IS NOT NULL", name="ck_digest_runs_rendered_state"),
        sa.ForeignKeyConstraint(["subscription_id"], ["subscriptions.id"], name="fk_digest_runs_subscription_id_subscriptions"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], name="fk_digest_runs_user_id_users"),
        sa.PrimaryKeyConstraint("id", name="pk_digest_runs"),
        sa.UniqueConstraint("correlation_id", name="uq_digest_runs_correlation_id"),
        sa.UniqueConstraint("subscription_id", "logical_schedule_slot", name="uq_digest_runs_subscription_slot"),
    )
    op.create_index("ix_digest_runs_claim", "digest_runs", ["state", "next_attempt_at", "created_at"])
    op.create_index("ix_digest_runs_expired_lease", "digest_runs", ["state", "lease_until"])
    op.create_index("ix_digest_runs_correlation", "digest_runs", ["correlation_id"])

    op.create_table(
        "digest_outbox_messages",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("run_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("chat_id", sa.BigInteger(), nullable=False),
        sa.Column("text", sa.Text(), nullable=False),
        sa.Column("parse_mode", sa.String(16), nullable=True),
        sa.Column("outcomes", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("state", sa.String(16), server_default="pending", nullable=False),
        sa.Column("generation", sa.Integer(), server_default="1", nullable=False),
        sa.Column("attempt_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("lease_owner", sa.String(128), nullable=True),
        sa.Column("lease_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("telegram_message_id", sa.BigInteger(), nullable=True),
        sa.Column("ambiguous_send", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("last_error", sa.String(128), nullable=True),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.CheckConstraint("ordinal >= 0", name="ck_digest_outbox_messages_ordinal"),
        sa.CheckConstraint("state IN ('pending', 'sending', 'retry_wait', 'sent', 'dead_letter')", name="ck_digest_outbox_messages_state"),
        sa.CheckConstraint("attempt_count >= 0", name="ck_digest_outbox_messages_attempt_count"),
        sa.CheckConstraint("generation >= 1", name="ck_digest_outbox_messages_generation"),
        sa.CheckConstraint("parse_mode IS NULL OR parse_mode IN ('HTML')", name="ck_digest_outbox_messages_parse_mode"),
        sa.CheckConstraint("length(text) BETWEEN 1 AND 4096", name="ck_digest_outbox_messages_text_length"),
        sa.CheckConstraint("last_error IS NULL OR length(last_error) <= 128", name="ck_digest_outbox_messages_error_length"),
        sa.CheckConstraint("(state = 'sending' AND lease_owner IS NOT NULL AND lease_until IS NOT NULL) OR (state <> 'sending' AND lease_owner IS NULL AND lease_until IS NULL)", name="ck_digest_outbox_messages_sending_lease"),
        sa.CheckConstraint("(state = 'sent' AND telegram_message_id IS NOT NULL AND sent_at IS NOT NULL) OR (state <> 'sent' AND telegram_message_id IS NULL AND sent_at IS NULL)", name="ck_digest_outbox_messages_sent"),
        sa.ForeignKeyConstraint(["run_id"], ["digest_runs.id"], name="fk_digest_outbox_messages_run_id_digest_runs"),
        sa.PrimaryKeyConstraint("id", name="pk_digest_outbox_messages"),
        sa.UniqueConstraint("run_id", "ordinal", name="uq_digest_outbox_messages_run_ordinal"),
    )
    op.create_index("ix_digest_outbox_messages_claim", "digest_outbox_messages", ["state", "next_attempt_at", "created_at"])
    op.create_index("ix_digest_outbox_messages_expired_lease", "digest_outbox_messages", ["state", "lease_until"])


def downgrade() -> None:
    op.drop_index("ix_digest_outbox_messages_expired_lease", table_name="digest_outbox_messages")
    op.drop_index("ix_digest_outbox_messages_claim", table_name="digest_outbox_messages")
    op.drop_table("digest_outbox_messages")
    op.drop_index("ix_digest_runs_correlation", table_name="digest_runs")
    op.drop_index("ix_digest_runs_expired_lease", table_name="digest_runs")
    op.drop_index("ix_digest_runs_claim", table_name="digest_runs")
    op.drop_table("digest_runs")
    op.drop_index("ix_inbox_events_recovery", table_name="inbox_events")
    op.drop_table("inbox_events")
