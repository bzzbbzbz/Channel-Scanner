"""add transactional outbox events

Revision ID: 0020_outbox_events
Revises: 0019_benchmark_v2_metrics
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "0020_outbox_events"
down_revision = "0019_benchmark_v2_metrics"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "outbox_events",
        sa.Column("event_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("correlation_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("causation_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("event_type", sa.String(length=128), nullable=False),
        sa.Column("event_version", sa.Integer(), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("aggregate_type", sa.String(length=64), nullable=False),
        sa.Column("aggregate_id", sa.String(length=128), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("generation", sa.Integer(), nullable=False),
        sa.Column("topic", sa.String(length=255), nullable=False),
        sa.Column("event_key", sa.String(length=255), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("state", sa.String(length=16), server_default="pending", nullable=False),
        sa.Column("lease_owner", sa.String(length=128), nullable=True),
        sa.Column("lease_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("publication_attempt_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("published_partition", sa.Integer(), nullable=True),
        sa.Column("published_offset", sa.BigInteger(), nullable=True),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.String(length=128), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.CheckConstraint("state IN ('pending', 'publishing', 'published')", name="ck_outbox_events_state"),
        sa.CheckConstraint("event_version >= 1", name="ck_outbox_events_event_version"),
        sa.CheckConstraint("attempt >= 1", name="ck_outbox_events_attempt"),
        sa.CheckConstraint("generation >= 1", name="ck_outbox_events_generation"),
        sa.CheckConstraint("publication_attempt_count >= 0", name="ck_outbox_events_publication_attempt_count"),
        sa.CheckConstraint("published_partition IS NULL OR published_partition >= 0", name="ck_outbox_events_partition"),
        sa.CheckConstraint("published_offset IS NULL OR published_offset >= 0", name="ck_outbox_events_offset"),
        sa.CheckConstraint("last_error IS NULL OR length(last_error) <= 128", name="ck_outbox_events_error_length"),
        sa.CheckConstraint(
            "(state = 'publishing' AND lease_owner IS NOT NULL AND lease_until IS NOT NULL) OR "
            "(state <> 'publishing' AND lease_owner IS NULL AND lease_until IS NULL)",
            name="ck_outbox_events_publishing_lease",
        ),
        sa.CheckConstraint(
            "(state = 'published' AND published_partition IS NOT NULL AND published_offset IS NOT NULL "
            "AND published_at IS NOT NULL) OR "
            "(state <> 'published' AND published_partition IS NULL AND published_offset IS NULL "
            "AND published_at IS NULL)",
            name="ck_outbox_events_publication",
        ),
        sa.PrimaryKeyConstraint("event_id", name="pk_outbox_events"),
    )
    op.create_index("ix_outbox_events_claim", "outbox_events", ["state", "next_attempt_at", "created_at"])
    op.create_index("ix_outbox_events_expired_lease", "outbox_events", ["state", "lease_until"])
    op.create_index("ix_outbox_events_correlation", "outbox_events", ["correlation_id"])


def downgrade() -> None:
    op.drop_index("ix_outbox_events_correlation", table_name="outbox_events")
    op.drop_index("ix_outbox_events_expired_lease", table_name="outbox_events")
    op.drop_index("ix_outbox_events_claim", table_name="outbox_events")
    op.drop_table("outbox_events")
