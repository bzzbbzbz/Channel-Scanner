"""Durable inbox, digest run, and rendered-message state for BL-22."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    JSON,
    String,
    Text,
    UniqueConstraint,
    Uuid,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import Base


class InboxEvent(Base):
    """One consumer's durable processing outcome for a Kafka event."""

    __tablename__ = "inbox_events"
    __table_args__ = (
        UniqueConstraint("consumer_name", "event_id", name="uq_inbox_events_consumer_event"),
        CheckConstraint("state IN ('pending', 'processing', 'completed')", name="ck_inbox_events_state"),
        CheckConstraint("attempt >= 1", name="ck_inbox_events_attempt"),
        CheckConstraint("generation >= 1", name="ck_inbox_events_generation"),
        CheckConstraint("processing_attempt_count >= 0", name="ck_inbox_events_processing_attempt_count"),
        CheckConstraint("last_error IS NULL OR length(last_error) <= 128", name="ck_inbox_events_error_length"),
        CheckConstraint(
            "(state = 'processing' AND lease_owner IS NOT NULL AND lease_until IS NOT NULL) OR "
            "(state <> 'processing' AND lease_owner IS NULL AND lease_until IS NULL)",
            name="ck_inbox_events_processing_lease",
        ),
        CheckConstraint(
            "(state = 'completed' AND completed_at IS NOT NULL) OR "
            "(state <> 'completed' AND completed_at IS NULL)",
            name="ck_inbox_events_completion",
        ),
        Index("ix_inbox_events_recovery", "consumer_name", "state", "lease_until"),
    )

    id: Mapped[int] = mapped_column(BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True)
    consumer_name: Mapped[str] = mapped_column(String(128), nullable=False)
    event_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)
    attempt: Mapped[int] = mapped_column(Integer, nullable=False)
    generation: Mapped[int] = mapped_column(Integer, nullable=False)
    state: Mapped[str] = mapped_column(String(16), nullable=False, default="pending", server_default="pending")
    lease_owner: Mapped[str | None] = mapped_column(String(128), nullable=True)
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    processing_attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    last_error: Mapped[str | None] = mapped_column(String(128), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


class DigestRun(Base):
    """One idempotent scheduled digest rendering and delivery run."""

    __tablename__ = "digest_runs"
    __table_args__ = (
        UniqueConstraint("subscription_id", "logical_schedule_slot", name="uq_digest_runs_subscription_slot"),
        CheckConstraint(
            "state IN ('pending', 'rendering', 'render_retry_wait', 'ready', 'delivering', 'completed', 'failed')",
            name="ck_digest_runs_state",
        ),
        CheckConstraint("render_attempt_count >= 0", name="ck_digest_runs_render_attempt_count"),
        CheckConstraint("generation >= 1", name="ck_digest_runs_generation"),
        CheckConstraint("last_error IS NULL OR length(last_error) <= 128", name="ck_digest_runs_error_length"),
        CheckConstraint(
            "(state = 'rendering' AND lease_owner IS NOT NULL AND lease_until IS NOT NULL) OR "
            "(state <> 'rendering' AND lease_owner IS NULL AND lease_until IS NULL)",
            name="ck_digest_runs_rendering_lease",
        ),
        CheckConstraint(
            "(state = 'completed' AND completed_at IS NOT NULL) OR "
            "(state <> 'completed' AND completed_at IS NULL)",
            name="ck_digest_runs_completion",
        ),
        CheckConstraint(
            "state NOT IN ('ready', 'delivering', 'completed') OR rendered_at IS NOT NULL",
            name="ck_digest_runs_rendered_state",
        ),
        Index("ix_digest_runs_claim", "state", "next_attempt_at", "created_at"),
        Index("ix_digest_runs_expired_lease", "state", "lease_until"),
        Index("ix_digest_runs_correlation", "correlation_id"),
    )

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    subscription_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("subscriptions.id", ondelete="CASCADE"), nullable=False
    )
    user_id: Mapped[int] = mapped_column(Integer, ForeignKey("users.id"), nullable=False)
    logical_schedule_slot: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    correlation_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), nullable=False, unique=True)
    generation: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default="1")
    state: Mapped[str] = mapped_column(String(24), nullable=False, default="pending", server_default="pending")
    lease_owner: Mapped[str | None] = mapped_column(String(128), nullable=True)
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    render_attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    next_attempt_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_error: Mapped[str | None] = mapped_column(String(128), nullable=True)
    rendered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


class DigestOutboxMessage(Base):
    """A rendered Telegram message persisted before any delivery request."""

    __tablename__ = "digest_outbox_messages"
    __table_args__ = (
        UniqueConstraint("run_id", "ordinal", name="uq_digest_outbox_messages_run_ordinal"),
        CheckConstraint("ordinal >= 0", name="ck_digest_outbox_messages_ordinal"),
        CheckConstraint(
            "state IN ('pending', 'sending', 'retry_wait', 'sent', 'dead_letter')",
            name="ck_digest_outbox_messages_state",
        ),
        CheckConstraint("attempt_count >= 0", name="ck_digest_outbox_messages_attempt_count"),
        CheckConstraint("generation >= 1", name="ck_digest_outbox_messages_generation"),
        CheckConstraint("parse_mode IS NULL OR parse_mode IN ('HTML')", name="ck_digest_outbox_messages_parse_mode"),
        CheckConstraint("length(text) BETWEEN 1 AND 4096", name="ck_digest_outbox_messages_text_length"),
        CheckConstraint("last_error IS NULL OR length(last_error) <= 128", name="ck_digest_outbox_messages_error_length"),
        CheckConstraint(
            "(state = 'sending' AND lease_owner IS NOT NULL AND lease_until IS NOT NULL) OR "
            "(state <> 'sending' AND lease_owner IS NULL AND lease_until IS NULL)",
            name="ck_digest_outbox_messages_sending_lease",
        ),
        CheckConstraint(
            "(state = 'sent' AND telegram_message_id IS NOT NULL AND sent_at IS NOT NULL) OR "
            "(state <> 'sent' AND telegram_message_id IS NULL AND sent_at IS NULL)",
            name="ck_digest_outbox_messages_sent",
        ),
        Index("ix_digest_outbox_messages_claim", "state", "next_attempt_at", "created_at"),
        Index("ix_digest_outbox_messages_expired_lease", "state", "lease_until"),
    )

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    run_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("digest_runs.id", ondelete="CASCADE"), nullable=False
    )
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
    chat_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    text: Mapped[str] = mapped_column(Text, nullable=False)
    parse_mode: Mapped[str | None] = mapped_column(String(16), nullable=True)
    outcomes: Mapped[list[dict]] = mapped_column(JSON().with_variant(JSONB(), "postgresql"), nullable=False)
    state: Mapped[str] = mapped_column(String(16), nullable=False, default="pending", server_default="pending")
    generation: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default="1")
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    next_attempt_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    lease_owner: Mapped[str | None] = mapped_column(String(128), nullable=True)
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    telegram_message_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    ambiguous_send: Mapped[bool] = mapped_column(nullable=False, default=False, server_default="false")
    last_error: Mapped[str | None] = mapped_column(String(128), nullable=True)
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
