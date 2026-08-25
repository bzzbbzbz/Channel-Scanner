"""Content-free dead-letter records and append-only replay audit for BL-22."""

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
    UniqueConstraint,
    Uuid,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import Base


class DeadLetterRecord(Base):
    """One terminal work generation or unreadable Kafka offset."""

    __tablename__ = "dead_letter_records"
    __table_args__ = (
        UniqueConstraint("work_type", "entity_ref", "generation", name="uq_dead_letters_work_generation"),
        UniqueConstraint(
            "source_topic",
            "source_partition",
            "source_offset",
            name="uq_dead_letters_source_offset",
        ),
        CheckConstraint(
            "work_type IN ('digest_run', 'digest_message', 'unreadable_event')",
            name="ck_dead_letters_work_type",
        ),
        CheckConstraint(
            "status IN ('open', 'replayed', 'replay_rejected')",
            name="ck_dead_letters_status",
        ),
        CheckConstraint("generation >= 1", name="ck_dead_letters_generation"),
        CheckConstraint("length(terminal_reason) BETWEEN 1 AND 64", name="ck_dead_letters_reason_length"),
        CheckConstraint("length(error_code) BETWEEN 1 AND 128", name="ck_dead_letters_error_length"),
        CheckConstraint(
            "source_partition IS NULL OR source_partition >= 0",
            name="ck_dead_letters_partition",
        ),
        CheckConstraint("source_offset IS NULL OR source_offset >= 0", name="ck_dead_letters_offset"),
        CheckConstraint(
            "work_type <> 'unreadable_event' OR "
            "(source_partition IS NOT NULL AND source_offset IS NOT NULL AND dlq_outbox_event_id IS NULL)",
            name="ck_dead_letters_unreadable_source",
        ),
        Index("ix_dead_letters_list", "status", "last_failed_at", "id"),
        Index("ix_dead_letters_correlation", "correlation_id"),
        Index("ix_dead_letters_run", "run_id"),
        Index("ix_dead_letters_message", "message_id"),
    )

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    source_topic: Mapped[str] = mapped_column(String(255), nullable=False)
    source_event_id: Mapped[UUID | None] = mapped_column(Uuid(as_uuid=True), nullable=True)
    source_partition: Mapped[int | None] = mapped_column(Integer, nullable=True)
    source_offset: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    work_type: Mapped[str] = mapped_column(String(32), nullable=False)
    entity_ref: Mapped[str] = mapped_column(String(512), nullable=False)
    run_id: Mapped[UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("digest_runs.id", ondelete="SET NULL"), nullable=True
    )
    message_id: Mapped[UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("digest_outbox_messages.id", ondelete="SET NULL"), nullable=True
    )
    subscription_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("subscriptions.id", ondelete="SET NULL"), nullable=True
    )
    correlation_id: Mapped[UUID | None] = mapped_column(Uuid(as_uuid=True), nullable=True)
    terminal_reason: Mapped[str] = mapped_column(String(64), nullable=False)
    error_code: Mapped[str] = mapped_column(String(128), nullable=False)
    attempt_summary: Mapped[dict] = mapped_column(JSON().with_variant(JSONB(), "postgresql"), nullable=False)
    first_failed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_failed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    status: Mapped[str] = mapped_column(String(24), nullable=False, default="open", server_default="open")
    generation: Mapped[int] = mapped_column(Integer, nullable=False)
    dlq_outbox_event_id: Mapped[UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("outbox_events.event_id"), nullable=True, unique=True
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


class DeadLetterReplay(Base):
    """Append-only audit result for one administrator replay command."""

    __tablename__ = "dead_letter_replays"
    __table_args__ = (
        UniqueConstraint("dead_letter_id", "idempotency_key", name="uq_dead_letter_replays_key"),
        CheckConstraint(
            "result IN ('replayed', 'replay_rejected')",
            name="ck_dead_letter_replays_result",
        ),
        CheckConstraint("generation >= 1", name="ck_dead_letter_replays_generation"),
        CheckConstraint(
            "error_code IS NULL OR length(error_code) BETWEEN 1 AND 128",
            name="ck_dead_letter_replays_error_length",
        ),
        CheckConstraint(
            "(result = 'replayed' AND outbox_event_id IS NOT NULL AND error_code IS NULL) OR "
            "(result = 'replay_rejected' AND outbox_event_id IS NULL AND error_code IS NOT NULL)",
            name="ck_dead_letter_replays_outcome",
        ),
        Index("ix_dead_letter_replays_record_time", "dead_letter_id", "requested_at"),
    )

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    dead_letter_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("dead_letter_records.id"), nullable=False
    )
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    actor: Mapped[str] = mapped_column(String(128), nullable=False)
    requested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    result: Mapped[str] = mapped_column(String(24), nullable=False)
    generation: Mapped[int] = mapped_column(Integer, nullable=False)
    error_code: Mapped[str | None] = mapped_column(String(128), nullable=True)
    outbox_event_id: Mapped[UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("outbox_events.event_id"), nullable=True, unique=True
    )
