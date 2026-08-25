"""Transactional Kafka outbox records for BL-22."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import BigInteger, CheckConstraint, DateTime, Index, Integer, JSON, String, Uuid, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import Base


class OutboxEvent(Base):
    """One immutable event envelope plus mutable publication state."""

    __tablename__ = "outbox_events"
    __table_args__ = (
        CheckConstraint("state IN ('pending', 'publishing', 'published')", name="ck_outbox_events_state"),
        CheckConstraint("event_version >= 1", name="ck_outbox_events_event_version"),
        CheckConstraint("attempt >= 1", name="ck_outbox_events_attempt"),
        CheckConstraint("generation >= 1", name="ck_outbox_events_generation"),
        CheckConstraint("publication_attempt_count >= 0", name="ck_outbox_events_publication_attempt_count"),
        CheckConstraint("published_partition IS NULL OR published_partition >= 0", name="ck_outbox_events_partition"),
        CheckConstraint("published_offset IS NULL OR published_offset >= 0", name="ck_outbox_events_offset"),
        CheckConstraint("last_error IS NULL OR length(last_error) <= 128", name="ck_outbox_events_error_length"),
        CheckConstraint(
            "(state = 'publishing' AND lease_owner IS NOT NULL AND lease_until IS NOT NULL) OR "
            "(state <> 'publishing' AND lease_owner IS NULL AND lease_until IS NULL)",
            name="ck_outbox_events_publishing_lease",
        ),
        CheckConstraint(
            "(state = 'published' AND published_partition IS NOT NULL AND published_offset IS NOT NULL "
            "AND published_at IS NOT NULL) OR "
            "(state <> 'published' AND published_partition IS NULL AND published_offset IS NULL "
            "AND published_at IS NULL)",
            name="ck_outbox_events_publication",
        ),
        Index("ix_outbox_events_claim", "state", "next_attempt_at", "created_at"),
        Index("ix_outbox_events_expired_lease", "state", "lease_until"),
        Index("ix_outbox_events_correlation", "correlation_id"),
    )

    event_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    correlation_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)
    causation_id: Mapped[UUID | None] = mapped_column(Uuid(as_uuid=True), nullable=True)
    event_type: Mapped[str] = mapped_column(String(128), nullable=False)
    event_version: Mapped[int] = mapped_column(Integer, nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    aggregate_type: Mapped[str] = mapped_column(String(64), nullable=False)
    aggregate_id: Mapped[str] = mapped_column(String(128), nullable=False)
    attempt: Mapped[int] = mapped_column(Integer, nullable=False)
    generation: Mapped[int] = mapped_column(Integer, nullable=False)
    topic: Mapped[str] = mapped_column(String(255), nullable=False)
    event_key: Mapped[str] = mapped_column(String(255), nullable=False)
    payload: Mapped[dict] = mapped_column(JSON().with_variant(JSONB(), "postgresql"), nullable=False)

    state: Mapped[str] = mapped_column(String(16), nullable=False, default="pending", server_default="pending")
    lease_owner: Mapped[str | None] = mapped_column(String(128), nullable=True)
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    publication_attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    next_attempt_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    published_partition: Mapped[int | None] = mapped_column(Integer, nullable=True)
    published_offset: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[str | None] = mapped_column(String(128), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
