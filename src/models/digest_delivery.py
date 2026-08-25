"""Digest delivery state for per-subscription post deduplication."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import DateTime, ForeignKey, Index, Integer, String, Text, UniqueConstraint, Uuid, func
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import Base


class DigestDelivery(Base):
    """A post processed by digest delivery for a specific subscription."""

    __tablename__ = "digest_deliveries"
    __table_args__ = (
        UniqueConstraint("subscription_id", "post_id", name="uq_digest_deliveries_subscription_post"),
        Index("ix_digest_deliveries_user_id", "user_id"),
        Index("ix_digest_deliveries_subscription_id", "subscription_id"),
        Index("ix_digest_deliveries_post_id", "post_id"),
        Index("ix_digest_deliveries_digest_run_id", "digest_run_id"),
        Index("ix_digest_deliveries_digest_message_id", "digest_message_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(Integer, ForeignKey("users.id"), nullable=False)
    subscription_id: Mapped[int] = mapped_column(Integer, ForeignKey("subscriptions.id"), nullable=False)
    post_id: Mapped[int] = mapped_column(Integer, ForeignKey("posts.id"), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, server_default="delivered")
    skip_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    summary_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    summary_mode: Mapped[str | None] = mapped_column(String(32), nullable=True)
    summary_model: Mapped[str | None] = mapped_column(String(255), nullable=True)
    prompt_snapshot: Mapped[str | None] = mapped_column(Text, nullable=True)
    digest_run_id: Mapped[UUID | None] = mapped_column(Uuid(as_uuid=True), ForeignKey("digest_runs.id"), nullable=True)
    digest_message_id: Mapped[UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("digest_outbox_messages.id"), nullable=True
    )
    delivered_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(),
    )
