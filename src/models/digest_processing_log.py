"""Completed digest processing run statistics."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import DateTime, ForeignKey, Index, Integer, UniqueConstraint, Uuid, func
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import Base


class DigestProcessingLog(Base):
    """Aggregate post outcomes for one completed subscription processing run."""

    __tablename__ = "digest_processing_logs"
    __table_args__ = (
        Index("ix_digest_processing_logs_user_id", "user_id"),
        Index("ix_digest_processing_logs_subscription_completed_at", "subscription_id", "completed_at"),
        UniqueConstraint("digest_run_id", name="uq_digest_processing_logs_digest_run_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(Integer, ForeignKey("users.id"), nullable=False)
    subscription_id: Mapped[int] = mapped_column(Integer, ForeignKey("subscriptions.id"), nullable=False)
    found_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    filtered_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    included_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    digest_run_id: Mapped[UUID | None] = mapped_column(Uuid(as_uuid=True), ForeignKey("digest_runs.id"), nullable=True)
    completed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(),
    )
