"""Content-free process lifecycle state for reliability roles."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, String, func
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import Base


class ReliabilityRoleHeartbeat(Base):
    """Latest process generation and heartbeat for one expected role."""

    __tablename__ = "reliability_role_heartbeats"
    __table_args__ = (
        CheckConstraint(
            "role IN ('scheduler', 'outbox-relay', 'digest-worker', 'telegram-delivery-worker')",
            name="ck_reliability_role_heartbeats_role",
        ),
        CheckConstraint(
            "state IN ('starting', 'ready', 'stopped', 'failed')",
            name="ck_reliability_role_heartbeats_state",
        ),
        CheckConstraint(
            "length(instance_id) BETWEEN 1 AND 128",
            name="ck_reliability_role_heartbeats_instance_length",
        ),
        CheckConstraint(
            "last_error_code IS NULL OR length(last_error_code) BETWEEN 1 AND 128",
            name="ck_reliability_role_heartbeats_error_length",
        ),
    )

    role: Mapped[str] = mapped_column(String(32), primary_key=True)
    instance_id: Mapped[str] = mapped_column(String(128), nullable=False)
    state: Mapped[str] = mapped_column(String(16), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    heartbeat_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    stopped_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error_code: Mapped[str | None] = mapped_column(String(128), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
