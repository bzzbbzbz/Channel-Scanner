"""Telegram user model with persisted bot preferences."""

from __future__ import annotations

from datetime import datetime
from enum import Enum as PyEnum
from typing import TYPE_CHECKING, Optional

from sqlalchemy import BigInteger, DateTime, Enum, Index, Integer, String, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.models.base import Base

if TYPE_CHECKING:
    from src.models.subscription import Subscription


class DigestFormat(str, PyEnum):
    """Supported digest formats."""

    SHORT = "short"
    SUMMARY = "summary"


class SummaryMode(str, PyEnum):
    """Supported LLM summary modes."""

    BRIEF = "brief"
    DETAILED = "detailed"
    CUSTOM = "custom"


class DeliveryFrequency(str, PyEnum):
    """Supported digest delivery frequencies."""

    HOURLY = "hourly"
    DAILY = "daily"


class User(Base):
    """Telegram bot user with stored preferences."""

    __tablename__ = "users"
    __table_args__ = (
        Index("ix_users_chat_id", "chat_id"),
        Index("ix_users_telegram_user_id", "telegram_user_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    telegram_user_id: Mapped[int] = mapped_column(BigInteger, unique=True, nullable=False)
    chat_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    chat_type: Mapped[str] = mapped_column(String(32), nullable=False, default="private")
    username: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    first_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    last_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    timezone: Mapped[str] = mapped_column(String(64), nullable=False, default="UTC", server_default="UTC")
    language: Mapped[str] = mapped_column(String(8), nullable=False, default="ru", server_default="ru")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        server_default=func.now(), onupdate=func.now(),
    )

    subscriptions: Mapped[list["Subscription"]] = relationship(
        back_populates="user", lazy="selectin",
    )
