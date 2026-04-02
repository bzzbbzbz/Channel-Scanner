"""Channel model — tracks Telegram channels to scrape."""

from datetime import datetime
from enum import Enum as PyEnum
from typing import TYPE_CHECKING, Optional

from sqlalchemy import (
    BigInteger,
    DateTime,
    Enum,
    Index,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.models.base import Base

if TYPE_CHECKING:
    from src.models.post import Post


class ChannelStatus(str, PyEnum):
    """Channel scraping status."""

    ACTIVE = "active"
    ERROR = "error"
    PAUSED = "paused"


class Channel(Base):
    """Telegram channel being tracked for scraping."""

    __tablename__ = "channels"
    __table_args__ = (
        Index("ix_channels_telegram_id", "telegram_id"),
        Index("ix_channels_status", "status"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    telegram_id: Mapped[int] = mapped_column(
        BigInteger, unique=True, nullable=False,
        comment="Numeric channel ID (survives username changes)",
    )
    username: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True, comment="Channel username (can change)",
    )
    name: Mapped[Optional[str]] = mapped_column(
        String(500), nullable=True, comment="Display name",
    )
    status: Mapped[str] = mapped_column(
        Enum(ChannelStatus, name="channel_status", create_constraint=True),
        nullable=False,
        default=ChannelStatus.ACTIVE,
        server_default=ChannelStatus.ACTIVE.value,
    )
    last_scraped: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True,
    )
    last_error: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True, comment="Last error message if status=error",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        server_default=func.now(), onupdate=func.now(),
    )

    # Relationships
    posts: Mapped[list["Post"]] = relationship(
        back_populates="channel", lazy="selectin",
    )

    def __repr__(self) -> str:
        return (
            f"<Channel id={self.id} telegram_id={self.telegram_id}"
            f" username={self.username!r} status={self.status}>"
        )
