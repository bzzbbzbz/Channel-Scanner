"""Post model — stores scraped Telegram posts with full metadata."""

from datetime import datetime
from typing import Any, Optional

from sqlalchemy import (
    BigInteger,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy import JSON
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.models.base import Base


class Post(Base):
    """A scraped Telegram post with full metadata."""

    __tablename__ = "posts"
    __table_args__ = (
        UniqueConstraint("channel_id", "post_id", name="uq_posts_channel_post"),
        Index("ix_posts_channel_id", "channel_id"),
        Index("ix_posts_datetime", "datetime"),
        Index("ix_posts_channel_post_id", "channel_id", "post_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    post_id: Mapped[int] = mapped_column(
        BigInteger, nullable=False,
        comment="Numeric ID from data-post attribute",
    )
    channel_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("channels.id"), nullable=False,
    )
    content: Mapped[str] = mapped_column(
        Text, nullable=False, comment="Post content as Markdown",
    )
    datetime: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, comment="Post publication time",
    )
    views: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True, comment="View count (parsed from '1.5K' format)",
    )
    reactions: Mapped[Optional[dict[str, Any]]] = mapped_column(
        JSON, nullable=True, comment='{"emoji": count}',
    )
    link_preview: Mapped[Optional[dict[str, Any]]] = mapped_column(
        JSON, nullable=True,
        comment='{"title", "site_name", "description", "url"}',
    )
    author: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        server_default=func.now(), onupdate=func.now(),
    )

    # Relationships
    channel: Mapped["Channel"] = relationship(  # noqa: F821
        back_populates="posts", lazy="selectin",
    )
    knowledge_document: Mapped["KnowledgeDocument | None"] = relationship(  # noqa: F821
        back_populates="post", uselist=False, lazy="selectin",
    )

    def __repr__(self) -> str:
        return f"<Post id={self.id} post_id={self.post_id} channel_id={self.channel_id}>"
