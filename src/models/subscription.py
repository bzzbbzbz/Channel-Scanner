"""Named user subscriptions and their channel links."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.models.base import Base
from src.models.user import DeliveryFrequency, DigestFormat, SummaryMode
from sqlalchemy import Enum, String, Text


class Subscription(Base):
    """Named digest subscription owned by one user."""

    __tablename__ = "subscriptions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(Integer, ForeignKey("users.id"), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    digest_format: Mapped[DigestFormat] = mapped_column(
        Enum(
            DigestFormat,
            name="digest_format",
            create_constraint=True,
            values_callable=lambda enum_cls: [member.value for member in enum_cls],
        ),
        nullable=False,
        default=DigestFormat.SUMMARY,
        server_default=DigestFormat.SUMMARY.value,
    )
    summary_mode: Mapped[SummaryMode] = mapped_column(
        Enum(
            SummaryMode,
            name="summary_mode",
            create_constraint=True,
            values_callable=lambda enum_cls: [member.value for member in enum_cls],
        ),
        nullable=False,
        default=SummaryMode.BRIEF,
        server_default=SummaryMode.BRIEF.value,
    )
    custom_prompt: Mapped[str | None] = mapped_column(Text, nullable=True)
    filter_prompt: Mapped[str | None] = mapped_column(Text, nullable=True)
    notification_cron: Mapped[str | None] = mapped_column(String(64), nullable=True)
    frequency: Mapped[DeliveryFrequency] = mapped_column(
        Enum(
            DeliveryFrequency,
            name="delivery_frequency",
            create_constraint=True,
            values_callable=lambda enum_cls: [member.value for member in enum_cls],
        ),
        nullable=False,
        default=DeliveryFrequency.DAILY,
        server_default=DeliveryFrequency.DAILY.value,
    )
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="true")
    last_digest_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        server_default=func.now(), onupdate=func.now(),
    )

    user = relationship("User", back_populates="subscriptions", lazy="selectin")
    channel_links: Mapped[list["SubscriptionChannel"]] = relationship(
        back_populates="subscription", lazy="selectin", cascade="all, delete-orphan",
    )


class SubscriptionChannel(Base):
    """Channel membership inside a named subscription."""

    __tablename__ = "subscription_channels"
    __table_args__ = (
        UniqueConstraint("subscription_id", "channel_id", name="uq_subscription_channels_subscription_channel"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    subscription_id: Mapped[int] = mapped_column(Integer, ForeignKey("subscriptions.id"), nullable=False)
    channel_id: Mapped[int] = mapped_column(Integer, ForeignKey("channels.id"), nullable=False)
    subscribed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(),
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(),
    )

    subscription = relationship("Subscription", back_populates="channel_links", lazy="selectin")
    channel = relationship("Channel", back_populates="subscription_links", lazy="selectin")
