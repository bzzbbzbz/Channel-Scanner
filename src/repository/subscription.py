"""Repository for named subscriptions and channel membership."""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from src.models.channel import Channel
from src.models.digest_delivery import DigestDelivery
from src.models.digest_processing_log import DigestProcessingLog
from src.models.on_demand_digest import OnDemandDigest
from src.models.subscription import Subscription, SubscriptionChannel
from src.models.user import DeliveryFrequency, DigestFormat, SummaryMode


class SubscriptionRepository:
    """Manage named subscriptions and their channels."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create_subscription(
        self,
        user_id: int,
        name: str,
        *,
        digest_format: DigestFormat = DigestFormat.SUMMARY,
        summary_mode: SummaryMode = SummaryMode.BRIEF,
        custom_prompt: str | None = None,
        filter_prompt: str | None = None,
        notification_cron: str | None = None,
        frequency: DeliveryFrequency = DeliveryFrequency.DAILY,
        enabled: bool = True,
    ) -> Subscription:
        subscription = Subscription(
            user_id=user_id,
            name=name,
            digest_format=digest_format,
            summary_mode=summary_mode,
            custom_prompt=custom_prompt,
            filter_prompt=filter_prompt,
            notification_cron=notification_cron,
            frequency=frequency,
            enabled=enabled,
        )
        self._session.add(subscription)
        await self._session.flush()
        return subscription

    async def get_by_id(self, subscription_id: int) -> Subscription | None:
        stmt = (
            select(Subscription)
            .options(
                selectinload(Subscription.user),
                selectinload(Subscription.channel_links).selectinload(SubscriptionChannel.channel),
            )
            .where(Subscription.id == subscription_id)
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_for_user(self, user_id: int, subscription_id: int) -> Subscription | None:
        stmt = (
            select(Subscription)
            .options(
                selectinload(Subscription.user),
                selectinload(Subscription.channel_links).selectinload(SubscriptionChannel.channel),
            )
            .where(Subscription.user_id == user_id, Subscription.id == subscription_id)
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def list_for_user(self, user_id: int) -> list[Subscription]:
        stmt = (
            select(Subscription)
            .options(selectinload(Subscription.channel_links).selectinload(SubscriptionChannel.channel))
            .where(Subscription.user_id == user_id)
            .order_by(Subscription.created_at.asc(), Subscription.id.asc())
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def list_enabled(self) -> list[Subscription]:
        stmt = (
            select(Subscription)
            .options(
                selectinload(Subscription.user),
                selectinload(Subscription.channel_links).selectinload(SubscriptionChannel.channel),
            )
            .where(Subscription.enabled.is_(True))
            .order_by(Subscription.id.asc())
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def add_channel(
        self,
        subscription_id: int,
        channel_id: int,
        subscribed_at: datetime | None = None,
    ) -> tuple[SubscriptionChannel, bool]:
        stmt = select(SubscriptionChannel).where(
            SubscriptionChannel.subscription_id == subscription_id,
            SubscriptionChannel.channel_id == channel_id,
        )
        result = await self._session.execute(stmt)
        existing = result.scalar_one_or_none()
        if existing is not None:
            return existing, False

        link = SubscriptionChannel(
            subscription_id=subscription_id,
            channel_id=channel_id,
            subscribed_at=subscribed_at,
        )
        self._session.add(link)
        await self._session.flush()
        return link, True

    async def remove_channel(self, subscription_id: int, channel_id: int) -> bool:
        stmt = delete(SubscriptionChannel).where(
            SubscriptionChannel.subscription_id == subscription_id,
            SubscriptionChannel.channel_id == channel_id,
        )
        result = await self._session.execute(stmt)
        await self._session.flush()
        return bool(result.rowcount)

    async def list_channels(self, subscription_id: int) -> list[Channel]:
        stmt = (
            select(Channel)
            .join(SubscriptionChannel, SubscriptionChannel.channel_id == Channel.id)
            .where(SubscriptionChannel.subscription_id == subscription_id)
            .order_by(Channel.username.asc())
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def rename(self, subscription: Subscription, name: str) -> Subscription:
        subscription.name = name
        subscription.updated_at = datetime.now(timezone.utc)
        await self._session.flush()
        return subscription

    async def update_digest_format(self, subscription: Subscription, digest_format: DigestFormat) -> Subscription:
        subscription.digest_format = digest_format
        subscription.updated_at = datetime.now(timezone.utc)
        await self._session.flush()
        return subscription

    async def update_summary_mode(self, subscription: Subscription, summary_mode: SummaryMode) -> Subscription:
        subscription.summary_mode = summary_mode
        subscription.updated_at = datetime.now(timezone.utc)
        await self._session.flush()
        return subscription

    async def update_custom_prompt(self, subscription: Subscription, custom_prompt: str | None) -> Subscription:
        subscription.custom_prompt = custom_prompt
        subscription.updated_at = datetime.now(timezone.utc)
        await self._session.flush()
        return subscription

    async def update_filter_prompt(self, subscription: Subscription, filter_prompt: str | None) -> Subscription:
        subscription.filter_prompt = filter_prompt
        subscription.updated_at = datetime.now(timezone.utc)
        await self._session.flush()
        return subscription

    async def reset_prompts(self, subscription: Subscription) -> Subscription:
        subscription.custom_prompt = None
        subscription.filter_prompt = None
        subscription.summary_mode = SummaryMode.BRIEF
        subscription.digest_format = DigestFormat.SUMMARY
        subscription.updated_at = datetime.now(timezone.utc)
        await self._session.flush()
        return subscription

    async def update_frequency(
        self,
        subscription: Subscription,
        frequency: DeliveryFrequency,
        notification_cron: str | None = None,
    ) -> Subscription:
        subscription.frequency = frequency
        subscription.notification_cron = notification_cron
        subscription.updated_at = datetime.now(timezone.utc)
        await self._session.flush()
        return subscription

    async def update_notification_cron(self, subscription: Subscription, notification_cron: str) -> Subscription:
        subscription.notification_cron = notification_cron
        subscription.updated_at = datetime.now(timezone.utc)
        await self._session.flush()
        return subscription

    async def update_enabled(self, subscription: Subscription, enabled: bool) -> Subscription:
        subscription.enabled = enabled
        subscription.updated_at = datetime.now(timezone.utc)
        await self._session.flush()
        return subscription

    async def mark_digest_sent(self, subscription: Subscription, sent_at: datetime) -> Subscription:
        subscription.last_digest_at = sent_at
        subscription.updated_at = datetime.now(timezone.utc)
        await self._session.flush()
        return subscription

    async def delete(self, subscription_id: int) -> bool:
        await self._session.execute(delete(DigestDelivery).where(DigestDelivery.subscription_id == subscription_id))
        await self._session.execute(delete(DigestProcessingLog).where(DigestProcessingLog.subscription_id == subscription_id))
        await self._session.execute(delete(OnDemandDigest).where(OnDemandDigest.subscription_id == subscription_id))
        await self._session.execute(delete(SubscriptionChannel).where(SubscriptionChannel.subscription_id == subscription_id))
        result = await self._session.execute(delete(Subscription).where(Subscription.id == subscription_id))
        await self._session.flush()
        return bool(result.rowcount)

    async def count_for_user(self, user_id: int) -> int:
        stmt = select(Subscription.id).where(Subscription.user_id == user_id).order_by(Subscription.id.asc())
        result = await self._session.execute(stmt)
        return len(result.scalars().all())

    async def clear_user_delivery_state(self, user_id: int) -> None:
        await self._session.execute(
            update(Subscription)
            .where(Subscription.user_id == user_id)
            .values(last_digest_at=None, updated_at=datetime.now(timezone.utc))
        )
        await self._session.flush()
