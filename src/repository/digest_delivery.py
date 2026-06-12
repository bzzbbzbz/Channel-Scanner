"""Repository for digest delivery selection and dedup state."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import and_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.channel import Channel
from src.models.digest_delivery import DigestDelivery
from src.models.post import Post
from src.models.subscription import SubscriptionChannel


@dataclass(slots=True)
class PendingDigestPost:
    """A post ready to be rendered into a user digest."""

    post_db_id: int
    telegram_post_id: int
    channel_username: str | None
    content: str
    published_at: datetime


@dataclass(slots=True)
class DeliveredSummary:
    """Persisted processing metadata for a delivered or skipped post."""

    post_id: int
    summary_text: str | None
    summary_mode: str | None
    summary_model: str | None
    prompt_snapshot: str | None
    status: str = "delivered"
    skip_reason: str | None = None


class DigestDeliveryRepository:
    """Select undelivered posts and persist successful deliveries."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_pending_posts_for_subscription(self, subscription_id: int) -> list[PendingDigestPost]:
        stmt = (
            select(Post.id, Post.post_id, Channel.username, Post.content, Post.datetime)
            .join(Channel, Channel.id == Post.channel_id)
            .join(SubscriptionChannel, SubscriptionChannel.channel_id == Channel.id)
            .outerjoin(
                DigestDelivery,
                and_(DigestDelivery.subscription_id == subscription_id, DigestDelivery.post_id == Post.id),
            )
            .where(
                SubscriptionChannel.subscription_id == subscription_id,
                DigestDelivery.id.is_(None),
                Post.datetime >= SubscriptionChannel.subscribed_at,
            )
            .order_by(Post.datetime.asc(), Post.id.asc())
        )
        result = await self._session.execute(stmt)
        return [
            PendingDigestPost(
                post_db_id=row[0],
                telegram_post_id=row[1],
                channel_username=row[2],
                content=row[3],
                published_at=row[4],
            )
            for row in result.all()
        ]

    async def mark_posts_delivered(
        self,
        user_id: int,
        subscription_id: int,
        delivered_summaries: list[DeliveredSummary],
        delivered_at: datetime,
    ) -> None:
        if not delivered_summaries:
            return

        rows = [
            {
                "user_id": user_id,
                "subscription_id": subscription_id,
                "post_id": item.post_id,
                "status": item.status,
                "skip_reason": item.skip_reason,
                "summary_text": item.summary_text,
                "summary_mode": item.summary_mode,
                "summary_model": item.summary_model,
                "prompt_snapshot": item.prompt_snapshot,
                "delivered_at": delivered_at,
            }
            for item in {summary.post_id: summary for summary in delivered_summaries}.values()
        ]
        dialect = self._session.bind.dialect.name if self._session.bind else "unknown"

        if dialect == "postgresql":
            stmt = pg_insert(DigestDelivery).values(rows).on_conflict_do_nothing(
                index_elements=["subscription_id", "post_id"],
            )
            await self._session.execute(stmt)
        elif dialect == "sqlite":
            stmt = sqlite_insert(DigestDelivery).values(rows).on_conflict_do_nothing(
                index_elements=["subscription_id", "post_id"],
            )
            await self._session.execute(stmt)
        else:
            for row in rows:
                self._session.add(DigestDelivery(**row))

        await self._session.flush()

    async def list_delivered_post_ids_for_subscription(self, subscription_id: int) -> list[int]:
        stmt = (
            select(DigestDelivery.post_id)
            .where(DigestDelivery.subscription_id == subscription_id)
            .order_by(DigestDelivery.post_id.asc())
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())
