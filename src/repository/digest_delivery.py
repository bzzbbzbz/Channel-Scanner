"""Repository for digest delivery selection and dedup state."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from sqlalchemy import and_, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.channel import Channel
from src.models.digest_delivery import DigestDelivery
from src.models.digest_processing_log import DigestProcessingLog
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


@dataclass(slots=True)
class DigestProcessingStats:
    """Aggregate outcomes for completed processing runs in a period."""

    run_count: int
    found_count: int
    filtered_count: int
    included_count: int


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

    async def get_posts_for_subscription_period(
        self,
        subscription_id: int,
        period_start: datetime,
        period_end: datetime,
    ) -> list[PendingDigestPost]:
        """Return stored subscription posts for a replay without delivery-state filtering."""
        stmt = (
            select(Post.id, Post.post_id, Channel.username, Post.content, Post.datetime)
            .join(Channel, Channel.id == Post.channel_id)
            .join(SubscriptionChannel, SubscriptionChannel.channel_id == Channel.id)
            .where(
                SubscriptionChannel.subscription_id == subscription_id,
                Post.datetime >= SubscriptionChannel.subscribed_at,
                Post.datetime >= period_start,
                Post.datetime < period_end,
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
        *,
        digest_run_id: UUID | None = None,
        digest_message_id: UUID | None = None,
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
                "digest_run_id": digest_run_id,
                "digest_message_id": digest_message_id,
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

    async def record_processing_log(
        self,
        user_id: int,
        subscription_id: int,
        *,
        found_count: int,
        filtered_count: int,
        included_count: int,
        completed_at: datetime,
        digest_run_id: UUID | None = None,
    ) -> None:
        self._session.add(
            DigestProcessingLog(
                user_id=user_id,
                subscription_id=subscription_id,
                found_count=found_count,
                filtered_count=filtered_count,
                included_count=included_count,
                digest_run_id=digest_run_id,
                completed_at=completed_at,
            )
        )
        await self._session.flush()

    async def get_processing_stats_for_period(
        self,
        user_id: int,
        subscription_id: int,
        period_start: datetime,
        period_end: datetime,
    ) -> DigestProcessingStats:
        stmt = select(
            func.count(DigestProcessingLog.id),
            func.coalesce(func.sum(DigestProcessingLog.found_count), 0),
            func.coalesce(func.sum(DigestProcessingLog.filtered_count), 0),
            func.coalesce(func.sum(DigestProcessingLog.included_count), 0),
        ).where(
            DigestProcessingLog.user_id == user_id,
            DigestProcessingLog.subscription_id == subscription_id,
            DigestProcessingLog.completed_at >= period_start,
            DigestProcessingLog.completed_at < period_end,
        )
        row = (await self._session.execute(stmt)).one()
        return DigestProcessingStats(
            run_count=int(row[0]),
            found_count=int(row[1]),
            filtered_count=int(row[2]),
            included_count=int(row[3]),
        )

    async def list_delivered_post_ids_for_subscription(self, subscription_id: int) -> list[int]:
        stmt = (
            select(DigestDelivery.post_id)
            .where(DigestDelivery.subscription_id == subscription_id)
            .order_by(DigestDelivery.post_id.asc())
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())
