"""Reliable digest scheduler producer for BL-22 stage 3."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.assistant.cron import latest_due_slot
from src.config.settings import ReliableDeliverySettings
from src.repository.outbox import OutboxRepository
from src.repository.reliable_digest import ReliableDigestRepository
from src.repository.subscription import SubscriptionRepository

logger = logging.getLogger(__name__)


class ReliableDigestScheduler:
    """Create at most one durable run for each subscription's latest due slot."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        outbox: OutboxRepository,
        settings: ReliableDeliverySettings,
    ) -> None:
        self._session_factory = session_factory
        self._repository = ReliableDigestRepository(outbox)
        self._settings = settings

    async def run_once(self, now: datetime | None = None) -> int:
        now = now or datetime.now(timezone.utc)
        async with self._session_factory() as session:
            subscriptions = await SubscriptionRepository(session).list_enabled()
        created = 0
        for subscription in subscriptions:
            if not self._settings.owns_subscription(subscription.id) or subscription.user is None:
                continue
            slot = latest_due_slot(subscription, subscription.user, now)
            if slot is None:
                continue
            async with self._session_factory() as session, session.begin():
                run, inserted = await self._repository.create_scheduled_run(
                    session,
                    subscription_id=subscription.id,
                    user_id=subscription.user.id,
                    logical_schedule_slot=slot,
                    occurred_at=now,
                )
            created += int(inserted)
            if inserted:
                logger.info(
                    "Reliable digest run transition: correlation_id=%s run_id=%s attempt=0 state=pending",
                    run.correlation_id,
                    run.id,
                )
        return created

    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            created = await self.run_once()
            if created:
                logger.info("Reliable digest scheduler created runs: count=%d", created)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self._settings.poll_interval_seconds)
            except TimeoutError:
                pass
