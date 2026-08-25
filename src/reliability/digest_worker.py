"""Durable digest rendering worker without Telegram side effects."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Callable
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.config.settings import LlmSettings, ReliableDeliverySettings
from src.digest.service import build_digest_messages
from src.llm import OpenRouterModelPool
from src.models.reliable_digest import DigestRun
from src.repository.digest_delivery import DigestDeliveryRepository
from src.repository.dead_letter import DeadLetterRepository
from src.repository.inbox import InboxClaim, InboxRepository
from src.repository.outbox import OutboxRepository
from src.repository.reliable_digest import ReliableDigestRepository
from src.repository.subscription import SubscriptionRepository
from src.reliability.kafka_consumer import ConsumerOutcome, DIGEST_CONSUMER_GROUP, RejectedKafkaEvent

logger = logging.getLogger(__name__)


class DigestWorker:
    """Render a root event once and atomically expose all persisted message parts."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        outbox: OutboxRepository,
        reliable_settings: ReliableDeliverySettings,
        llm_settings: LlmSettings | None = None,
        model_pool: OpenRouterModelPool | None = None,
        *,
        owner: str,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        self._session_factory = session_factory
        self._inbox = InboxRepository()
        self._digests = ReliableDigestRepository(outbox)
        self._dead_letters = DeadLetterRepository(outbox)
        self._settings = reliable_settings
        self._llm_settings = llm_settings or LlmSettings()
        self._model_pool = model_pool
        self._owner = owner
        self._clock = clock

    async def handle(self, event: dict[str, Any]) -> ConsumerOutcome:
        event_id = UUID(event["event_id"])
        run_id = UUID(event["payload"]["run_id"])
        subscription_id = int(event["payload"]["subscription_id"])
        now = self._clock()

        async with self._session_factory() as session, session.begin():
            claim = await self._inbox.claim(
                session,
                consumer_name=DIGEST_CONSUMER_GROUP,
                event_id=event_id,
                attempt=int(event["attempt"]),
                generation=int(event["generation"]),
                owner=self._owner,
                now=now,
                lease_seconds=self._settings.inbox_lease_seconds,
            )
            if claim == InboxClaim.COMPLETED:
                return ConsumerOutcome.COMMIT
            if claim == InboxClaim.BUSY:
                return ConsumerOutcome.RETRY
            run = await session.get(DigestRun, run_id)
            if run is None or not _event_matches_run(event, run, subscription_id):
                await self._inbox.complete(
                    session,
                    consumer_name=DIGEST_CONSUMER_GROUP,
                    event_id=event_id,
                    owner=self._owner,
                    completed_at=now,
                )
                return ConsumerOutcome.COMMIT
            claimed_run = await self._digests.claim_run(
                session,
                run_id=run_id,
                owner=self._owner,
                now=now,
                lease_seconds=self._settings.render_lease_seconds,
            )
            if claimed_run is None:
                if run.state in {"delivering", "completed", "failed"}:
                    await self._inbox.complete(
                        session,
                        consumer_name=DIGEST_CONSUMER_GROUP,
                        event_id=event_id,
                        owner=self._owner,
                        completed_at=now,
                    )
                    return ConsumerOutcome.COMMIT
                return ConsumerOutcome.RETRY

        try:
            async with self._session_factory() as session:
                subscription = await SubscriptionRepository(session).get_by_id(subscription_id)
                if subscription is None or subscription.user is None or not self._settings.owns_subscription(subscription.id):
                    raise LookupError("subscription_not_owned")
                items = await DigestDeliveryRepository(session).get_pending_posts_for_subscription(subscription.id)
                user = subscription.user
            messages = []
            if items:
                messages = await build_digest_messages(
                    subscription,
                    user,
                    items,
                    self._llm_settings,
                    self._model_pool,
                    memory_service=None,
                )
        except Exception as exc:
            return await self._record_failure(event_id, run_id, type(exc).__name__, now)

        async with self._session_factory() as session, session.begin():
            if not messages:
                completed = await self._digests.complete_empty_run(
                    session,
                    run_id=run_id,
                    owner=self._owner,
                    completed_at=self._clock(),
                )
                if not completed:
                    return ConsumerOutcome.RETRY
                inbox_completed = await self._inbox.complete(
                    session,
                    consumer_name=DIGEST_CONSUMER_GROUP,
                    event_id=event_id,
                    owner=self._owner,
                    completed_at=self._clock(),
                )
                if not inbox_completed:
                    raise RuntimeError("Inbox lease lost before empty run was committed")
                logger.info(
                    "Digest run transition: event_id=%s correlation_id=%s run_id=%s attempt=%d state=completed messages=0",
                    event_id,
                    event["correlation_id"],
                    run_id,
                    int(event["attempt"]),
                )
                return ConsumerOutcome.COMMIT
            saved = await self._digests.save_rendered_messages(
                session,
                run_id=run_id,
                owner=self._owner,
                chat_id=user.chat_id,
                messages=messages,
                causation_id=event_id,
                now=self._clock(),
            )
            if not saved:
                return ConsumerOutcome.RETRY
            completed = await self._inbox.complete(
                session,
                consumer_name=DIGEST_CONSUMER_GROUP,
                event_id=event_id,
                owner=self._owner,
                completed_at=self._clock(),
            )
            if not completed:
                raise RuntimeError("Inbox lease lost before rendered messages were committed")
        logger.info(
            "Digest run transition: event_id=%s correlation_id=%s run_id=%s attempt=%d state=delivering messages=%d",
            event_id,
            event["correlation_id"],
            run_id,
            int(event["attempt"]),
            len(messages),
        )
        return ConsumerOutcome.COMMIT

    async def handle_rejected(self, rejected: RejectedKafkaEvent) -> ConsumerOutcome:
        """Make malformed input terminal without retaining its payload."""
        async with self._session_factory() as session, session.begin():
            await self._dead_letters.record_consumer_rejection(
                session,
                source_topic=rejected.topic,
                source_partition=rejected.partition,
                source_offset=rejected.offset,
                event=rejected.event,
                expected_work_type="digest_run",
                error_code=rejected.error_code,
                failed_at=self._clock(),
            )
        return ConsumerOutcome.COMMIT

    async def _record_failure(self, event_id: UUID, run_id: UUID, error_code: str, now: datetime) -> ConsumerOutcome:
        async with self._session_factory() as session, session.begin():
            await self._digests.mark_render_failure(
                session,
                run_id=run_id,
                owner=self._owner,
                error_code=error_code,
                max_attempts=self._settings.render_max_attempts,
                now=now,
                source_event_id=event_id,
            )
            state = await session.scalar(select(DigestRun.state).where(DigestRun.id == run_id))
            if state == "failed":
                await self._inbox.complete(
                    session,
                    consumer_name=DIGEST_CONSUMER_GROUP,
                    event_id=event_id,
                    owner=self._owner,
                    completed_at=now,
                )
                return ConsumerOutcome.COMMIT
            await self._inbox.release(
                session,
                consumer_name=DIGEST_CONSUMER_GROUP,
                event_id=event_id,
                owner=self._owner,
                error_code=error_code[:128] if error_code else "RenderError",
                now=now,
            )
        return ConsumerOutcome.RETRY


def _event_matches_run(event: dict[str, Any], run: DigestRun, subscription_id: int) -> bool:
    try:
        slot = datetime.fromisoformat(event["payload"]["logical_schedule_slot"].replace("Z", "+00:00"))
        stored_slot = run.logical_schedule_slot
        if stored_slot.tzinfo is None:
            stored_slot = stored_slot.replace(tzinfo=timezone.utc)
        return (
            event["aggregate_id"] == str(run.id)
            and UUID(event["correlation_id"]) == run.correlation_id
            and run.subscription_id == subscription_id
            and int(event["generation"]) == run.generation
            and slot.astimezone(timezone.utc) == stored_slot.astimezone(timezone.utc)
        )
    except (KeyError, TypeError, ValueError):
        return False
