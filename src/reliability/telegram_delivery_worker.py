"""Durable at-least-once delivery of persisted digest parts to Telegram."""

from __future__ import annotations

import asyncio
import logging
import random
from datetime import datetime, timedelta, timezone
from typing import Any, Callable
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.config.settings import ReliableDeliverySettings
from src.models.reliable_digest import DigestOutboxMessage, DigestRun
from src.repository.inbox import InboxClaim, InboxRepository
from src.repository.dead_letter import DeadLetterRepository
from src.repository.outbox import OutboxRepository
from src.repository.reliable_digest import ReliableDigestRepository
from src.reliability.kafka_consumer import ConsumerOutcome, DELIVERY_CONSUMER_GROUP, RejectedKafkaEvent
from src.reliability.telegram_sender import (
    ClassifiedDeliveryError,
    DeliveryErrorKind,
    ReliableTelegramSender,
    classify_delivery_error,
)

logger = logging.getLogger(__name__)


class TelegramDeliveryWorker:
    """Claim one persisted message, send outside a transaction, then persist its outcome."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        sender: ReliableTelegramSender,
        reliable_settings: ReliableDeliverySettings,
        *,
        owner: str,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        random_value: Callable[[], float] = random.random,
        outbox: OutboxRepository | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._sender = sender
        self._inbox = InboxRepository()
        outbox_repository = outbox or OutboxRepository(max_event_bytes=65_536)
        self._digests = ReliableDigestRepository(outbox_repository)
        self._dead_letters = DeadLetterRepository(outbox_repository)
        self._settings = reliable_settings
        self._owner = owner
        self._clock = clock
        self._random_value = random_value

    async def handle(self, event: dict[str, Any]) -> ConsumerOutcome:
        event_id = UUID(event["event_id"])
        message_id = UUID(event["payload"]["message_id"])
        run_id = UUID(event["payload"]["run_id"])
        now = self._clock()

        async with self._session_factory() as session, session.begin():
            claim = await self._inbox.claim(
                session,
                consumer_name=DELIVERY_CONSUMER_GROUP,
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

            message = await session.get(DigestOutboxMessage, message_id)
            run = await session.get(DigestRun, run_id)
            if message is None or run is None or not _event_matches_message(event, message, run):
                if not await self._complete_inbox(session, event_id, now):
                    raise RuntimeError("Inbox lease lost before rejected event was committed")
                return ConsumerOutcome.COMMIT

            claimed_message = await self._digests.claim_message(
                session,
                message_id=message_id,
                owner=self._owner,
                now=now,
                lease_seconds=self._settings.delivery_lease_seconds,
            )
            if claimed_message is None:
                await session.refresh(message)
                if message.state in {"sent", "dead_letter"}:
                    if not await self._complete_inbox(session, event_id, now):
                        raise RuntimeError("Inbox lease lost before terminal message dedup was committed")
                    return ConsumerOutcome.COMMIT
                await self._release_inbox(session, event_id, "MessageNotDue", now)
                return ConsumerOutcome.RETRY

        if run.state == "failed":
            return await self._persist_failure(
                event_id,
                message_id,
                run_id,
                UUID(event["correlation_id"]),
                claimed_message.attempt_count,
                ClassifiedDeliveryError(DeliveryErrorKind.PERMANENT, "DigestRunFailed"),
            )

        try:
            _validate_persisted_message(claimed_message)
            async with asyncio.timeout(self._settings.delivery_send_timeout_seconds):
                telegram_message_id = await self._sender.send_message(
                    claimed_message.chat_id,
                    claimed_message.text,
                    parse_mode=claimed_message.parse_mode,
                )
            if not isinstance(telegram_message_id, int) or isinstance(telegram_message_id, bool) or telegram_message_id <= 0:
                raise ValueError("invalid_telegram_message_id")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            return await self._persist_failure(
                event_id,
                message_id,
                run_id,
                UUID(event["correlation_id"]),
                claimed_message.attempt_count,
                classify_delivery_error(exc),
            )

        completed_at = self._clock()
        async with self._session_factory() as session, session.begin():
            saved = await self._digests.mark_message_sent(
                session,
                message_id=message_id,
                owner=self._owner,
                telegram_message_id=telegram_message_id,
                sent_at=completed_at,
            )
            if not saved:
                await self._release_inbox(session, event_id, "MessageLeaseLost", completed_at)
                return ConsumerOutcome.RETRY
            if not await self._complete_inbox(session, event_id, completed_at):
                raise RuntimeError("Inbox lease lost before Telegram outcome was committed")
        logger.info(
            "Digest part transition: event_id=%s correlation_id=%s run_id=%s message_id=%s attempt=%d state=sent",
            event_id,
            event["correlation_id"],
            run_id,
            message_id,
            claimed_message.attempt_count,
        )
        return ConsumerOutcome.COMMIT

    async def handle_rejected(self, rejected: RejectedKafkaEvent) -> ConsumerOutcome:
        """Persist a permanent contract rejection before committing its offset."""
        async with self._session_factory() as session, session.begin():
            await self._dead_letters.record_consumer_rejection(
                session,
                source_topic=rejected.topic,
                source_partition=rejected.partition,
                source_offset=rejected.offset,
                event=rejected.event,
                expected_work_type="digest_message",
                error_code=rejected.error_code,
                failed_at=self._clock(),
            )
        return ConsumerOutcome.COMMIT

    async def _persist_failure(
        self,
        event_id: UUID,
        message_id: UUID,
        run_id: UUID,
        correlation_id: UUID,
        attempt_count: int,
        error: ClassifiedDeliveryError,
    ) -> ConsumerOutcome:
        now = self._clock()
        ceiling = min(
            self._settings.delivery_backoff_cap_seconds,
            self._settings.delivery_backoff_base_seconds * (2 ** max(0, attempt_count - 1)),
        )
        delay = self._random_value() * ceiling
        if error.retry_after_seconds is not None:
            delay = max(delay, error.retry_after_seconds)
        retry_at = now + timedelta(seconds=delay)
        async with self._session_factory() as session, session.begin():
            state = await self._digests.mark_message_failure(
                session,
                message_id=message_id,
                owner=self._owner,
                error_code=error.code,
                permanent=error.kind == DeliveryErrorKind.PERMANENT,
                ambiguous=error.kind == DeliveryErrorKind.AMBIGUOUS,
                retry_at=retry_at,
                max_attempts=self._settings.delivery_max_attempts,
                causation_id=event_id,
                now=now,
            )
            if state is None:
                await self._release_inbox(session, event_id, "MessageLeaseLost", now)
                return ConsumerOutcome.RETRY
            if not await self._complete_inbox(session, event_id, now):
                raise RuntimeError("Inbox lease lost before Telegram failure outcome was committed")
        logger.warning(
            "Digest part transition: event_id=%s correlation_id=%s run_id=%s message_id=%s attempt=%d state=%s error=%s ambiguous=%s",
            event_id,
            correlation_id,
            run_id,
            message_id,
            attempt_count,
            state,
            error.code,
            error.kind == DeliveryErrorKind.AMBIGUOUS,
        )
        return ConsumerOutcome.COMMIT

    async def _complete_inbox(self, session: AsyncSession, event_id: UUID, now: datetime) -> bool:
        return await self._inbox.complete(
            session,
            consumer_name=DELIVERY_CONSUMER_GROUP,
            event_id=event_id,
            owner=self._owner,
            completed_at=now,
        )

    async def _release_inbox(self, session: AsyncSession, event_id: UUID, code: str, now: datetime) -> bool:
        return await self._inbox.release(
            session,
            consumer_name=DELIVERY_CONSUMER_GROUP,
            event_id=event_id,
            owner=self._owner,
            error_code=code,
            now=now,
        )


def _event_matches_message(event: dict[str, Any], message: DigestOutboxMessage, run: DigestRun) -> bool:
    try:
        event_attempt = int(event["attempt"])
        expected_attempt = message.attempt_count if message.state in {"sending", "sent", "dead_letter"} else message.attempt_count + 1
        return (
            event["aggregate_id"] == str(message.id)
            and message.run_id == run.id
            and UUID(event["correlation_id"]) == run.correlation_id
            and UUID(event["payload"]["run_id"]) == run.id
            and int(event["payload"]["ordinal"]) == message.ordinal
            and int(event["generation"]) == message.generation
            and event_attempt == expected_attempt
        )
    except (KeyError, TypeError, ValueError):
        return False


def _validate_persisted_message(message: DigestOutboxMessage) -> None:
    if message.parse_mode not in {None, "HTML"} or not 1 <= len(message.text) <= 4096:
        raise ValueError("invalid_persisted_message")
    required = {"post_id", "summary_text", "summary_mode", "summary_model", "prompt_snapshot", "status", "skip_reason"}
    if any(not isinstance(outcome, dict) or set(outcome) != required for outcome in message.outcomes):
        raise ValueError("invalid_persisted_outcome")
    if any(
        not isinstance(outcome["post_id"], int)
        or isinstance(outcome["post_id"], bool)
        or outcome["post_id"] <= 0
        or outcome["status"] not in {"delivered", "skipped"}
        for outcome in message.outcomes
    ):
        raise ValueError("invalid_persisted_outcome")
