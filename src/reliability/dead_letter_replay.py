"""Narrow administrator command for replaying one BL-22 dead letter."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.models.dead_letter import DeadLetterRecord, DeadLetterReplay
from src.models.reliable_digest import DigestOutboxMessage, DigestRun
from src.models.subscription import Subscription
from src.repository.outbox import OutboxRepository
from src.reliability.contracts import (
    DIGEST_RUN_REQUESTED_EVENT,
    DIGEST_RUN_REQUESTED_TOPIC,
    TELEGRAM_DELIVERY_REQUESTED_EVENT,
    TELEGRAM_DELIVERY_REQUESTED_TOPIC,
)

_IDEMPOTENCY_KEY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return _utc(value).isoformat().replace("+00:00", "Z")


def dead_letter_lock_statement(record_id: UUID):
    return select(DeadLetterRecord).where(DeadLetterRecord.id == record_id).with_for_update()


@dataclass(frozen=True)
class ReplayResult:
    replay_id: UUID
    dead_letter_id: UUID
    result: str
    generation: int
    outbox_event_id: UUID | None
    error_code: str | None


class DeadLetterReplayService:
    """Execute only the fixed message/run recovery command, never arbitrary events."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        outbox: OutboxRepository,
        *,
        clock=lambda: datetime.now(timezone.utc),
    ) -> None:
        self._session_factory = session_factory
        self._outbox = outbox
        self._clock = clock

    async def replay(self, record_id: UUID, *, idempotency_key: str, actor: str) -> ReplayResult:
        if not _IDEMPOTENCY_KEY.fullmatch(idempotency_key):
            raise ValueError("Idempotency-Key must be a bounded opaque token")
        if not actor or len(actor) > 128:
            raise ValueError("Replay actor must contain 1..128 characters")
        now = _utc(self._clock())
        async with self._session_factory() as session, session.begin():
            existing = await self._existing(session, record_id, idempotency_key)
            if existing is not None:
                return _result(existing)
            record = await session.scalar(dead_letter_lock_statement(record_id))
            if record is None:
                raise KeyError(record_id)
            # A waiter can observe a replay committed while it was blocked on the record lock.
            existing = await self._existing(session, record_id, idempotency_key)
            if existing is not None:
                return _result(existing)
            if record.status != "open":
                replay = self._reject(session, record, idempotency_key, actor, now, "DeadLetterNotOpen")
                return _result(replay)
            if record.work_type == "digest_run":
                replay = await self._replay_run(session, record, idempotency_key, actor, now)
            elif record.work_type == "digest_message":
                replay = await self._replay_message(session, record, idempotency_key, actor, now)
            else:
                replay = self._reject(session, record, idempotency_key, actor, now, "UnsupportedReplayCategory")
            await session.flush()
            return _result(replay)

    async def _replay_run(
        self,
        session: AsyncSession,
        record: DeadLetterRecord,
        key: str,
        actor: str,
        now: datetime,
    ) -> DeadLetterReplay:
        run = await session.scalar(select(DigestRun).where(DigestRun.id == record.run_id).with_for_update()) if record.run_id else None
        subscription = await session.get(Subscription, run.subscription_id) if run is not None else None
        existing_messages = []
        if run is not None:
            existing_messages = list((await session.scalars(select(DigestOutboxMessage.id).where(DigestOutboxMessage.run_id == run.id))).all())
        if run is None or subscription is None:
            return self._reject(session, record, key, actor, now, "ReplayEntityMissing")
        if run.state != "failed" or existing_messages:
            return self._reject(session, record, key, actor, now, "ReplayStateInvalid")

        generation = record.generation + 1
        event_id = uuid4()
        run.generation = generation
        run.state = "pending"
        run.lease_owner = None
        run.lease_until = None
        run.render_attempt_count = 0
        run.next_attempt_at = now
        run.last_error = None
        run.rendered_at = None
        run.completed_at = None
        run.updated_at = now
        event = {
            "event_id": str(event_id),
            "event_type": DIGEST_RUN_REQUESTED_EVENT,
            "event_version": 1,
            "occurred_at": _iso(now),
            "correlation_id": str(run.correlation_id),
            "causation_id": str(record.dlq_outbox_event_id) if record.dlq_outbox_event_id else str(record.source_event_id),
            "aggregate_type": "digest_run",
            "aggregate_id": str(run.id),
            "attempt": 1,
            "generation": generation,
            "payload": {
                "run_id": str(run.id),
                "subscription_id": run.subscription_id,
                "logical_schedule_slot": _iso(run.logical_schedule_slot),
            },
        }
        await self._outbox.enqueue(session, topic=DIGEST_RUN_REQUESTED_TOPIC, event_key=str(run.subscription_id), event=event)
        return self._accept(session, record, key, actor, now, generation, event_id)

    async def _replay_message(
        self,
        session: AsyncSession,
        record: DeadLetterRecord,
        key: str,
        actor: str,
        now: datetime,
    ) -> DeadLetterReplay:
        message = (
            await session.scalar(
                select(DigestOutboxMessage).where(DigestOutboxMessage.id == record.message_id).with_for_update()
            )
            if record.message_id
            else None
        )
        run = await session.scalar(select(DigestRun).where(DigestRun.id == message.run_id).with_for_update()) if message else None
        subscription = await session.get(Subscription, run.subscription_id) if run is not None else None
        if message is None or run is None or subscription is None:
            return self._reject(session, record, key, actor, now, "ReplayEntityMissing")
        if message.state != "dead_letter" or run.state not in {"failed", "delivering"}:
            return self._reject(session, record, key, actor, now, "ReplayStateInvalid")

        generation = record.generation + 1
        event_id = uuid4()
        message.generation = generation
        message.state = "pending"
        message.attempt_count = 0
        message.next_attempt_at = now
        message.lease_owner = None
        message.lease_until = None
        message.telegram_message_id = None
        message.sent_at = None
        message.ambiguous_send = False
        message.last_error = None
        message.updated_at = now
        run.generation = max(run.generation, generation)
        run.state = "delivering"
        run.lease_owner = None
        run.lease_until = None
        run.completed_at = None
        run.last_error = None
        run.updated_at = now
        event = {
            "event_id": str(event_id),
            "event_type": TELEGRAM_DELIVERY_REQUESTED_EVENT,
            "event_version": 1,
            "occurred_at": _iso(now),
            "correlation_id": str(run.correlation_id),
            "causation_id": str(record.dlq_outbox_event_id) if record.dlq_outbox_event_id else str(record.source_event_id),
            "aggregate_type": "digest_message",
            "aggregate_id": str(message.id),
            "attempt": 1,
            "generation": generation,
            "payload": {"message_id": str(message.id), "run_id": str(run.id), "ordinal": message.ordinal},
        }
        await self._outbox.enqueue(session, topic=TELEGRAM_DELIVERY_REQUESTED_TOPIC, event_key=str(run.id), event=event)
        return self._accept(session, record, key, actor, now, generation, event_id)

    def _accept(
        self,
        session: AsyncSession,
        record: DeadLetterRecord,
        key: str,
        actor: str,
        now: datetime,
        generation: int,
        outbox_event_id: UUID,
    ) -> DeadLetterReplay:
        replay = DeadLetterReplay(
            id=uuid4(),
            dead_letter_id=record.id,
            idempotency_key=key,
            actor=actor,
            requested_at=now,
            result="replayed",
            generation=generation,
            outbox_event_id=outbox_event_id,
        )
        session.add(replay)
        record.status = "replayed"
        record.updated_at = now
        return replay

    def _reject(
        self,
        session: AsyncSession,
        record: DeadLetterRecord,
        key: str,
        actor: str,
        now: datetime,
        error_code: str,
    ) -> DeadLetterReplay:
        replay = DeadLetterReplay(
            id=uuid4(),
            dead_letter_id=record.id,
            idempotency_key=key,
            actor=actor,
            requested_at=now,
            result="replay_rejected",
            generation=record.generation,
            error_code=error_code,
        )
        session.add(replay)
        if record.status == "open":
            record.status = "replay_rejected"
            record.updated_at = now
        return replay

    async def _existing(
        self,
        session: AsyncSession,
        record_id: UUID,
        key: str,
    ) -> DeadLetterReplay | None:
        return await session.scalar(
            select(DeadLetterReplay).where(
                DeadLetterReplay.dead_letter_id == record_id,
                DeadLetterReplay.idempotency_key == key,
            )
        )


def _result(replay: DeadLetterReplay) -> ReplayResult:
    return ReplayResult(
        replay_id=replay.id,
        dead_letter_id=replay.dead_letter_id,
        result=replay.result,
        generation=replay.generation,
        outbox_event_id=replay.outbox_event_id,
        error_code=replay.error_code,
    )
