"""Transactional persistence for reliable digest scheduling and rendering."""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.digest.service import PreparedDigestMessage
from src.models.chat_message import ChatMessage
from src.models.digest_processing_log import DigestProcessingLog
from src.models.reliable_digest import DigestOutboxMessage, DigestRun
from src.models.subscription import Subscription
from src.repository.digest_delivery import DeliveredSummary, DigestDeliveryRepository
from src.repository.dead_letter import DeadLetterRepository
from src.reliability.contracts import (
    DIGEST_RUN_REQUESTED_EVENT,
    DIGEST_RUN_REQUESTED_TOPIC,
    TELEGRAM_DELIVERY_REQUESTED_EVENT,
    TELEGRAM_DELIVERY_REQUESTED_TOPIC,
)
from src.repository.outbox import OutboxRepository


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return _utc(value).isoformat().replace("+00:00", "Z")


def run_claim_statement(*, run_id: UUID, now: datetime):
    return (
        select(DigestRun)
        .where(
            DigestRun.id == run_id,
            or_(
                and_(DigestRun.state.in_(("pending", "render_retry_wait")), DigestRun.next_attempt_at <= now),
                and_(DigestRun.state == "rendering", DigestRun.lease_until <= now),
            ),
        )
        .with_for_update(skip_locked=True)
    )


def message_claim_statement(*, message_id: UUID, now: datetime):
    return (
        select(DigestOutboxMessage)
        .where(
            DigestOutboxMessage.id == message_id,
            or_(
                and_(
                    DigestOutboxMessage.state.in_(("pending", "retry_wait")),
                    DigestOutboxMessage.next_attempt_at <= now,
                ),
                and_(DigestOutboxMessage.state == "sending", DigestOutboxMessage.lease_until <= now),
            ),
        )
        .with_for_update(skip_locked=True)
    )


class ReliableDigestRepository:
    def __init__(self, outbox: OutboxRepository) -> None:
        self._outbox = outbox
        self._dead_letters = DeadLetterRepository(outbox)

    async def create_scheduled_run(
        self,
        session: AsyncSession,
        *,
        subscription_id: int,
        user_id: int,
        logical_schedule_slot: datetime,
        occurred_at: datetime,
    ) -> tuple[DigestRun, bool]:
        """Create a run and root outbox event atomically in the caller transaction."""
        run_id = uuid4()
        correlation_id = uuid4()
        slot = _utc(logical_schedule_slot)
        now = _utc(occurred_at)
        values = {
            "id": run_id,
            "subscription_id": subscription_id,
            "user_id": user_id,
            "logical_schedule_slot": slot,
            "correlation_id": correlation_id,
            "generation": 1,
            "state": "pending",
            "render_attempt_count": 0,
            "next_attempt_at": now,
            "created_at": now,
            "updated_at": now,
        }
        dialect = session.bind.dialect.name if session.bind else "unknown"
        inserted = False
        if dialect == "postgresql":
            result = await session.execute(
                pg_insert(DigestRun).values(**values).on_conflict_do_nothing(
                    index_elements=["subscription_id", "logical_schedule_slot"]
                ).returning(DigestRun.id)
            )
            inserted = result.scalar_one_or_none() is not None
        elif dialect == "sqlite":
            result = await session.execute(
                sqlite_insert(DigestRun).values(**values).on_conflict_do_nothing(
                    index_elements=["subscription_id", "logical_schedule_slot"]
                )
            )
            inserted = bool(result.rowcount)
        else:
            existing = await session.scalar(select(DigestRun.id).where(
                DigestRun.subscription_id == subscription_id,
                DigestRun.logical_schedule_slot == slot,
            ))
            if existing is None:
                session.add(DigestRun(**values))
                inserted = True
        await session.flush()
        run = await session.scalar(select(DigestRun).where(
            DigestRun.subscription_id == subscription_id,
            DigestRun.logical_schedule_slot == slot,
        ))
        if run is None:
            raise RuntimeError("Digest run disappeared during scheduling")
        if inserted:
            event_id = uuid4()
            event = {
                "event_id": str(event_id),
                "event_type": DIGEST_RUN_REQUESTED_EVENT,
                "event_version": 1,
                "occurred_at": _iso(now),
                "correlation_id": str(correlation_id),
                "causation_id": None,
                "aggregate_type": "digest_run",
                "aggregate_id": str(run_id),
                "attempt": 1,
                "generation": 1,
                "payload": {
                    "run_id": str(run_id),
                    "subscription_id": subscription_id,
                    "logical_schedule_slot": _iso(slot),
                },
            }
            await self._outbox.enqueue(session, topic=DIGEST_RUN_REQUESTED_TOPIC, event_key=str(subscription_id), event=event)
        return run, inserted

    async def claim_run(
        self,
        session: AsyncSession,
        *,
        run_id: UUID,
        owner: str,
        now: datetime,
        lease_seconds: float,
    ) -> DigestRun | None:
        now = _utc(now)
        run = await session.scalar(run_claim_statement(run_id=run_id, now=now))
        if run is None:
            return None
        run.state = "rendering"
        run.lease_owner = owner
        run.lease_until = now + timedelta(seconds=lease_seconds)
        run.render_attempt_count += 1
        run.last_error = None
        run.updated_at = now
        await session.flush()
        return run

    async def save_rendered_messages(
        self,
        session: AsyncSession,
        *,
        run_id: UUID,
        owner: str,
        chat_id: int,
        messages: list[PreparedDigestMessage],
        causation_id: UUID,
        now: datetime,
    ) -> bool:
        """Persist every part and every delivery event before exposing delivering state."""
        now = _utc(now)
        run = await session.scalar(
            select(DigestRun).where(
                DigestRun.id == run_id,
                DigestRun.state == "rendering",
                DigestRun.lease_owner == owner,
            ).with_for_update()
        )
        if run is None:
            return False

        rows: list[tuple[DigestOutboxMessage, UUID]] = []
        for ordinal, message in enumerate(messages):
            message_id = uuid4()
            row = DigestOutboxMessage(
                id=message_id,
                run_id=run.id,
                ordinal=ordinal,
                chat_id=chat_id,
                text=message.text,
                parse_mode=(getattr(message.parse_mode, "value", message.parse_mode) if message.parse_mode is not None else None),
                outcomes=[asdict(outcome) for outcome in message.delivered_summaries],
                state="pending",
                generation=run.generation,
                attempt_count=0,
                next_attempt_at=now,
                ambiguous_send=False,
                created_at=now,
                updated_at=now,
            )
            session.add(row)
            rows.append((row, uuid4()))
        await session.flush()

        for row, event_id in rows:
            event = {
                "event_id": str(event_id),
                "event_type": TELEGRAM_DELIVERY_REQUESTED_EVENT,
                "event_version": 1,
                "occurred_at": _iso(now),
                "correlation_id": str(run.correlation_id),
                "causation_id": str(causation_id),
                "aggregate_type": "digest_message",
                "aggregate_id": str(row.id),
                "attempt": 1,
                "generation": run.generation,
                "payload": {"message_id": str(row.id), "run_id": str(run.id), "ordinal": row.ordinal},
            }
            await self._outbox.enqueue(
                session,
                topic=TELEGRAM_DELIVERY_REQUESTED_TOPIC,
                event_key=str(run.id),
                event=event,
            )

        run.state = "delivering"
        run.lease_owner = None
        run.lease_until = None
        run.rendered_at = now
        run.updated_at = now
        await session.flush()
        return True

    async def mark_render_failure(
        self,
        session: AsyncSession,
        *,
        run_id: UUID,
        owner: str,
        error_code: str,
        max_attempts: int,
        now: datetime,
        source_event_id: UUID,
    ) -> bool:
        run = await session.scalar(select(DigestRun).where(
            DigestRun.id == run_id, DigestRun.state == "rendering", DigestRun.lease_owner == owner
        ).with_for_update())
        if run is None:
            return False
        run.state = "failed" if run.render_attempt_count >= max_attempts else "render_retry_wait"
        run.next_attempt_at = _utc(now)
        run.lease_owner = None
        run.lease_until = None
        run.last_error = error_code[:128]
        run.updated_at = _utc(now)
        if run.state == "failed":
            await self._dead_letters.record_terminal(
                session,
                source_topic=DIGEST_RUN_REQUESTED_TOPIC,
                source_event_id=source_event_id,
                source_partition=None,
                source_offset=None,
                work_type="digest_run",
                entity_ref=str(run.id),
                run_id=run.id,
                message_id=None,
                subscription_id=run.subscription_id,
                correlation_id=run.correlation_id,
                terminal_reason="attempts_exhausted",
                error_code=error_code[:128],
                attempt_summary={
                    "attempt_count": run.render_attempt_count,
                    "max_attempts": max_attempts,
                },
                generation=run.generation,
                failed_at=now,
            )
        await session.flush()
        return True

    async def claim_message(
        self,
        session: AsyncSession,
        *,
        message_id: UUID,
        owner: str,
        now: datetime,
        lease_seconds: float,
    ) -> DigestOutboxMessage | None:
        now = _utc(now)
        message = await session.scalar(message_claim_statement(message_id=message_id, now=now))
        if message is None:
            return None
        stale_send = message.state == "sending"
        message.state = "sending"
        message.lease_owner = owner
        message.lease_until = now + timedelta(seconds=lease_seconds)
        message.attempt_count += 1
        message.ambiguous_send = message.ambiguous_send or stale_send
        message.last_error = None
        message.updated_at = now
        await session.flush()
        return message

    async def mark_message_sent(
        self,
        session: AsyncSession,
        *,
        message_id: UUID,
        owner: str,
        telegram_message_id: int,
        sent_at: datetime,
    ) -> bool:
        """Persist one acknowledged part and finalize its run when it is the last part."""
        now = _utc(sent_at)
        message = await session.scalar(
            select(DigestOutboxMessage)
            .where(
                DigestOutboxMessage.id == message_id,
                DigestOutboxMessage.state == "sending",
                DigestOutboxMessage.lease_owner == owner,
            )
            .with_for_update()
        )
        if message is None:
            return False
        run = await session.scalar(select(DigestRun).where(DigestRun.id == message.run_id).with_for_update())
        if run is None or run.state != "delivering":
            return False

        message.state = "sent"
        message.lease_owner = None
        message.lease_until = None
        message.telegram_message_id = telegram_message_id
        message.sent_at = now
        message.last_error = None
        message.updated_at = now

        outcomes = [DeliveredSummary(**outcome) for outcome in message.outcomes]
        await DigestDeliveryRepository(session).mark_posts_delivered(
            run.user_id,
            run.subscription_id,
            outcomes,
            now,
            digest_run_id=run.id,
            digest_message_id=message.id,
        )
        session.add(
            ChatMessage(
                user_id=run.user_id,
                chat_id=message.chat_id,
                role="digest",
                text=message.text,
                message_metadata={
                    "subscription_id": run.subscription_id,
                    "digest_run_id": str(run.id),
                    "digest_message_id": str(message.id),
                    "telegram_message_id": telegram_message_id,
                },
                created_at=now,
            )
        )
        await session.flush()

        unsent_count = await session.scalar(
            select(func.count())
            .select_from(DigestOutboxMessage)
            .where(DigestOutboxMessage.run_id == run.id, DigestOutboxMessage.state != "sent")
        )
        if int(unsent_count or 0) == 0:
            await self._finalize_run(session, run=run, completed_at=now)
        await session.flush()
        return True

    async def mark_message_failure(
        self,
        session: AsyncSession,
        *,
        message_id: UUID,
        owner: str,
        error_code: str,
        permanent: bool,
        ambiguous: bool,
        retry_at: datetime,
        max_attempts: int,
        causation_id: UUID,
        now: datetime,
    ) -> str | None:
        now = _utc(now)
        message = await session.scalar(
            select(DigestOutboxMessage)
            .where(
                DigestOutboxMessage.id == message_id,
                DigestOutboxMessage.state == "sending",
                DigestOutboxMessage.lease_owner == owner,
            )
            .with_for_update()
        )
        if message is None:
            return None
        terminal = permanent or message.attempt_count >= max_attempts
        message.state = "dead_letter" if terminal else "retry_wait"
        message.next_attempt_at = _utc(retry_at)
        message.lease_owner = None
        message.lease_until = None
        message.ambiguous_send = message.ambiguous_send or ambiguous
        message.last_error = error_code[:128]
        message.updated_at = now
        if terminal:
            await session.execute(
                update(DigestRun)
                .where(DigestRun.id == message.run_id, DigestRun.state == "delivering")
                .values(state="failed", last_error=error_code[:128], updated_at=now)
            )
            run = await session.get(DigestRun, message.run_id)
            if run is None:
                raise RuntimeError("Digest run disappeared before terminal delivery failure")
            await self._dead_letters.record_terminal(
                session,
                source_topic=TELEGRAM_DELIVERY_REQUESTED_TOPIC,
                source_event_id=causation_id,
                source_partition=None,
                source_offset=None,
                work_type="digest_message",
                entity_ref=str(message.id),
                run_id=run.id,
                message_id=message.id,
                subscription_id=run.subscription_id,
                correlation_id=run.correlation_id,
                terminal_reason="permanent_failure" if permanent else "attempts_exhausted",
                error_code=error_code[:128],
                attempt_summary={
                    "attempt_count": message.attempt_count,
                    "max_attempts": max_attempts,
                    "ambiguous": message.ambiguous_send,
                },
                generation=message.generation,
                failed_at=now,
            )
        else:
            run = await session.get(DigestRun, message.run_id)
            if run is None:
                raise RuntimeError("Digest run disappeared before delivery retry enqueue")
            event_id = uuid4()
            event = {
                "event_id": str(event_id),
                "event_type": TELEGRAM_DELIVERY_REQUESTED_EVENT,
                "event_version": 1,
                "occurred_at": _iso(retry_at),
                "correlation_id": str(run.correlation_id),
                "causation_id": str(causation_id),
                "aggregate_type": "digest_message",
                "aggregate_id": str(message.id),
                "attempt": message.attempt_count + 1,
                "generation": message.generation,
                "payload": {"message_id": str(message.id), "run_id": str(run.id), "ordinal": message.ordinal},
            }
            await self._outbox.enqueue(
                session,
                topic=TELEGRAM_DELIVERY_REQUESTED_TOPIC,
                event_key=str(run.id),
                event=event,
            )
        await session.flush()
        return message.state

    async def complete_empty_run(
        self,
        session: AsyncSession,
        *,
        run_id: UUID,
        owner: str,
        completed_at: datetime,
    ) -> bool:
        now = _utc(completed_at)
        run = await session.scalar(
            select(DigestRun)
            .where(DigestRun.id == run_id, DigestRun.state == "rendering", DigestRun.lease_owner == owner)
            .with_for_update()
        )
        if run is None:
            return False
        run.rendered_at = now
        await self._finalize_run(session, run=run, completed_at=now, empty=True)
        await session.flush()
        return True

    async def _finalize_run(
        self,
        session: AsyncSession,
        *,
        run: DigestRun,
        completed_at: datetime,
        empty: bool = False,
    ) -> None:
        existing_log = await session.scalar(
            select(DigestProcessingLog.id).where(DigestProcessingLog.digest_run_id == run.id)
        )
        if existing_log is None:
            outcomes_by_post: dict[int, dict] = {}
            if not empty:
                outcome_rows = await session.scalars(
                    select(DigestOutboxMessage.outcomes).where(DigestOutboxMessage.run_id == run.id)
                )
                for outcomes in outcome_rows:
                    for outcome in outcomes:
                        outcomes_by_post[int(outcome["post_id"])] = outcome
            session.add(
                DigestProcessingLog(
                    user_id=run.user_id,
                    subscription_id=run.subscription_id,
                    digest_run_id=run.id,
                    found_count=len(outcomes_by_post),
                    filtered_count=sum(outcome.get("status") == "skipped" for outcome in outcomes_by_post.values()),
                    included_count=sum(outcome.get("status") == "delivered" for outcome in outcomes_by_post.values()),
                    completed_at=completed_at,
                )
            )
        await session.execute(
            update(Subscription)
            .where(Subscription.id == run.subscription_id)
            .values(last_digest_at=completed_at, updated_at=completed_at)
        )
        run.state = "completed"
        run.lease_owner = None
        run.lease_until = None
        run.completed_at = completed_at
        run.last_error = None
        run.updated_at = completed_at
