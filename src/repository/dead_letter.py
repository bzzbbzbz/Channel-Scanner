"""Transactional content-free dead-letter persistence."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Mapping
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.dead_letter import DeadLetterRecord
from src.models.reliable_digest import DigestOutboxMessage, DigestRun
from src.repository.outbox import OutboxRepository
from src.reliability.contracts import (
    DIGEST_RUN_REQUESTED_DLQ_EVENT,
    DIGEST_RUN_REQUESTED_DLQ_TOPIC,
    TELEGRAM_DELIVERY_REQUESTED_DLQ_EVENT,
    TELEGRAM_DELIVERY_REQUESTED_DLQ_TOPIC,
)

_REASON = re.compile(r"^[a-z][a-z0-9_.:-]{0,63}$")
_ERROR_CODE = re.compile(r"^[A-Za-z][A-Za-z0-9_.:-]{0,127}$")


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return _utc(value).isoformat().replace("+00:00", "Z")


def _uuid(value: Any) -> UUID | None:
    try:
        return UUID(str(value))
    except (TypeError, ValueError, AttributeError):
        return None


def _positive_int(value: Any) -> int | None:
    try:
        result = int(value)
    except (TypeError, ValueError):
        return None
    return result if result >= 1 and not isinstance(value, bool) else None


def _attempt_summary(value: Mapping[str, Any] | None) -> dict[str, int | bool]:
    value = value or {}
    summary: dict[str, int | bool] = {
        "attempt_count": max(0, int(value.get("attempt_count", 0))),
    }
    if "max_attempts" in value:
        summary["max_attempts"] = max(1, int(value["max_attempts"]))
    if "ambiguous" in value:
        summary["ambiguous"] = bool(value["ambiguous"])
    if "processing_attempt_count" in value:
        summary["processing_attempt_count"] = max(0, int(value["processing_attempt_count"]))
    return summary


class DeadLetterRepository:
    """Create one DB record and, when identifiers exist, one DLQ outbox event."""

    def __init__(self, outbox: OutboxRepository) -> None:
        self._outbox = outbox

    async def record_terminal(
        self,
        session: AsyncSession,
        *,
        source_topic: str,
        source_event_id: UUID | None,
        source_partition: int | None,
        source_offset: int | None,
        work_type: str,
        entity_ref: str,
        run_id: UUID | None,
        message_id: UUID | None,
        subscription_id: int | None,
        correlation_id: UUID | None,
        terminal_reason: str,
        error_code: str,
        attempt_summary: Mapping[str, Any],
        generation: int,
        failed_at: datetime,
        emit_dlq: bool = True,
    ) -> tuple[DeadLetterRecord, bool]:
        """Upsert a work generation without committing the caller transaction."""
        if work_type not in {"digest_run", "digest_message", "unreadable_event"}:
            raise ValueError("Unsupported dead-letter work type")
        if not source_topic or len(source_topic) > 255 or not entity_ref or len(entity_ref) > 512:
            raise ValueError("Dead-letter source and entity reference must be bounded")
        if not _REASON.fullmatch(terminal_reason) or not _ERROR_CODE.fullmatch(error_code):
            raise ValueError("Dead-letter reason and error must be content-free machine codes")
        if generation < 1:
            raise ValueError("Dead-letter generation must be positive")
        now = _utc(failed_at)
        record_id = uuid4()
        values = {
            "id": record_id,
            "source_topic": source_topic,
            "source_event_id": source_event_id,
            "source_partition": source_partition,
            "source_offset": source_offset,
            "work_type": work_type,
            "entity_ref": entity_ref,
            "run_id": run_id,
            "message_id": message_id,
            "subscription_id": subscription_id,
            "correlation_id": correlation_id,
            "terminal_reason": terminal_reason,
            "error_code": error_code,
            "attempt_summary": _attempt_summary(attempt_summary),
            "first_failed_at": now,
            "last_failed_at": now,
            "status": "open",
            "generation": generation,
            "created_at": now,
            "updated_at": now,
        }
        dialect = session.bind.dialect.name if session.bind else "unknown"
        inserted = False
        if dialect == "postgresql":
            result = await session.execute(
                pg_insert(DeadLetterRecord)
                .values(**values)
                .on_conflict_do_nothing(index_elements=["work_type", "entity_ref", "generation"])
                .returning(DeadLetterRecord.id)
            )
            inserted = result.scalar_one_or_none() is not None
        elif dialect == "sqlite":
            result = await session.execute(
                sqlite_insert(DeadLetterRecord)
                .values(**values)
                .on_conflict_do_nothing(index_elements=["work_type", "entity_ref", "generation"])
            )
            inserted = bool(result.rowcount)
        else:
            existing = await session.scalar(
                select(DeadLetterRecord.id).where(
                    DeadLetterRecord.work_type == work_type,
                    DeadLetterRecord.entity_ref == entity_ref,
                    DeadLetterRecord.generation == generation,
                )
            )
            if existing is None:
                session.add(DeadLetterRecord(**values))
                inserted = True
        await session.flush()
        record = await session.scalar(
            select(DeadLetterRecord).where(
                DeadLetterRecord.work_type == work_type,
                DeadLetterRecord.entity_ref == entity_ref,
                DeadLetterRecord.generation == generation,
            )
        )
        if record is None:
            raise RuntimeError("Dead-letter record disappeared during upsert")
        if not inserted:
            record.last_failed_at = now
            record.error_code = error_code
            record.terminal_reason = terminal_reason
            record.attempt_summary = _attempt_summary(attempt_summary)
            record.updated_at = now
        elif emit_dlq:
            if source_event_id is None or correlation_id is None or work_type == "unreadable_event":
                raise ValueError("DLQ outbox events require contract identifiers")
            await self._enqueue_dlq(session, record=record, occurred_at=now)
        await session.flush()
        return record, inserted

    async def record_consumer_rejection(
        self,
        session: AsyncSession,
        *,
        source_topic: str,
        source_partition: int,
        source_offset: int,
        event: Mapping[str, Any] | None,
        expected_work_type: str,
        error_code: str,
        failed_at: datetime,
    ) -> DeadLetterRecord:
        """Persist a rejected envelope, falling back to an offset-only record."""
        payload = event.get("payload") if isinstance(event, Mapping) else None
        payload = payload if isinstance(payload, Mapping) else {}
        source_event_id = _uuid(event.get("event_id")) if isinstance(event, Mapping) else None
        correlation_id = _uuid(event.get("correlation_id")) if isinstance(event, Mapping) else None
        generation = _positive_int(event.get("generation")) if isinstance(event, Mapping) else None
        attempt = _positive_int(event.get("attempt")) if isinstance(event, Mapping) else None
        run_id = _uuid(payload.get("run_id"))
        message_id = _uuid(payload.get("message_id")) if expected_work_type == "digest_message" else None
        enough_ids = bool(
            source_event_id
            and correlation_id
            and generation
            and run_id
            and (expected_work_type == "digest_run" or message_id)
        )
        if not enough_ids:
            entity_ref = f"{source_topic}:{source_partition}:{source_offset}"
            record, _ = await self.record_terminal(
                session,
                source_topic=source_topic,
                source_event_id=None,
                source_partition=source_partition,
                source_offset=source_offset,
                work_type="unreadable_event",
                entity_ref=entity_ref,
                run_id=None,
                message_id=None,
                subscription_id=None,
                correlation_id=None,
                terminal_reason="unreadable_payload",
                error_code="UnreadablePayload",
                attempt_summary={"attempt_count": 0},
                generation=1,
                failed_at=failed_at,
                emit_dlq=False,
            )
            return record

        stored_run = await session.get(DigestRun, run_id)
        stored_message = await session.get(DigestOutboxMessage, message_id) if message_id is not None else None
        subscription_id = stored_run.subscription_id if stored_run is not None else None
        if expected_work_type == "digest_run" and stored_run is not None and stored_run.generation == generation:
            if stored_run.state not in {"completed", "failed"}:
                stored_run.state = "failed"
                stored_run.lease_owner = None
                stored_run.lease_until = None
                stored_run.last_error = error_code
                stored_run.updated_at = _utc(failed_at)
        if expected_work_type == "digest_message" and stored_message is not None and stored_message.generation == generation:
            if stored_message.state not in {"sent", "dead_letter"}:
                stored_message.state = "dead_letter"
                stored_message.lease_owner = None
                stored_message.lease_until = None
                stored_message.last_error = error_code
                stored_message.updated_at = _utc(failed_at)
            if stored_run is not None and stored_run.state != "completed":
                stored_run.state = "failed"
                stored_run.last_error = error_code
                stored_run.updated_at = _utc(failed_at)

        entity_id = run_id if expected_work_type == "digest_run" else message_id
        record, _ = await self.record_terminal(
            session,
            source_topic=source_topic,
            source_event_id=source_event_id,
            source_partition=source_partition,
            source_offset=source_offset,
            work_type=expected_work_type,
            entity_ref=str(entity_id),
            run_id=run_id if stored_run is not None else None,
            message_id=message_id if stored_message is not None else None,
            subscription_id=subscription_id,
            correlation_id=correlation_id,
            terminal_reason="contract_rejected",
            error_code=error_code,
            attempt_summary={"attempt_count": attempt or 0},
            generation=generation,
            failed_at=failed_at,
        )
        return record

    async def _enqueue_dlq(
        self,
        session: AsyncSession,
        *,
        record: DeadLetterRecord,
        occurred_at: datetime,
    ) -> None:
        event_id = uuid4()
        is_run = record.work_type == "digest_run"
        event_type = DIGEST_RUN_REQUESTED_DLQ_EVENT if is_run else TELEGRAM_DELIVERY_REQUESTED_DLQ_EVENT
        topic = DIGEST_RUN_REQUESTED_DLQ_TOPIC if is_run else TELEGRAM_DELIVERY_REQUESTED_DLQ_TOPIC
        entity_field = "run_id" if is_run else "message_id"
        attempt = max(1, int(record.attempt_summary.get("attempt_count", 1)))
        event = {
            "event_id": str(event_id),
            "event_type": event_type,
            "event_version": 1,
            "occurred_at": _iso(occurred_at),
            "correlation_id": str(record.correlation_id),
            "causation_id": str(record.source_event_id),
            "aggregate_type": "digest_run" if is_run else "digest_message",
            "aggregate_id": record.entity_ref,
            "attempt": attempt,
            "generation": record.generation,
            "payload": {
                "dead_letter_id": str(record.id),
                entity_field: record.entity_ref,
                "reason": record.terminal_reason,
            },
        }
        await self._outbox.enqueue(
            session,
            topic=topic,
            event_key=record.entity_ref,
            event=event,
        )
        record.dlq_outbox_event_id = event_id
