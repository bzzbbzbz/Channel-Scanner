"""PostgreSQL transactional outbox persistence."""

from __future__ import annotations

import re
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Mapping, Any
from uuid import UUID

from sqlalchemy import and_, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.outbox_event import OutboxEvent
from src.reliability.contracts import EVENT_TOPICS, EventContractError, serialize_event

_ERROR_CODE = re.compile(r"^[A-Za-z][A-Za-z0-9_.:-]{0,127}$")


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _iso_utc(value: datetime) -> str:
    return _utc(value).isoformat().replace("+00:00", "Z")


def claim_statement(*, now: datetime, batch_size: int):
    """Build the PostgreSQL lock-skipping claim query."""
    due = and_(OutboxEvent.state == "pending", OutboxEvent.next_attempt_at <= now)
    expired = and_(OutboxEvent.state == "publishing", OutboxEvent.lease_until <= now)
    return (
        select(OutboxEvent)
        .where(or_(due, expired))
        .order_by(OutboxEvent.next_attempt_at, OutboxEvent.created_at, OutboxEvent.event_id)
        .limit(batch_size)
        .with_for_update(skip_locked=True)
    )


class OutboxRepository:
    """Mutate outbox rows inside caller-owned transactions."""

    def __init__(self, *, max_event_bytes: int) -> None:
        self._max_event_bytes = max_event_bytes

    async def enqueue(
        self,
        session: AsyncSession,
        *,
        topic: str,
        event_key: str,
        event: Mapping[str, Any],
    ) -> OutboxEvent:
        """Validate and add an event without committing the caller's transaction."""
        serialize_event(event, max_bytes=self._max_event_bytes)
        event_type = str(event["event_type"])
        if EVENT_TOPICS.get(event_type) != topic:
            raise EventContractError(f"Topic {topic!r} does not match event_type {event_type!r}")
        if not event_key or len(event_key) > 255:
            raise EventContractError("Event key must contain 1..255 characters")

        occurred_at = datetime.fromisoformat(str(event["occurred_at"]).replace("Z", "+00:00"))
        row = OutboxEvent(
            event_id=UUID(str(event["event_id"])),
            correlation_id=UUID(str(event["correlation_id"])),
            causation_id=UUID(str(event["causation_id"])) if event["causation_id"] is not None else None,
            event_type=event_type,
            event_version=int(event["event_version"]),
            occurred_at=_utc(occurred_at),
            aggregate_type=str(event["aggregate_type"]),
            aggregate_id=str(event["aggregate_id"]),
            attempt=int(event["attempt"]),
            generation=int(event["generation"]),
            topic=topic,
            event_key=event_key,
            payload=deepcopy(dict(event["payload"])),
            state="pending",
            publication_attempt_count=0,
            next_attempt_at=_utc(occurred_at),
        )
        session.add(row)
        await session.flush()
        return row

    async def claim_batch(
        self,
        session: AsyncSession,
        *,
        owner: str,
        now: datetime,
        lease_seconds: float,
        batch_size: int,
    ) -> list[OutboxEvent]:
        """Claim due rows, including publishing rows whose leases expired."""
        if not owner or len(owner) > 128:
            raise ValueError("Outbox lease owner must contain 1..128 characters")
        now = _utc(now)
        statement = claim_statement(now=now, batch_size=batch_size)
        rows = list((await session.scalars(statement)).all())
        lease_until = now + timedelta(seconds=lease_seconds)
        for row in rows:
            row.state = "publishing"
            row.lease_owner = owner
            row.lease_until = lease_until
            row.publication_attempt_count += 1
            row.updated_at = now
        await session.flush()
        return rows

    async def mark_published(
        self,
        session: AsyncSession,
        *,
        event_id: UUID,
        owner: str,
        partition: int,
        offset: int,
        published_at: datetime,
    ) -> bool:
        """Complete only the lease still owned by this relay."""
        now = _utc(published_at)
        result = await session.execute(
            update(OutboxEvent)
            .where(
                OutboxEvent.event_id == event_id,
                OutboxEvent.state == "publishing",
                OutboxEvent.lease_owner == owner,
            )
            .values(
                state="published",
                lease_owner=None,
                lease_until=None,
                published_partition=partition,
                published_offset=offset,
                published_at=now,
                last_error=None,
                updated_at=now,
            )
        )
        return bool(result.rowcount)

    async def mark_failure(
        self,
        session: AsyncSession,
        *,
        event_id: UUID,
        owner: str,
        error_code: str,
        next_attempt_at: datetime,
    ) -> bool:
        """Release an owned lease with a bounded content-free machine code."""
        if not _ERROR_CODE.fullmatch(error_code):
            raise ValueError("Outbox error_code must be a content-free machine code of at most 128 characters")
        retry_at = _utc(next_attempt_at)
        result = await session.execute(
            update(OutboxEvent)
            .where(
                OutboxEvent.event_id == event_id,
                OutboxEvent.state == "publishing",
                OutboxEvent.lease_owner == owner,
            )
            .values(
                state="pending",
                lease_owner=None,
                lease_until=None,
                next_attempt_at=retry_at,
                last_error=error_code,
                updated_at=datetime.now(timezone.utc),
            )
        )
        return bool(result.rowcount)

    def serialize(self, event: OutboxEvent) -> bytes:
        """Rebuild the immutable contract envelope from normalized columns."""
        envelope = {
            "event_id": str(event.event_id),
            "event_type": event.event_type,
            "event_version": event.event_version,
            "occurred_at": _iso_utc(event.occurred_at),
            "correlation_id": str(event.correlation_id),
            "causation_id": str(event.causation_id) if event.causation_id is not None else None,
            "aggregate_type": event.aggregate_type,
            "aggregate_id": event.aggregate_id,
            "attempt": event.attempt,
            "generation": event.generation,
            "payload": event.payload,
        }
        return serialize_event(envelope, max_bytes=self._max_event_bytes)
