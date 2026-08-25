"""Generic leased Kafka inbox with completed-event deduplication."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from enum import Enum
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.reliable_digest import InboxEvent


class InboxClaim(str, Enum):
    CLAIMED = "claimed"
    COMPLETED = "completed"
    BUSY = "busy"


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


class InboxRepository:
    """Claim and complete events without committing caller-owned transactions."""

    async def claim(
        self,
        session: AsyncSession,
        *,
        consumer_name: str,
        event_id: UUID,
        attempt: int,
        generation: int,
        owner: str,
        now: datetime,
        lease_seconds: float,
    ) -> InboxClaim:
        if not consumer_name or len(consumer_name) > 128 or not owner or len(owner) > 128:
            raise ValueError("Inbox consumer and lease owner must contain 1..128 characters")
        now = _utc(now)
        values = {
            "consumer_name": consumer_name,
            "event_id": event_id,
            "attempt": attempt,
            "generation": generation,
            "state": "pending",
            "processing_attempt_count": 0,
            "created_at": now,
            "updated_at": now,
        }
        dialect = session.bind.dialect.name if session.bind else "unknown"
        if dialect == "postgresql":
            await session.execute(pg_insert(InboxEvent).values(**values).on_conflict_do_nothing(
                index_elements=["consumer_name", "event_id"]
            ))
        elif dialect == "sqlite":
            await session.execute(sqlite_insert(InboxEvent).values(**values).on_conflict_do_nothing(
                index_elements=["consumer_name", "event_id"]
            ))
        else:
            existing = await session.scalar(select(InboxEvent.id).where(
                InboxEvent.consumer_name == consumer_name, InboxEvent.event_id == event_id
            ))
            if existing is None:
                session.add(InboxEvent(**values))
        await session.flush()

        row = await session.scalar(
            select(InboxEvent)
            .where(InboxEvent.consumer_name == consumer_name, InboxEvent.event_id == event_id)
            .with_for_update()
        )
        if row is None:
            raise RuntimeError("Inbox row disappeared during claim")
        if row.state == "completed":
            return InboxClaim.COMPLETED
        lease_until = _utc(row.lease_until) if row.lease_until is not None else None
        if row.state == "processing" and lease_until is not None and lease_until > now and row.lease_owner != owner:
            return InboxClaim.BUSY
        row.state = "processing"
        row.lease_owner = owner
        row.lease_until = now + timedelta(seconds=lease_seconds)
        row.processing_attempt_count += 1
        row.attempt = attempt
        row.generation = generation
        row.last_error = None
        row.updated_at = now
        await session.flush()
        return InboxClaim.CLAIMED

    async def complete(
        self,
        session: AsyncSession,
        *,
        consumer_name: str,
        event_id: UUID,
        owner: str,
        completed_at: datetime,
    ) -> bool:
        now = _utc(completed_at)
        result = await session.execute(
            update(InboxEvent)
            .where(
                InboxEvent.consumer_name == consumer_name,
                InboxEvent.event_id == event_id,
                InboxEvent.state == "processing",
                InboxEvent.lease_owner == owner,
            )
            .values(
                state="completed",
                lease_owner=None,
                lease_until=None,
                completed_at=now,
                last_error=None,
                updated_at=now,
            )
        )
        return bool(result.rowcount)

    async def release(
        self,
        session: AsyncSession,
        *,
        consumer_name: str,
        event_id: UUID,
        owner: str,
        error_code: str,
        now: datetime,
    ) -> bool:
        if not error_code or len(error_code) > 128 or not error_code.replace("_", "").isalnum():
            raise ValueError("Inbox error_code must be a bounded content-free machine code")
        result = await session.execute(
            update(InboxEvent)
            .where(
                InboxEvent.consumer_name == consumer_name,
                InboxEvent.event_id == event_id,
                InboxEvent.state == "processing",
                InboxEvent.lease_owner == owner,
            )
            .values(
                state="pending",
                lease_owner=None,
                lease_until=None,
                last_error=error_code,
                updated_at=_utc(now),
            )
        )
        return bool(result.rowcount)
