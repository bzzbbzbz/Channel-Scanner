from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.dialects import postgresql

from src.models.outbox_event import OutboxEvent
from src.reliability.contracts import (
    DIGEST_RUN_REQUESTED_EVENT,
    DIGEST_RUN_REQUESTED_TOPIC,
    load_event_example,
)
from src.repository.outbox import OutboxRepository, claim_statement


def _event(*, occurred_at: datetime | None = None) -> dict:
    event = deepcopy(load_event_example(DIGEST_RUN_REQUESTED_EVENT))
    event["event_id"] = str(uuid4())
    event["correlation_id"] = str(uuid4())
    if occurred_at is not None:
        event["occurred_at"] = occurred_at.isoformat().replace("+00:00", "Z")
    return event


def test_claim_query_uses_postgresql_skip_locked() -> None:
    statement = claim_statement(now=datetime.now(timezone.utc), batch_size=10)
    sql = str(statement.compile(dialect=postgresql.dialect())).upper()

    assert "FOR UPDATE SKIP LOCKED" in sql
    assert "LIMIT" in sql


@pytest.mark.asyncio
async def test_enqueue_validates_and_never_commits(session) -> None:
    repository = OutboxRepository(max_event_bytes=65_536)
    event = _event()
    session.commit = AsyncMock()

    row = await repository.enqueue(
        session,
        topic=DIGEST_RUN_REQUESTED_TOPIC,
        event_key="42",
        event=event,
    )

    session.commit.assert_not_awaited()
    assert row.event_id.hex == event["event_id"].replace("-", "")
    assert await session.scalar(select(OutboxEvent).where(OutboxEvent.event_id == row.event_id)) is row


@pytest.mark.asyncio
async def test_claim_recovers_expired_lease_and_conditionally_marks_publication(session) -> None:
    repository = OutboxRepository(max_event_bytes=65_536)
    now = datetime(2026, 8, 23, 12, 0, tzinfo=timezone.utc)
    row = await repository.enqueue(
        session,
        topic=DIGEST_RUN_REQUESTED_TOPIC,
        event_key="42",
        event=_event(occurred_at=now),
    )
    await session.commit()

    first = await repository.claim_batch(
        session, owner="relay-a", now=now, lease_seconds=30, batch_size=10
    )
    stable_value = repository.serialize(first[0])
    await session.commit()

    assert [item.event_id for item in first] == [row.event_id]
    assert first[0].state == "publishing"
    assert first[0].publication_attempt_count == 1
    assert await repository.claim_batch(
        session, owner="relay-b", now=now + timedelta(seconds=29), lease_seconds=30, batch_size=10
    ) == []

    recovered = await repository.claim_batch(
        session, owner="relay-b", now=now + timedelta(seconds=31), lease_seconds=30, batch_size=10
    )
    await session.commit()

    assert recovered[0].publication_attempt_count == 2
    assert recovered[0].lease_owner == "relay-b"
    assert repository.serialize(recovered[0]) == stable_value
    assert await repository.mark_published(
        session,
        event_id=row.event_id,
        owner="relay-a",
        partition=0,
        offset=10,
        published_at=now + timedelta(seconds=32),
    ) is False
    assert await repository.mark_published(
        session,
        event_id=row.event_id,
        owner="relay-b",
        partition=1,
        offset=11,
        published_at=now + timedelta(seconds=32),
    ) is True
    await session.commit()
    await session.refresh(row)

    assert row.state == "published"
    assert row.lease_owner is None
    assert row.published_partition == 1
    assert row.published_offset == 11


@pytest.mark.asyncio
async def test_failure_persists_only_bounded_content_free_error_code(session) -> None:
    repository = OutboxRepository(max_event_bytes=65_536)
    now = datetime(2026, 8, 23, 12, 0, tzinfo=timezone.utc)
    row = await repository.enqueue(
        session,
        topic=DIGEST_RUN_REQUESTED_TOPIC,
        event_key="42",
        event=_event(occurred_at=now),
    )
    await repository.claim_batch(session, owner="relay-a", now=now, lease_seconds=30, batch_size=1)

    with pytest.raises(ValueError, match="content-free machine code"):
        await repository.mark_failure(
            session,
            event_id=row.event_id,
            owner="relay-a",
            error_code="Kafka failed while publishing rendered secret text",
            next_attempt_at=now + timedelta(seconds=1),
        )

    assert await repository.mark_failure(
        session,
        event_id=row.event_id,
        owner="relay-a",
        error_code="KafkaTimeoutError",
        next_attempt_at=now + timedelta(seconds=1),
    ) is True
    await session.commit()
    await session.refresh(row)

    assert row.state == "pending"
    assert row.last_error == "KafkaTimeoutError"
    assert len(row.last_error) <= 128
