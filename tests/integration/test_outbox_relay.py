from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from src.config.settings import KafkaSettings
from src.models.outbox_event import OutboxEvent
from src.reliability.contracts import DIGEST_RUN_REQUESTED_EVENT, DIGEST_RUN_REQUESTED_TOPIC, load_event_example
from src.reliability.kafka_producer import PublicationResult
from src.reliability.outbox_relay import OutboxRelay
from src.repository.outbox import OutboxRepository


def _event(now: datetime) -> dict:
    event = deepcopy(load_event_example(DIGEST_RUN_REQUESTED_EVENT))
    event["event_id"] = str(uuid4())
    event["correlation_id"] = str(uuid4())
    event["occurred_at"] = now.isoformat().replace("+00:00", "Z")
    return event


class _Producer:
    def __init__(self, failure: Exception | None = None) -> None:
        self.failure = failure
        self.calls = []

    async def publish(self, **kwargs):
        self.calls.append(kwargs)
        if self.failure is not None:
            raise self.failure
        return PublicationResult(partition=0, offset=len(self.calls) - 1, published_at=datetime.now(timezone.utc))


class _LoseFirstAcknowledgementRepository(OutboxRepository):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.lose_first_acknowledgement = True

    async def mark_published(self, session, **kwargs):
        if self.lose_first_acknowledgement:
            self.lose_first_acknowledgement = False
            raise OSError("database acknowledgement unavailable")
        return await super().mark_published(session, **kwargs)


@pytest.mark.asyncio
async def test_relay_fault_boundary_runs_after_broker_ack_before_database_publish(engine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    repository = OutboxRepository(max_event_bytes=65_536)
    producer = _Producer()
    now = datetime(2026, 8, 23, 12, 0, tzinfo=timezone.utc)
    async with session_factory() as session, session.begin():
        row = await repository.enqueue(
            session,
            topic=DIGEST_RUN_REQUESTED_TOPIC,
            event_key="42",
            event=_event(now),
        )
        event_id = row.event_id

    def crash_boundary():
        raise SystemExit(86)

    relay = OutboxRelay(
        session_factory,
        repository,
        producer,
        KafkaSettings(outbox_lease_seconds=10, outbox_publish_timeout_seconds=5),
        owner="relay-a",
        clock=lambda: now,
        after_broker_ack=crash_boundary,
    )
    with pytest.raises(SystemExit) as error:
        await relay.run_once()
    assert error.value.code == 86
    assert len(producer.calls) == 1
    async with session_factory() as session:
        publishing = await session.get(OutboxEvent, event_id)
        assert publishing.state == "publishing"
        assert publishing.published_at is None


@pytest.mark.asyncio
async def test_relay_republishes_stable_event_after_lost_database_acknowledgement(engine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    repository = _LoseFirstAcknowledgementRepository(max_event_bytes=65_536)
    producer = _Producer()
    now = [datetime(2026, 8, 23, 12, 0, tzinfo=timezone.utc)]
    settings = KafkaSettings(outbox_lease_seconds=10, outbox_publish_timeout_seconds=5, outbox_batch_size=5)
    async with session_factory() as session, session.begin():
        row = await repository.enqueue(
            session,
            topic=DIGEST_RUN_REQUESTED_TOPIC,
            event_key="42",
            event=_event(now[0]),
        )
        event_id = row.event_id
    relay = OutboxRelay(
        session_factory,
        repository,
        producer,
        settings,
        owner="relay-a",
        clock=lambda: now[0],
    )

    with pytest.raises(OSError, match="acknowledgement unavailable"):
        await relay.run_once()

    now[0] += timedelta(seconds=11)
    assert await relay.run_once() == 1

    assert len(producer.calls) == 2
    assert producer.calls[0]["value"] == producer.calls[1]["value"]
    assert producer.calls[0]["event_key"] == producer.calls[1]["event_key"]
    assert str(event_id).encode() in producer.calls[0]["value"]
    async with session_factory() as session:
        published = await session.scalar(select(OutboxEvent).where(OutboxEvent.event_id == event_id))
    assert published.state == "published"
    assert published.publication_attempt_count == 2


@pytest.mark.asyncio
async def test_relay_failure_stores_exception_class_without_exception_message(engine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    repository = OutboxRepository(max_event_bytes=65_536)
    producer = _Producer(RuntimeError("rendered digest and secret token must not persist"))
    now = datetime(2026, 8, 23, 12, 0, tzinfo=timezone.utc)
    async with session_factory() as session, session.begin():
        row = await repository.enqueue(
            session,
            topic=DIGEST_RUN_REQUESTED_TOPIC,
            event_key="42",
            event=_event(now),
        )
        event_id = row.event_id
    relay = OutboxRelay(
        session_factory,
        repository,
        producer,
        KafkaSettings(outbox_backoff_base_seconds=2, outbox_backoff_cap_seconds=60),
        owner="relay-a",
        clock=lambda: now,
        random_uniform=lambda _low, high: high,
    )

    assert await relay.run_once() == 1

    async with session_factory() as session:
        failed = await session.scalar(select(OutboxEvent).where(OutboxEvent.event_id == event_id))
    assert failed.state == "pending"
    assert failed.last_error == "RuntimeError"
    assert failed.next_attempt_at.replace(tzinfo=timezone.utc) == now + timedelta(seconds=2)
    assert "digest" not in failed.last_error.lower()


@pytest.mark.asyncio
async def test_relay_times_out_hung_producer_and_releases_lease_to_backoff(engine) -> None:
    class HungProducer:
        def __init__(self) -> None:
            self.cancelled = False

        async def publish(self, **kwargs):
            try:
                await asyncio.Event().wait()
            finally:
                self.cancelled = True

    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    repository = OutboxRepository(max_event_bytes=65_536)
    producer = HungProducer()
    now = datetime(2026, 8, 23, 12, 0, tzinfo=timezone.utc)
    async with session_factory() as session, session.begin():
        row = await repository.enqueue(
            session,
            topic=DIGEST_RUN_REQUESTED_TOPIC,
            event_key="42",
            event=_event(now),
        )
        event_id = row.event_id
    relay = OutboxRelay(
        session_factory,
        repository,
        producer,
        KafkaSettings(
            outbox_lease_seconds=1,
            outbox_publish_timeout_seconds=0.01,
            outbox_backoff_base_seconds=2,
            outbox_backoff_cap_seconds=60,
        ),
        owner="relay-a",
        clock=lambda: now,
        random_uniform=lambda _low, high: high,
    )

    assert await relay.run_once() == 1

    async with session_factory() as session:
        failed = await session.scalar(select(OutboxEvent).where(OutboxEvent.event_id == event_id))
    assert producer.cancelled is True
    assert failed.state == "pending"
    assert failed.lease_owner is None
    assert failed.lease_until is None
    assert failed.last_error == "TimeoutError"
    assert failed.next_attempt_at.replace(tzinfo=timezone.utc) == now + timedelta(seconds=2)


@pytest.mark.asyncio
async def test_relay_timeout_does_not_wait_for_cancellation_resistant_producer(engine) -> None:
    class CancellationResistantProducer:
        def __init__(self) -> None:
            self.release = asyncio.Event()

        async def publish(self, **kwargs):
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await self.release.wait()

    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    repository = OutboxRepository(max_event_bytes=65_536)
    producer = CancellationResistantProducer()
    now = datetime(2026, 8, 23, 12, 0, tzinfo=timezone.utc)
    async with session_factory() as session, session.begin():
        row = await repository.enqueue(
            session,
            topic=DIGEST_RUN_REQUESTED_TOPIC,
            event_key="42",
            event=_event(now),
        )
        event_id = row.event_id
    relay = OutboxRelay(
        session_factory,
        repository,
        producer,
        KafkaSettings(outbox_lease_seconds=1, outbox_publish_timeout_seconds=0.01),
        owner="relay-a",
        clock=lambda: now,
    )

    try:
        assert await asyncio.wait_for(relay.run_once(), timeout=0.2) == 1
        async with session_factory() as session:
            failed = await session.get(OutboxEvent, event_id)
        assert failed.state == "pending"
        assert failed.last_error == "TimeoutError"
    finally:
        producer.release.set()
        await asyncio.sleep(0)
