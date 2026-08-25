from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from sqlalchemy import delete, func, select
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from src.config.settings import LlmSettings, ReliableDeliverySettings
from src.digest.service import DigestService
from src.models.channel import Channel, ChannelStatus
from src.models.digest_processing_log import DigestProcessingLog
from src.models.outbox_event import OutboxEvent
from src.models.post import Post
from src.models.reliable_digest import DigestOutboxMessage, DigestRun, InboxEvent
from src.models.subscription import Subscription, SubscriptionChannel
from src.models.user import DeliveryFrequency, DigestFormat, SummaryMode, User
from src.reliability.digest_worker import DigestWorker
from src.reliability.kafka_consumer import ConsumerOutcome, DIGEST_CONSUMER_GROUP
from src.reliability.scheduler import ReliableDigestScheduler
from src.repository.inbox import InboxRepository
from src.repository.outbox import OutboxRepository
from src.repository.reliable_digest import ReliableDigestRepository
from src.repository.reliable_digest import run_claim_statement


class FakeSender:
    def __init__(self) -> None:
        self.messages: list[str] = []

    async def send_message(self, chat_id: int, text: str, parse_mode: str | None = None) -> None:
        self.messages.append(text)

    async def close(self) -> None:
        pass


async def _seed(session, *, two_subscriptions: bool = False):
    user = User(
        telegram_user_id=9001,
        chat_id=9002,
        chat_type="private",
        timezone="UTC",
        language="en",
    )
    session.add(user)
    await session.flush()
    subscriptions = []
    for index in range(2 if two_subscriptions else 1):
        subscription = Subscription(
            user_id=user.id,
            name=f"Reliable {index}",
            digest_format=DigestFormat.SHORT,
            summary_mode=SummaryMode.BRIEF,
            frequency=DeliveryFrequency.HOURLY,
            notification_cron="0 * * * *",
            enabled=True,
            created_at=datetime(2026, 8, 23, 8, 0, tzinfo=timezone.utc),
        )
        session.add(subscription)
        await session.flush()
        subscriptions.append(subscription)
    channel = Channel(telegram_id=9010, username="reliable", name="Reliable", status=ChannelStatus.ACTIVE)
    session.add(channel)
    await session.flush()
    for subscription in subscriptions:
        session.add(SubscriptionChannel(
            subscription_id=subscription.id,
            channel_id=channel.id,
            subscribed_at=datetime(2026, 8, 23, 8, 0, tzinfo=timezone.utc),
        ))
    session.add(Post(
        post_id=1,
        channel_id=channel.id,
        content="persist me before delivery",
        datetime=datetime(2026, 8, 23, 9, 30, tzinfo=timezone.utc),
    ))
    await session.commit()
    return user, subscriptions


def _event_from_row(repository: OutboxRepository, row: OutboxEvent) -> dict:
    return json.loads(repository.serialize(row))


def test_run_claim_query_uses_postgresql_skip_locked() -> None:
    statement = run_claim_statement(
        run_id=uuid4(),
        now=datetime.now(timezone.utc),
    )
    assert "FOR UPDATE SKIP LOCKED" in str(statement.compile(dialect=postgresql.dialect())).upper()


@pytest.mark.asyncio
async def test_scheduler_creates_unique_run_and_root_event_in_one_transaction(engine: AsyncEngine) -> None:
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        _, subscriptions = await _seed(session)
    policy = ReliableDeliverySettings(enabled=True, subscription_ids=[subscriptions[0].id])
    outbox = OutboxRepository(max_event_bytes=65_536)
    scheduler = ReliableDigestScheduler(factory, outbox, policy)
    now = datetime(2026, 8, 23, 12, 37, tzinfo=timezone.utc)

    assert await scheduler.run_once(now) == 1
    assert await scheduler.run_once(now) == 0

    async with factory() as session:
        run = await session.scalar(select(DigestRun))
        root = await session.scalar(select(OutboxEvent))
        assert run.logical_schedule_slot == datetime(2026, 8, 23, 12, 0)
        assert root.aggregate_id == str(run.id)
        assert root.payload["logical_schedule_slot"] == "2026-08-23T12:00:00Z"
        assert await session.scalar(select(func.count()).select_from(DigestRun)) == 1
        assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 1


@pytest.mark.asyncio
async def test_worker_persists_all_parts_and_delivery_events_without_rerender(engine: AsyncEngine, monkeypatch) -> None:
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        user, subscriptions = await _seed(session)
    policy = ReliableDeliverySettings(enabled=True, subscription_ids=[subscriptions[0].id])
    outbox = OutboxRepository(max_event_bytes=65_536)
    scheduler = ReliableDigestScheduler(factory, outbox, policy)
    now = datetime(2026, 8, 23, 12, 37, tzinfo=timezone.utc)
    await scheduler.run_once(now)
    async with factory() as session:
        root = await session.scalar(select(OutboxEvent))
        event = _event_from_row(outbox, root)

    render_calls = 0
    from src.digest.service import PreparedDigestMessage
    from src.repository.digest_delivery import DeliveredSummary

    async def fake_render(*args, **kwargs):
        nonlocal render_calls
        render_calls += 1
        return [
            PreparedDigestMessage("part 1", [DeliveredSummary(1, "one", "short", None, None)]),
            PreparedDigestMessage("part 2", []),
        ]

    monkeypatch.setattr("src.reliability.digest_worker.build_digest_messages", fake_render)
    worker = DigestWorker(factory, outbox, policy, LlmSettings(), owner="worker-a", clock=lambda: now)

    assert await worker.handle(event) == ConsumerOutcome.COMMIT
    assert await worker.handle(event) == ConsumerOutcome.COMMIT
    assert render_calls == 1

    async with factory() as session:
        run = await session.scalar(select(DigestRun))
        messages = list((await session.scalars(select(DigestOutboxMessage).order_by(DigestOutboxMessage.ordinal))).all())
        events = list((await session.scalars(select(OutboxEvent).order_by(OutboxEvent.occurred_at))).all())
        inbox = await session.scalar(select(InboxEvent))
        assert run.state == "delivering"
        assert [(message.ordinal, message.text, message.chat_id) for message in messages] == [
            (0, "part 1", user.chat_id),
            (1, "part 2", user.chat_id),
        ]
        assert messages[0].outcomes[0]["post_id"] == 1
        assert len(events) == 3
        assert {event.payload["message_id"] for event in events[1:]} == {str(message.id) for message in messages}
        assert inbox.state == "completed"
        assert inbox.processing_attempt_count == 1


@pytest.mark.asyncio
async def test_worker_recovers_stale_inbox_and_run_leases(engine: AsyncEngine, monkeypatch) -> None:
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        _, subscriptions = await _seed(session)
    policy = ReliableDeliverySettings(
        enabled=True,
        subscription_ids=[subscriptions[0].id],
        inbox_lease_seconds=10,
        render_lease_seconds=10,
        delivery_lease_seconds=5,
        delivery_send_timeout_seconds=1,
    )
    outbox = OutboxRepository(max_event_bytes=65_536)
    now = datetime(2026, 8, 23, 12, 0, tzinfo=timezone.utc)
    await ReliableDigestScheduler(factory, outbox, policy).run_once(now)
    async with factory() as session:
        root = await session.scalar(select(OutboxEvent))
        event = _event_from_row(outbox, root)
        run_id = root.aggregate_id
    async with factory() as session, session.begin():
        await InboxRepository().claim(
            session,
            consumer_name=DIGEST_CONSUMER_GROUP,
            event_id=root.event_id,
            attempt=1,
            generation=1,
            owner="dead-worker",
            now=now,
            lease_seconds=10,
        )
        await ReliableDigestRepository(outbox).claim_run(
            session,
            run_id=root.event_id.__class__(run_id),
            owner="dead-worker",
            now=now,
            lease_seconds=10,
        )

    async def fake_render(*args, **kwargs):
        return []

    monkeypatch.setattr("src.reliability.digest_worker.build_digest_messages", fake_render)
    worker = DigestWorker(factory, outbox, policy, owner="worker-b", clock=lambda: now + timedelta(seconds=11))
    assert await worker.handle(event) == ConsumerOutcome.COMMIT

    async with factory() as session:
        run = await session.get(DigestRun, root.event_id.__class__(run_id))
        inbox = await session.scalar(select(InboxEvent))
        processing_log = await session.scalar(select(DigestProcessingLog))
        assert run.state == "completed"
        assert run.render_attempt_count == 2
        assert inbox.state == "completed"
        assert inbox.processing_attempt_count == 2
        assert (processing_log.found_count, processing_log.filtered_count, processing_log.included_count) == (0, 0, 0)


@pytest.mark.asyncio
async def test_worker_atomically_completes_run_with_no_pending_posts_and_emits_no_delivery_event(
    engine: AsyncEngine,
    monkeypatch,
) -> None:
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        _, subscriptions = await _seed(session)
        await session.execute(delete(Post))
        await session.commit()
    policy = ReliableDeliverySettings(enabled=True, subscription_ids=[subscriptions[0].id])
    outbox = OutboxRepository(max_event_bytes=65_536)
    now = datetime(2026, 8, 23, 12, 37, tzinfo=timezone.utc)
    await ReliableDigestScheduler(factory, outbox, policy).run_once(now)
    async with factory() as session:
        root = await session.scalar(select(OutboxEvent))
        event = _event_from_row(outbox, root)

    async def unexpected_render(*args, **kwargs):
        raise AssertionError("empty runs must not invoke the digest builder")

    monkeypatch.setattr("src.reliability.digest_worker.build_digest_messages", unexpected_render)
    worker = DigestWorker(factory, outbox, policy, owner="worker-empty", clock=lambda: now)
    assert await worker.handle(event) == ConsumerOutcome.COMMIT

    async with factory() as session:
        run = await session.scalar(select(DigestRun))
        inbox = await session.scalar(select(InboxEvent))
        log = await session.scalar(select(DigestProcessingLog))
        subscription = await session.get(Subscription, subscriptions[0].id)
        assert run.state == "completed"
        assert inbox.state == "completed"
        assert (log.found_count, log.filtered_count, log.included_count) == (0, 0, 0)
        assert log.digest_run_id == run.id
        assert subscription.last_digest_at == now.replace(tzinfo=None)
        assert await session.scalar(select(func.count()).select_from(DigestOutboxMessage)) == 0
        assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 1


@pytest.mark.asyncio
async def test_legacy_service_skips_subscription_owned_by_reliable_policy(engine: AsyncEngine) -> None:
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        _, subscriptions = await _seed(session, two_subscriptions=True)
    policy = ReliableDeliverySettings(enabled=True, subscription_ids=[subscriptions[0].id])
    sender = FakeSender()

    delivered = await DigestService(
        factory,
        bot_token="test",
        sender=sender,
        reliable_delivery=policy,
    ).run_once(datetime(2026, 8, 23, 12, 0, tzinfo=timezone.utc))

    assert delivered == 1
    assert len(sender.messages) == 1
