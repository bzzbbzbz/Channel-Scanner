from __future__ import annotations

import asyncio
import json
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter, TelegramServerError
from sqlalchemy import func, select
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from src.config.settings import ReliableDeliverySettings
from src.models.channel import Channel, ChannelStatus
from src.models.chat_message import ChatMessage
from src.models.digest_delivery import DigestDelivery
from src.models.digest_processing_log import DigestProcessingLog
from src.models.outbox_event import OutboxEvent
from src.models.post import Post
from src.models.reliable_digest import DigestOutboxMessage, DigestRun, InboxEvent
from src.models.subscription import Subscription, SubscriptionChannel
from src.models.user import DeliveryFrequency, DigestFormat, SummaryMode, User
from src.repository.digest_delivery import DeliveredSummary
from src.repository.outbox import OutboxRepository
from src.repository.reliable_digest import message_claim_statement
from src.reliability.kafka_consumer import ConsumerOutcome, DELIVERY_CONSUMER_GROUP
from src.reliability.telegram_delivery_worker import TelegramDeliveryWorker


class MutableClock:
    def __init__(self, value: datetime) -> None:
        self.value = value

    def __call__(self) -> datetime:
        return self.value


class FakeReliableSender:
    def __init__(self, *responses) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[int, str, str | None]] = []

    async def send_message(self, chat_id: int, text: str, parse_mode: str | None = None) -> int:
        self.calls.append((chat_id, text, parse_mode))
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response

    async def close(self) -> None:
        pass


class TimeoutSender(FakeReliableSender):
    async def send_message(self, chat_id: int, text: str, parse_mode: str | None = None) -> int:
        self.calls.append((chat_id, text, parse_mode))
        await asyncio.sleep(1)
        return 1


def _policy(subscription_id: int, **overrides) -> ReliableDeliverySettings:
    values = {
        "enabled": True,
        "subscription_ids": [subscription_id],
        "inbox_lease_seconds": 10,
        "render_lease_seconds": 10,
        "delivery_lease_seconds": 5,
        "delivery_send_timeout_seconds": 1,
        "delivery_max_attempts": 3,
        "delivery_backoff_base_seconds": 2,
        "delivery_backoff_cap_seconds": 10,
    }
    values.update(overrides)
    return ReliableDeliverySettings(**values)


async def _seed_run(engine: AsyncEngine, now: datetime, *, parts: int = 1):
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session, session.begin():
        user = User(
            telegram_user_id=7001,
            chat_id=7002,
            chat_type="private",
            timezone="UTC",
            language="en",
        )
        session.add(user)
        await session.flush()
        subscription = Subscription(
            user_id=user.id,
            name="Reliable stage 4",
            digest_format=DigestFormat.SHORT,
            summary_mode=SummaryMode.BRIEF,
            frequency=DeliveryFrequency.HOURLY,
            enabled=True,
            created_at=now - timedelta(hours=2),
        )
        session.add(subscription)
        await session.flush()
        channel = Channel(telegram_id=7100, username="stage4", name="Stage 4", status=ChannelStatus.ACTIVE)
        session.add(channel)
        await session.flush()
        session.add(SubscriptionChannel(subscription_id=subscription.id, channel_id=channel.id, subscribed_at=now - timedelta(hours=1)))
        posts = []
        for ordinal in range(parts):
            post = Post(
                post_id=ordinal + 1,
                channel_id=channel.id,
                content=f"post {ordinal}",
                datetime=now - timedelta(minutes=10 - ordinal),
            )
            session.add(post)
            posts.append(post)
        await session.flush()
        run = DigestRun(
            id=uuid4(),
            subscription_id=subscription.id,
            user_id=user.id,
            logical_schedule_slot=now - timedelta(minutes=30),
            correlation_id=uuid4(),
            state="delivering",
            generation=1,
            render_attempt_count=1,
            next_attempt_at=now,
            rendered_at=now,
            created_at=now,
            updated_at=now,
        )
        session.add(run)
        messages = []
        events = []
        for ordinal, post in enumerate(posts):
            message = DigestOutboxMessage(
                id=uuid4(),
                run_id=run.id,
                ordinal=ordinal,
                chat_id=user.chat_id,
                text=f"part {ordinal}",
                parse_mode="HTML",
                outcomes=[asdict(DeliveredSummary(post.id, f"summary {ordinal}", "short", None, None))],
                state="pending",
                generation=1,
                attempt_count=0,
                next_attempt_at=now,
                ambiguous_send=False,
                created_at=now,
                updated_at=now,
            )
            session.add(message)
            messages.append(message)
            events.append(_delivery_event(run, message, now))
        await session.flush()
        return factory, user.id, subscription.id, run.id, messages, events


def _delivery_event(run: DigestRun, message: DigestOutboxMessage, now: datetime, *, event_id=None) -> dict:
    return {
        "event_id": str(event_id or uuid4()),
        "event_type": "tpb.telegram.delivery.requested",
        "event_version": 1,
        "occurred_at": now.isoformat().replace("+00:00", "Z"),
        "correlation_id": str(run.correlation_id),
        "causation_id": str(uuid4()),
        "aggregate_type": "digest_message",
        "aggregate_id": str(message.id),
        "attempt": 1,
        "generation": 1,
        "payload": {"message_id": str(message.id), "run_id": str(run.id), "ordinal": message.ordinal},
    }


def test_message_claim_query_uses_postgresql_skip_locked() -> None:
    statement = message_claim_statement(message_id=uuid4(), now=datetime.now(timezone.utc))
    assert "FOR UPDATE SKIP LOCKED" in str(statement.compile(dialect=postgresql.dialect())).upper()


@pytest.mark.asyncio
async def test_delivery_success_is_atomic_and_duplicate_event_does_not_resend(engine: AsyncEngine) -> None:
    now = datetime(2026, 8, 23, 15, 0, tzinfo=timezone.utc)
    factory, user_id, subscription_id, run_id, messages, events = await _seed_run(engine, now)
    sender = FakeReliableSender(501)
    worker = TelegramDeliveryWorker(factory, sender, _policy(subscription_id), owner="delivery-a", clock=lambda: now)

    assert await worker.handle(events[0]) == ConsumerOutcome.COMMIT
    assert await worker.handle(events[0]) == ConsumerOutcome.COMMIT
    assert len(sender.calls) == 1

    async with factory() as session:
        message = await session.get(DigestOutboxMessage, messages[0].id)
        run = await session.get(DigestRun, run_id)
        delivery = await session.scalar(select(DigestDelivery))
        log = await session.scalar(select(DigestProcessingLog))
        chat = await session.scalar(select(ChatMessage))
        subscription = await session.get(Subscription, subscription_id)
        assert (message.state, message.telegram_message_id, message.attempt_count) == ("sent", 501, 1)
        assert run.state == "completed"
        assert delivery.digest_run_id == run_id
        assert delivery.digest_message_id == messages[0].id
        assert (log.found_count, log.filtered_count, log.included_count) == (1, 0, 1)
        assert log.digest_run_id == run_id
        assert subscription.last_digest_at == now.replace(tzinfo=None)
        assert chat.user_id == user_id
        assert chat.message_metadata["digest_run_id"] == str(run_id)
        assert chat.message_metadata["digest_message_id"] == str(messages[0].id)
        assert await session.scalar(select(func.count()).select_from(DigestProcessingLog)) == 1
        assert await session.scalar(select(func.count()).select_from(ChatMessage)) == 1
        inbox = await session.scalar(select(InboxEvent).where(InboxEvent.consumer_name == DELIVERY_CONSUMER_GROUP))
        assert inbox.state == "completed"


@pytest.mark.asyncio
async def test_different_known_event_after_sent_is_message_deduplicated(engine: AsyncEngine) -> None:
    now = datetime(2026, 8, 23, 15, 0, tzinfo=timezone.utc)
    factory, _, subscription_id, _, messages, events = await _seed_run(engine, now)
    sender = FakeReliableSender(502)
    worker = TelegramDeliveryWorker(factory, sender, _policy(subscription_id), owner="delivery-a", clock=lambda: now)
    assert await worker.handle(events[0]) == ConsumerOutcome.COMMIT

    duplicate = dict(events[0], event_id=str(uuid4()))
    assert await worker.handle(duplicate) == ConsumerOutcome.COMMIT
    assert len(sender.calls) == 1


@pytest.mark.asyncio
async def test_retry_after_is_persisted_as_minimum_then_succeeds(engine: AsyncEngine) -> None:
    now = datetime(2026, 8, 23, 15, 0, tzinfo=timezone.utc)
    factory, _, subscription_id, _, messages, events = await _seed_run(engine, now)
    clock = MutableClock(now)
    retry = TelegramRetryAfter(MagicMock(), "retry", retry_after=7)
    sender = FakeReliableSender(retry, 503)
    worker = TelegramDeliveryWorker(
        factory,
        sender,
        _policy(subscription_id),
        owner="delivery-a",
        clock=clock,
        random_value=lambda: 0.0,
    )

    assert await worker.handle(events[0]) == ConsumerOutcome.COMMIT
    async with factory() as session:
        message = await session.get(DigestOutboxMessage, messages[0].id)
        retry_row = await session.scalar(select(OutboxEvent).where(OutboxEvent.aggregate_id == str(message.id)))
        assert message.state == "retry_wait"
        assert message.attempt_count == 1
        assert message.next_attempt_at == (now + timedelta(seconds=7)).replace(tzinfo=None)
        assert retry_row.attempt == 2
        retry_event = json.loads(OutboxRepository(max_event_bytes=65_536).serialize(retry_row))

    clock.value = now + timedelta(seconds=7)
    restarted_worker = TelegramDeliveryWorker(
        factory,
        sender,
        _policy(subscription_id),
        owner="delivery-b",
        clock=clock,
        random_value=lambda: 0.0,
    )
    assert await restarted_worker.handle(retry_event) == ConsumerOutcome.COMMIT
    async with factory() as session:
        message = await session.get(DigestOutboxMessage, messages[0].id)
        assert (message.state, message.attempt_count, message.telegram_message_id) == ("sent", 2, 503)


@pytest.mark.asyncio
@pytest.mark.parametrize("error_type", [TelegramBadRequest, TelegramForbiddenError])
async def test_telegram_400_and_403_dead_letter_message_and_fail_run(engine: AsyncEngine, error_type) -> None:
    now = datetime(2026, 8, 23, 15, 0, tzinfo=timezone.utc)
    factory, _, subscription_id, run_id, messages, events = await _seed_run(engine, now)
    sender = FakeReliableSender(error_type(MagicMock(), "permanent"))
    worker = TelegramDeliveryWorker(factory, sender, _policy(subscription_id), owner="delivery-a", clock=lambda: now)

    assert await worker.handle(events[0]) == ConsumerOutcome.COMMIT
    async with factory() as session:
        message = await session.get(DigestOutboxMessage, messages[0].id)
        run = await session.get(DigestRun, run_id)
        assert message.state == "dead_letter"
        assert run.state == "failed"
        assert await session.scalar(select(func.count()).select_from(DigestProcessingLog)) == 0


@pytest.mark.asyncio
async def test_send_timeout_is_persisted_as_ambiguous_retry(engine: AsyncEngine) -> None:
    now = datetime(2026, 8, 23, 15, 0, tzinfo=timezone.utc)
    factory, _, subscription_id, _, messages, events = await _seed_run(engine, now)
    sender = TimeoutSender()
    policy = _policy(subscription_id, delivery_send_timeout_seconds=0.01)
    worker = TelegramDeliveryWorker(factory, sender, policy, owner="delivery-a", clock=lambda: now, random_value=lambda: 0.5)

    assert await worker.handle(events[0]) == ConsumerOutcome.COMMIT
    async with factory() as session:
        message = await session.get(DigestOutboxMessage, messages[0].id)
        assert message.state == "retry_wait"
        assert message.ambiguous_send is True
        assert message.attempt_count == 1


@pytest.mark.asyncio
async def test_exhausted_transient_attempts_dead_letter_message(engine: AsyncEngine) -> None:
    now = datetime(2026, 8, 23, 15, 0, tzinfo=timezone.utc)
    factory, _, subscription_id, run_id, messages, events = await _seed_run(engine, now)
    sender = FakeReliableSender(
        TelegramServerError(MagicMock(), "temporary"),
        TelegramServerError(MagicMock(), "temporary"),
    )
    worker = TelegramDeliveryWorker(
        factory,
        sender,
        _policy(subscription_id, delivery_max_attempts=2),
        owner="delivery-a",
        clock=lambda: now,
        random_value=lambda: 0.0,
    )

    assert await worker.handle(events[0]) == ConsumerOutcome.COMMIT
    async with factory() as session:
        retry_row = await session.scalar(select(OutboxEvent).where(OutboxEvent.aggregate_id == str(messages[0].id)))
        retry_event = json.loads(OutboxRepository(max_event_bytes=65_536).serialize(retry_row))
    assert await worker.handle(retry_event) == ConsumerOutcome.COMMIT
    async with factory() as session:
        message = await session.get(DigestOutboxMessage, messages[0].id)
        run = await session.get(DigestRun, run_id)
        assert (message.state, message.attempt_count) == ("dead_letter", 2)
        assert run.state == "failed"


@pytest.mark.asyncio
async def test_invalid_persisted_outcome_is_permanent_without_external_send(engine: AsyncEngine) -> None:
    now = datetime(2026, 8, 23, 15, 0, tzinfo=timezone.utc)
    factory, _, subscription_id, run_id, messages, events = await _seed_run(engine, now)
    async with factory() as session, session.begin():
        message = await session.get(DigestOutboxMessage, messages[0].id)
        message.outcomes = [{"post_id": "invalid"}]
    sender = FakeReliableSender()
    worker = TelegramDeliveryWorker(factory, sender, _policy(subscription_id), owner="delivery-a", clock=lambda: now)

    assert await worker.handle(events[0]) == ConsumerOutcome.COMMIT
    assert sender.calls == []
    async with factory() as session:
        message = await session.get(DigestOutboxMessage, messages[0].id)
        run = await session.get(DigestRun, run_id)
        assert message.state == "dead_letter"
        assert run.state == "failed"


@pytest.mark.asyncio
async def test_partial_multi_part_success_is_not_resent_when_later_part_fails(engine: AsyncEngine) -> None:
    now = datetime(2026, 8, 23, 15, 0, tzinfo=timezone.utc)
    factory, _, subscription_id, run_id, messages, events = await _seed_run(engine, now, parts=2)
    sender = FakeReliableSender(601, TelegramBadRequest(MagicMock(), "bad html"))
    worker = TelegramDeliveryWorker(factory, sender, _policy(subscription_id), owner="delivery-a", clock=lambda: now)

    assert await worker.handle(events[0]) == ConsumerOutcome.COMMIT
    assert await worker.handle(events[1]) == ConsumerOutcome.COMMIT
    assert await worker.handle(events[0]) == ConsumerOutcome.COMMIT
    assert [call[1] for call in sender.calls] == ["part 0", "part 1"]

    async with factory() as session:
        stored = list((await session.scalars(select(DigestOutboxMessage).order_by(DigestOutboxMessage.ordinal))).all())
        run = await session.get(DigestRun, run_id)
        deliveries = list((await session.scalars(select(DigestDelivery))).all())
        assert [message.state for message in stored] == ["sent", "dead_letter"]
        assert run.state == "failed"
        assert len(deliveries) == 1
        assert deliveries[0].digest_message_id == messages[0].id


@pytest.mark.asyncio
async def test_parts_after_permanent_failure_are_not_sent(engine: AsyncEngine) -> None:
    now = datetime(2026, 8, 23, 15, 0, tzinfo=timezone.utc)
    factory, _, subscription_id, run_id, messages, events = await _seed_run(engine, now, parts=3)
    sender = FakeReliableSender(801, TelegramBadRequest(MagicMock(), "bad html"))
    worker = TelegramDeliveryWorker(factory, sender, _policy(subscription_id), owner="delivery-a", clock=lambda: now)

    assert await worker.handle(events[0]) == ConsumerOutcome.COMMIT
    assert await worker.handle(events[1]) == ConsumerOutcome.COMMIT
    assert await worker.handle(events[2]) == ConsumerOutcome.COMMIT

    assert [call[1] for call in sender.calls] == ["part 0", "part 1"]
    async with factory() as session:
        stored = list((await session.scalars(select(DigestOutboxMessage).order_by(DigestOutboxMessage.ordinal))).all())
        run = await session.get(DigestRun, run_id)
        assert [message.state for message in stored] == ["sent", "dead_letter", "dead_letter"]
        assert stored[2].last_error == "DigestRunFailed"
        assert run.state == "failed"


@pytest.mark.asyncio
async def test_stale_sending_lease_is_reclaimed_and_marked_ambiguous(engine: AsyncEngine) -> None:
    now = datetime(2026, 8, 23, 15, 0, tzinfo=timezone.utc)
    factory, _, subscription_id, _, messages, events = await _seed_run(engine, now)
    async with factory() as session, session.begin():
        message = await session.get(DigestOutboxMessage, messages[0].id)
        message.state = "sending"
        message.lease_owner = "dead-worker"
        message.lease_until = now - timedelta(seconds=1)
        message.attempt_count = 1

    sender = FakeReliableSender(701)
    worker = TelegramDeliveryWorker(factory, sender, _policy(subscription_id), owner="delivery-b", clock=lambda: now)
    assert await worker.handle(events[0]) == ConsumerOutcome.COMMIT

    async with factory() as session:
        message = await session.get(DigestOutboxMessage, messages[0].id)
        assert message.state == "sent"
        assert message.attempt_count == 2
        assert message.ambiguous_send is True
