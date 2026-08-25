from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import httpx
import pytest
from aiogram.exceptions import TelegramBadRequest
from sqlalchemy import func, select, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from src.admin.app import create_admin_app
from src.admin.passwords import hash_password
from src.config.settings import AdminSettings, KafkaSettings, ReliableDeliverySettings
from src.models.channel import Channel, ChannelStatus
from src.models.dead_letter import DeadLetterRecord, DeadLetterReplay
from src.models.digest_delivery import DigestDelivery
from src.models.digest_processing_log import DigestProcessingLog
from src.models.outbox_event import OutboxEvent
from src.models.post import Post
from src.models.reliable_digest import DigestOutboxMessage, DigestRun
from src.models.subscription import Subscription
from src.models.user import User
from src.repository.dead_letter import DeadLetterRepository
from src.repository.outbox import OutboxRepository
from src.repository.reliable_digest import ReliableDigestRepository
from src.repository.subscription import SubscriptionRepository
from src.reliability.dead_letter_replay import DeadLetterReplayService, dead_letter_lock_statement
from src.reliability.kafka_consumer import ConsumerOutcome, KafkaDigestConsumer
from src.reliability.telegram_delivery_worker import TelegramDeliveryWorker


class FakeSender:
    def __init__(self, response) -> None:
        self.response = response
        self.calls = 0

    async def send_message(self, chat_id: int, text: str, parse_mode: str | None = None) -> int:
        self.calls += 1
        if isinstance(self.response, BaseException):
            raise self.response
        return self.response

    async def close(self) -> None:
        pass


async def _seed_run(
    engine: AsyncEngine,
    now: datetime,
    *,
    state: str,
    with_message: bool,
) -> tuple[async_sessionmaker, DigestRun, DigestOutboxMessage | None]:
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session, session.begin():
        user = User(telegram_user_id=8801, chat_id=8802, timezone="UTC", language="en")
        session.add(user)
        await session.flush()
        subscription = Subscription(user_id=user.id, name="Stage 5", enabled=True)
        session.add(subscription)
        await session.flush()
        run = DigestRun(
            id=uuid4(),
            subscription_id=subscription.id,
            user_id=user.id,
            logical_schedule_slot=now - timedelta(hours=1),
            correlation_id=uuid4(),
            generation=1,
            state=state,
            render_attempt_count=0,
            next_attempt_at=now,
            rendered_at=now if state in {"delivering", "failed"} and with_message else None,
            created_at=now,
            updated_at=now,
        )
        session.add(run)
        message = None
        if with_message:
            message = DigestOutboxMessage(
                id=uuid4(),
                run_id=run.id,
                ordinal=0,
                chat_id=user.chat_id,
                text="TOP SECRET DIGEST BODY",
                parse_mode="HTML",
                outcomes=[],
                state="pending" if state == "delivering" else "dead_letter",
                generation=1,
                attempt_count=0,
                next_attempt_at=now,
                ambiguous_send=False,
                created_at=now,
                updated_at=now,
            )
            session.add(message)
        await session.flush()
    return factory, run, message


def _delivery_event(run: DigestRun, message: DigestOutboxMessage, now: datetime, event_id: UUID) -> dict:
    return {
        "event_id": str(event_id),
        "event_type": "tpb.telegram.delivery.requested",
        "event_version": 1,
        "occurred_at": now.isoformat().replace("+00:00", "Z"),
        "correlation_id": str(run.correlation_id),
        "causation_id": str(uuid4()),
        "aggregate_type": "digest_message",
        "aggregate_id": str(message.id),
        "attempt": 1,
        "generation": 1,
        "payload": {"message_id": str(message.id), "run_id": str(run.id), "ordinal": 0},
    }


@pytest.mark.asyncio
async def test_terminal_telegram_failure_atomically_creates_one_record_and_dlq_outbox(engine: AsyncEngine) -> None:
    now = datetime(2026, 8, 23, 18, 0, tzinfo=timezone.utc)
    factory, run, message = await _seed_run(engine, now, state="delivering", with_message=True)
    event_id = uuid4()
    event = _delivery_event(run, message, now, event_id)
    sender = FakeSender(TelegramBadRequest(MagicMock(), "sensitive provider error"))
    worker = TelegramDeliveryWorker(
        factory,
        sender,
        ReliableDeliverySettings(
            enabled=True,
            subscription_ids=[run.subscription_id],
            delivery_lease_seconds=5,
            delivery_send_timeout_seconds=1,
        ),
        owner="stage5-delivery",
        clock=lambda: now,
    )

    assert await worker.handle(event) == ConsumerOutcome.COMMIT
    assert await worker.handle(event) == ConsumerOutcome.COMMIT

    async with factory() as session:
        record = await session.scalar(select(DeadLetterRecord))
        outbox = await session.scalar(select(OutboxEvent))
        stored_message = await session.get(DigestOutboxMessage, message.id)
        stored_run = await session.get(DigestRun, run.id)
        assert stored_message.state == "dead_letter"
        assert stored_run.state == "failed"
        assert record.message_id == message.id
        assert record.source_event_id == event_id
        assert record.error_code == "TelegramBadRequest"
        assert record.attempt_summary == {"attempt_count": 1, "max_attempts": 10, "ambiguous": False}
        assert record.dlq_outbox_event_id == outbox.event_id
        assert outbox.topic == "tpb.telegram.delivery.requested.dlq.v1"
        assert outbox.payload == {
            "dead_letter_id": str(record.id),
            "message_id": str(message.id),
            "reason": "permanent_failure",
        }
        assert await session.scalar(select(func.count()).select_from(DeadLetterRecord)) == 1
        assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 1
        assert "TOP SECRET" not in json.dumps(outbox.payload)


@pytest.mark.asyncio
async def test_terminal_transition_rolls_back_when_dlq_outbox_enqueue_fails(engine: AsyncEngine) -> None:
    now = datetime(2026, 8, 23, 18, 0, tzinfo=timezone.utc)
    factory, run, message = await _seed_run(engine, now, state="delivering", with_message=True)

    class FailingOutbox(OutboxRepository):
        async def enqueue(self, *args, **kwargs):
            raise RuntimeError("outbox_unavailable")

    repository = ReliableDigestRepository(FailingOutbox(max_event_bytes=65_536))
    async with factory() as session, session.begin():
        claimed = await repository.claim_message(
            session,
            message_id=message.id,
            owner="delivery",
            now=now,
            lease_seconds=30,
        )
        assert claimed is not None

    with pytest.raises(RuntimeError, match="outbox_unavailable"):
        async with factory() as session, session.begin():
            await repository.mark_message_failure(
                session,
                message_id=message.id,
                owner="delivery",
                error_code="TelegramBadRequest",
                permanent=True,
                ambiguous=False,
                retry_at=now,
                max_attempts=10,
                causation_id=uuid4(),
                now=now,
            )

    async with factory() as session:
        stored_message = await session.get(DigestOutboxMessage, message.id)
        stored_run = await session.get(DigestRun, run.id)
        assert stored_message.state == "sending"
        assert stored_run.state == "delivering"
        assert await session.scalar(select(func.count()).select_from(DeadLetterRecord)) == 0
        assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 0


@pytest.mark.asyncio
async def test_terminal_render_failure_creates_one_generation_and_replays_to_pending(engine: AsyncEngine) -> None:
    now = datetime(2026, 8, 23, 18, 0, tzinfo=timezone.utc)
    factory, run, _ = await _seed_run(engine, now, state="pending", with_message=False)
    outbox = OutboxRepository(max_event_bytes=65_536)
    repository = ReliableDigestRepository(outbox)
    source_event_id = uuid4()
    async with factory() as session, session.begin():
        claimed = await repository.claim_run(
            session,
            run_id=run.id,
            owner="renderer",
            now=now,
            lease_seconds=30,
        )
        assert claimed is not None
        await repository.mark_render_failure(
            session,
            run_id=run.id,
            owner="renderer",
            error_code="RenderProviderError",
            max_attempts=1,
            now=now,
            source_event_id=source_event_id,
        )

    replay_service = DeadLetterReplayService(factory, outbox, clock=lambda: now + timedelta(minutes=1))
    async with factory() as session:
        record = await session.scalar(select(DeadLetterRecord))
        assert record.work_type == "digest_run"
        assert record.dlq_outbox_event_id is not None

    first = await replay_service.replay(record.id, idempotency_key="render-replay-1", actor="admin")
    duplicate = await replay_service.replay(record.id, idempotency_key="render-replay-1", actor="admin")
    assert first == duplicate

    async with factory() as session:
        stored_run = await session.get(DigestRun, run.id)
        replays = list((await session.scalars(select(DeadLetterReplay))).all())
        root = await session.scalar(
            select(OutboxEvent).where(OutboxEvent.event_type == "tpb.digest.run.requested")
        )
        assert (stored_run.state, stored_run.generation, stored_run.render_attempt_count) == ("pending", 2, 0)
        assert (root.generation, root.attempt, root.aggregate_id) == (2, 1, str(run.id))
        assert len(replays) == 1
        assert replays[0].outbox_event_id == root.event_id
        assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 2


@pytest.mark.asyncio
async def test_message_replay_resets_only_failed_part_and_is_idempotent(engine: AsyncEngine) -> None:
    now = datetime(2026, 8, 23, 18, 0, tzinfo=timezone.utc)
    factory, run, message = await _seed_run(engine, now, state="failed", with_message=True)
    outbox = OutboxRepository(max_event_bytes=65_536)
    async with factory() as session, session.begin():
        record, _ = await DeadLetterRepository(outbox).record_terminal(
            session,
            source_topic="tpb.telegram.delivery.requested.v1",
            source_event_id=uuid4(),
            source_partition=None,
            source_offset=None,
            work_type="digest_message",
            entity_ref=str(message.id),
            run_id=run.id,
            message_id=message.id,
            subscription_id=run.subscription_id,
            correlation_id=run.correlation_id,
            terminal_reason="attempts_exhausted",
            error_code="TelegramServerError",
            attempt_summary={"attempt_count": 10, "max_attempts": 10, "ambiguous": True},
            generation=1,
            failed_at=now,
        )

    service = DeadLetterReplayService(factory, outbox, clock=lambda: now + timedelta(minutes=1))
    first = await service.replay(record.id, idempotency_key="message-replay-1", actor="admin")
    second = await service.replay(record.id, idempotency_key="message-replay-1", actor="admin")
    assert first == second

    async with factory() as session:
        stored_message = await session.get(DigestOutboxMessage, message.id)
        stored_run = await session.get(DigestRun, run.id)
        replay_event = await session.scalar(
            select(OutboxEvent).where(OutboxEvent.event_type == "tpb.telegram.delivery.requested")
        )
        assert (stored_message.state, stored_message.generation, stored_message.attempt_count) == ("pending", 2, 0)
        assert stored_message.ambiguous_send is False
        assert (stored_run.state, stored_run.generation) == ("delivering", 2)
        assert replay_event.payload["message_id"] == str(message.id)
        assert replay_event.generation == 2
        assert await session.scalar(select(func.count()).select_from(DeadLetterReplay)) == 1


@pytest.mark.asyncio
async def test_missing_replay_entity_is_audited_without_new_work(engine: AsyncEngine) -> None:
    now = datetime(2026, 8, 23, 18, 0, tzinfo=timezone.utc)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    outbox = OutboxRepository(max_event_bytes=65_536)
    missing_id = uuid4()
    async with factory() as session, session.begin():
        record, _ = await DeadLetterRepository(outbox).record_terminal(
            session,
            source_topic="tpb.digest.run.requested.v1",
            source_event_id=uuid4(),
            source_partition=None,
            source_offset=None,
            work_type="digest_run",
            entity_ref=str(missing_id),
            run_id=None,
            message_id=None,
            subscription_id=None,
            correlation_id=uuid4(),
            terminal_reason="contract_rejected",
            error_code="InvalidEventSchema",
            attempt_summary={"attempt_count": 1},
            generation=1,
            failed_at=now,
        )

    result = await DeadLetterReplayService(factory, outbox, clock=lambda: now).replay(
        record.id,
        idempotency_key="missing-entity-1",
        actor="admin",
    )
    assert result.result == "replay_rejected"
    assert result.error_code == "ReplayEntityMissing"
    assert result.outbox_event_id is None
    async with factory() as session:
        stored = await session.get(DeadLetterRecord, record.id)
        assert stored.status == "replay_rejected"
        assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 1


@pytest.mark.asyncio
async def test_subscription_delete_cascades_reliable_work_but_preserves_dead_letter_history(
    engine: AsyncEngine,
) -> None:
    now = datetime(2026, 8, 23, 18, 0, tzinfo=timezone.utc)
    async with engine.begin() as connection:
        await connection.execute(text("PRAGMA foreign_keys=ON"))
    factory = async_sessionmaker(engine, expire_on_commit=False)
    outbox = OutboxRepository(max_event_bytes=65_536)
    first_source_event_id = uuid4()
    async with factory() as session, session.begin():
        user = User(telegram_user_id=8851, chat_id=8852, timezone="UTC", language="en")
        channel = Channel(telegram_id=8853, username="delete_stage5", status=ChannelStatus.ACTIVE)
        session.add_all([user, channel])
        await session.flush()
        subscription = Subscription(user_id=user.id, name="Delete reliable history", enabled=True)
        post = Post(post_id=1, channel_id=channel.id, content="deleted work content", datetime=now)
        session.add_all([subscription, post])
        await session.flush()
        run = DigestRun(
            id=uuid4(),
            subscription_id=subscription.id,
            user_id=user.id,
            logical_schedule_slot=now - timedelta(hours=1),
            correlation_id=uuid4(),
            generation=1,
            state="failed",
            render_attempt_count=1,
            next_attempt_at=now,
            rendered_at=now,
            last_error="TelegramBadRequest",
            created_at=now,
            updated_at=now,
        )
        session.add(run)
        await session.flush()
        message = DigestOutboxMessage(
            id=uuid4(),
            run_id=run.id,
            ordinal=0,
            chat_id=user.chat_id,
            text="deleted persisted message",
            parse_mode="HTML",
            outcomes=[],
            state="dead_letter",
            generation=1,
            attempt_count=1,
            next_attempt_at=now,
            ambiguous_send=False,
            last_error="TelegramBadRequest",
            created_at=now,
            updated_at=now,
        )
        session.add(message)
        await session.flush()
        session.add_all(
            [
                DigestDelivery(
                    user_id=user.id,
                    subscription_id=subscription.id,
                    post_id=post.id,
                    status="delivered",
                    digest_run_id=run.id,
                    digest_message_id=message.id,
                    delivered_at=now,
                ),
                DigestProcessingLog(
                    user_id=user.id,
                    subscription_id=subscription.id,
                    digest_run_id=run.id,
                    found_count=1,
                    filtered_count=0,
                    included_count=1,
                    completed_at=now,
                ),
            ]
        )
        first_record, _ = await DeadLetterRepository(outbox).record_terminal(
            session,
            source_topic="tpb.telegram.delivery.requested.v1",
            source_event_id=first_source_event_id,
            source_partition=0,
            source_offset=12,
            work_type="digest_message",
            entity_ref=str(message.id),
            run_id=run.id,
            message_id=message.id,
            subscription_id=subscription.id,
            correlation_id=run.correlation_id,
            terminal_reason="permanent_failure",
            error_code="TelegramBadRequest",
            attempt_summary={"attempt_count": 1},
            generation=1,
            failed_at=now,
        )
        await session.flush()
        subscription_id = subscription.id
        run_id = run.id
        message_id = message.id
        first_record_id = first_record.id

    replayed = await DeadLetterReplayService(factory, outbox, clock=lambda: now + timedelta(seconds=1)).replay(
        first_record_id,
        idempotency_key="preserved-history-replay-1",
        actor="history-admin",
    )
    assert replayed.result == "replayed"
    async with factory() as session, session.begin():
        run = await session.get(DigestRun, run_id)
        message = await session.get(DigestOutboxMessage, message_id)
        run.state = "failed"
        run.last_error = "TelegramBadRequest"
        message.state = "dead_letter"
        message.attempt_count = 1
        message.last_error = "TelegramBadRequest"
        source_event_id = uuid4()
        record, _ = await DeadLetterRepository(outbox).record_terminal(
            session,
            source_topic="tpb.telegram.delivery.requested.v1",
            source_event_id=source_event_id,
            source_partition=0,
            source_offset=13,
            work_type="digest_message",
            entity_ref=str(message.id),
            run_id=run.id,
            message_id=message.id,
            subscription_id=run.subscription_id,
            correlation_id=run.correlation_id,
            terminal_reason="permanent_failure",
            error_code="TelegramBadRequest",
            attempt_summary={"attempt_count": 1},
            generation=message.generation,
            failed_at=now + timedelta(seconds=2),
        )
        await session.flush()
        record_id = record.id
        entity_ref = record.entity_ref
        correlation_id = record.correlation_id
        dlq_outbox_event_id = record.dlq_outbox_event_id

    async with factory() as session, session.begin():
        assert await SubscriptionRepository(session).delete(subscription_id) is True

    async with factory() as session:
        stored_record = await session.get(DeadLetterRecord, record_id)
        assert await session.get(Subscription, subscription_id) is None
        assert await session.get(DigestRun, run_id) is None
        assert await session.get(DigestOutboxMessage, message_id) is None
        assert await session.scalar(select(func.count()).select_from(DigestDelivery)) == 0
        assert await session.scalar(select(func.count()).select_from(DigestProcessingLog)) == 0
        assert stored_record is not None
        assert (stored_record.run_id, stored_record.message_id, stored_record.subscription_id) == (None, None, None)
        assert stored_record.entity_ref == entity_ref
        assert stored_record.correlation_id == correlation_id
        assert stored_record.source_event_id == source_event_id
        assert stored_record.dlq_outbox_event_id == dlq_outbox_event_id
        assert await session.get(OutboxEvent, dlq_outbox_event_id) is not None
        first_stored_record = await session.get(DeadLetterRecord, first_record_id)
        historical_audit = await session.scalar(
            select(DeadLetterReplay).where(DeadLetterReplay.dead_letter_id == first_record_id)
        )
        assert (first_stored_record.run_id, first_stored_record.message_id, first_stored_record.subscription_id) == (
            None,
            None,
            None,
        )
        assert first_stored_record.entity_ref == entity_ref
        assert first_stored_record.correlation_id == correlation_id
        assert first_stored_record.source_event_id == first_source_event_id
        assert (historical_audit.actor, historical_audit.result, historical_audit.outbox_event_id) == (
            "history-admin",
            "replayed",
            replayed.outbox_event_id,
        )

    result = await DeadLetterReplayService(factory, outbox, clock=lambda: now + timedelta(minutes=1)).replay(
        record_id,
        idempotency_key="deleted-subscription-replay-1",
        actor="admin",
    )
    assert result.result == "replay_rejected"
    assert result.error_code == "ReplayEntityMissing"
    assert result.outbox_event_id is None
    async with factory() as session:
        stored_record = await session.get(DeadLetterRecord, record_id)
        audit = await session.scalar(select(DeadLetterReplay).where(DeadLetterReplay.dead_letter_id == record_id))
        assert stored_record.status == "replay_rejected"
        assert stored_record.entity_ref == entity_ref
        assert stored_record.correlation_id == correlation_id
        assert (audit.actor, audit.result, audit.error_code) == (
            "admin",
            "replay_rejected",
            "ReplayEntityMissing",
        )
        assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 3


@pytest.mark.asyncio
async def test_consumer_commits_unreadable_offset_without_kafka_dlq_event(engine: AsyncEngine) -> None:
    now = datetime(2026, 8, 23, 18, 0, tzinfo=timezone.utc)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    worker_factory, run, _ = await _seed_run(engine, now, state="pending", with_message=False)
    outbox = OutboxRepository(max_event_bytes=65_536)
    from src.reliability.digest_worker import DigestWorker

    worker = DigestWorker(
        worker_factory,
        outbox,
        ReliableDeliverySettings(enabled=True, subscription_ids=[run.subscription_id]),
        owner="rejector",
        clock=lambda: now,
    )
    raw = MagicMock()
    raw.getone = AsyncMock(
        return_value=SimpleNamespace(
            topic="tpb.digest.run.requested.v1",
            partition=2,
            offset=91,
            value=b"not-json-and-no-identifiers",
        )
    )
    raw.commit = AsyncMock()
    consumer = KafkaDigestConsumer(
        KafkaSettings(),
        ReliableDeliverySettings(),
        AsyncMock(),
        rejection_handler=worker.handle_rejected,
        consumer_factory=lambda *_: raw,
    )

    assert await consumer.consume_one() is True
    raw.commit.assert_awaited_once()
    async with factory() as session:
        record = await session.scalar(select(DeadLetterRecord))
        assert record.work_type == "unreadable_event"
        assert (record.source_topic, record.source_partition, record.source_offset) == (
            "tpb.digest.run.requested.v1",
            2,
            91,
        )
        assert record.source_event_id is None
        assert record.dlq_outbox_event_id is None
        assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 0


@pytest.mark.asyncio
async def test_consumer_turns_identifiable_unsupported_version_into_permanent_dlq(engine: AsyncEngine) -> None:
    now = datetime(2026, 8, 23, 18, 0, tzinfo=timezone.utc)
    factory, run, _ = await _seed_run(engine, now, state="pending", with_message=False)
    outbox = OutboxRepository(max_event_bytes=65_536)
    from src.reliability.digest_worker import DigestWorker

    worker = DigestWorker(
        factory,
        outbox,
        ReliableDeliverySettings(enabled=True, subscription_ids=[run.subscription_id]),
        owner="rejector",
        clock=lambda: now,
    )
    source_event_id = uuid4()
    unsupported = {
        "event_id": str(source_event_id),
        "event_type": "tpb.digest.run.requested",
        "event_version": 2,
        "occurred_at": now.isoformat().replace("+00:00", "Z"),
        "correlation_id": str(run.correlation_id),
        "causation_id": None,
        "aggregate_type": "digest_run",
        "aggregate_id": str(run.id),
        "attempt": 1,
        "generation": 1,
        "payload": {
            "run_id": str(run.id),
            "subscription_id": run.subscription_id,
            "logical_schedule_slot": now.isoformat().replace("+00:00", "Z"),
        },
    }
    raw = MagicMock()
    raw.getone = AsyncMock(
        return_value=SimpleNamespace(
            topic="tpb.digest.run.requested.v1",
            partition=1,
            offset=33,
            value=json.dumps(unsupported).encode(),
        )
    )
    raw.commit = AsyncMock()
    consumer = KafkaDigestConsumer(
        KafkaSettings(),
        ReliableDeliverySettings(),
        AsyncMock(),
        rejection_handler=worker.handle_rejected,
        consumer_factory=lambda *_: raw,
    )

    assert await consumer.consume_one() is True
    async with factory() as session:
        record = await session.scalar(select(DeadLetterRecord))
        stored_run = await session.get(DigestRun, run.id)
        dlq = await session.scalar(select(OutboxEvent))
        assert record.work_type == "digest_run"
        assert record.error_code == "UnsupportedEventVersion"
        assert record.source_event_id == source_event_id
        assert stored_run.state == "failed"
        assert dlq.topic == "tpb.digest.run.requested.dlq.v1"
        assert record.dlq_outbox_event_id == dlq.event_id


@pytest.mark.asyncio
async def test_admin_dead_letter_api_requires_auth_csrf_and_never_returns_message_content(engine: AsyncEngine) -> None:
    now = datetime(2026, 8, 23, 18, 0, tzinfo=timezone.utc)
    factory, run, message = await _seed_run(engine, now, state="failed", with_message=True)
    outbox = OutboxRepository(max_event_bytes=65_536)
    async with factory() as session, session.begin():
        record, _ = await DeadLetterRepository(outbox).record_terminal(
            session,
            source_topic="tpb.telegram.delivery.requested.v1",
            source_event_id=uuid4(),
            source_partition=0,
            source_offset=7,
            work_type="digest_message",
            entity_ref=str(message.id),
            run_id=run.id,
            message_id=message.id,
            subscription_id=run.subscription_id,
            correlation_id=run.correlation_id,
            terminal_reason="permanent_failure",
            error_code="TelegramBadRequest",
            attempt_summary={"attempt_count": 1},
            generation=1,
            failed_at=now,
        )
    settings = AdminSettings(
        enabled=True,
        username="admin",
        password_hash=hash_password("stage5-password"),
        session_secret="stage5-session-secret",
        secure_cookies=False,
    )
    app = create_admin_app(settings, factory)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test", follow_redirects=False) as client:
        assert (await client.get("/admin/api/dead-letters")).status_code == 401
        await client.post("/admin/login", data={"username": "admin", "password": "stage5-password"})
        listing = await client.get("/admin/api/dead-letters?status=open&work_type=digest_message")
        detail = await client.get(f"/admin/api/dead-letters/{record.id}")
        assert (await client.post(f"/admin/api/dead-letters/{record.id}/replay")).status_code == 403
        csrf = listing.json()["csrf_token"]
        no_key = await client.post(
            f"/admin/api/dead-letters/{record.id}/replay",
            headers={"X-CSRF-Token": csrf},
        )
        assert no_key.status_code == 400
        headers = {"X-CSRF-Token": csrf, "Idempotency-Key": "admin-click-1"}
        first = await client.post(f"/admin/api/dead-letters/{record.id}/replay", headers=headers)
        second = await client.post(f"/admin/api/dead-letters/{record.id}/replay", headers=headers)

    assert listing.status_code == 200
    assert listing.json()["total"] == 1
    assert detail.status_code == 200
    assert first.status_code == 200
    assert first.json() == second.json()
    assert first.json()["result"] == "replayed"
    assert "TOP SECRET" not in listing.text
    assert "TOP SECRET" not in detail.text
    assert "text" not in detail.json()
    assert "payload" not in detail.json()


def test_replay_lock_compiles_for_postgresql() -> None:
    statement = dead_letter_lock_statement(uuid4())
    assert "FOR UPDATE" in str(statement.compile(dialect=postgresql.dialect())).upper()
