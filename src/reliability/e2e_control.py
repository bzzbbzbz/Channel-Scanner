"""Isolated PostgreSQL/Kafka control and assertions for BL-22 stage 6.

This module is packaged in the application image so the host harness does not
need database or Kafka ports. It never prints post or rendered message content.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import httpx
from aiokafka import AIOKafkaConsumer
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.config.settings import get_settings
from src.models.channel import Channel, ChannelStatus
from src.models.dead_letter import DeadLetterRecord, DeadLetterReplay
from src.models.digest_delivery import DigestDelivery
from src.models.digest_processing_log import DigestProcessingLog
from src.models.outbox_event import OutboxEvent
from src.models.post import Post
from src.models.reliable_digest import DigestOutboxMessage, DigestRun, InboxEvent
from src.models.subscription import Subscription, SubscriptionChannel
from src.models.user import DeliveryFrequency, DigestFormat, SummaryMode, User
from src.repository.outbox import OutboxRepository
from src.repository.digest_delivery import DigestDeliveryRepository
from src.reliability.contracts import (
    DIGEST_RUN_REQUESTED_DLQ_TOPIC,
    DIGEST_RUN_REQUESTED_TOPIC,
    TELEGRAM_DELIVERY_REQUESTED_DLQ_TOPIC,
    TELEGRAM_DELIVERY_REQUESTED_TOPIC,
    validate_event,
)
from src.reliability.kafka_consumer import DIGEST_CONSUMER_GROUP
from src.reliability.e2e_faults import isolated_e2e_context

_PRIVATE_MARKER = "BL22_STAGE6_PRIVATE_CONTENT"


def _json(value: dict) -> None:
    print(json.dumps(value, sort_keys=True))


def _fake_status(value: str) -> int | str:
    return value if value == "accept_timeout" else int(value)


async def _seed(factory: async_sessionmaker, name: str, posts: int) -> None:
    now = datetime.now(timezone.utc)
    suffix = uuid4().hex[:10]
    async with factory() as session, session.begin():
        user = User(
            telegram_user_id=8_000_000_000 + int(suffix[:7], 16),
            chat_id=-8_000_000_000 - int(suffix[:7], 16),
            chat_type="private",
            timezone="UTC",
            language="en",
        )
        session.add(user)
        await session.flush()
        subscription = Subscription(
            user_id=user.id,
            name=f"BL22-{name}-{suffix}",
            digest_format=DigestFormat.SHORT,
            summary_mode=SummaryMode.BRIEF,
            frequency=DeliveryFrequency.HOURLY,
            notification_cron="0 * * * *",
            enabled=True,
            created_at=now - timedelta(hours=2),
        )
        session.add(subscription)
        await session.flush()
        channel = Channel(
            telegram_id=9_000_000_000 + int(suffix[:7], 16),
            username=f"bl22_{suffix}",
            name="BL22 stage 6",
            status=ChannelStatus.ACTIVE,
        )
        session.add(channel)
        await session.flush()
        session.add(
            SubscriptionChannel(
                subscription_id=subscription.id,
                channel_id=channel.id,
                subscribed_at=now - timedelta(days=1),
            )
        )
        for index in range(posts):
            session.add(
                Post(
                    post_id=index + 1,
                    channel_id=channel.id,
                    content=f"{_PRIVATE_MARKER} {name} {index} " + ("x" * 240),
                    datetime=now - timedelta(minutes=max(1, posts - index)),
                )
            )
        await session.flush()
        subscription_id = subscription.id
    async with factory() as session:
        pending = len(await DigestDeliveryRepository(session).get_pending_posts_for_subscription(subscription_id))
    if pending != posts:
        raise AssertionError(f"isolated seed expected {posts} pending posts, found {pending}")
    _json({"subscription_id": subscription_id, "posts": posts, "pending": pending})


async def _run_for_subscription(session, subscription_id: int) -> DigestRun | None:
    return await session.scalar(
        select(DigestRun)
        .where(DigestRun.subscription_id == subscription_id)
        .order_by(DigestRun.created_at.desc())
    )


class _FailOnSendLegacySender:
    def __init__(self) -> None:
        self.calls = 0

    async def send_message(self, chat_id: int, text: str, parse_mode: str | None = None) -> None:
        self.calls += 1
        raise AssertionError("reliable-owned subscription reached the legacy sender")

    async def close(self) -> None:
        return None


async def _legacy_cycle(factory: async_sessionmaker, subscription_id: int, settings) -> None:
    from src.digest.service import is_digest_due
    from src.scheduler.digest_job import digest_delivery_job

    now = datetime.now(timezone.utc)
    async with factory() as session:
        subscription = await session.get(Subscription, subscription_id)
        if subscription is None:
            raise RuntimeError("subscription not found")
        await session.refresh(subscription, attribute_names=["user"])
        if subscription.user is None:
            raise RuntimeError("subscription user not found")
        due = is_digest_due(subscription, subscription.user, now)
        pending = len(await DigestDeliveryRepository(session).get_pending_posts_for_subscription(subscription_id))
    sender = _FailOnSendLegacySender()
    delivered = await digest_delivery_job(
        factory,
        "isolated-legacy-token",
        settings.llm,
        memory_service=None,
        reliable_delivery=settings.reliable_delivery,
        sender=sender,
        now=now,
    )
    async with factory() as session:
        run_count = int(
            (await session.scalar(select(func.count()).select_from(DigestRun).where(DigestRun.subscription_id == subscription_id)))
            or 0
        )
        legacy_delivery_count = int(
            (
                await session.scalar(
                    select(func.count())
                    .select_from(DigestDelivery)
                    .where(
                        DigestDelivery.subscription_id == subscription_id,
                        DigestDelivery.digest_run_id.is_(None),
                    )
                )
            )
            or 0
        )
    _json(
        {
            "subscription_id": subscription_id,
            "due": due,
            "pending": pending,
            "legacy_delivered": delivered,
            "legacy_sender_calls": sender.calls,
            "legacy_delivery_count": legacy_delivery_count,
            "reliable_run_count": run_count,
        }
    )


async def _wait(factory: async_sessionmaker, subscription_id: int, condition: str, timeout: float) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    last: dict = {}
    while asyncio.get_running_loop().time() < deadline:
        async with factory() as session:
            run = await _run_for_subscription(session, subscription_id)
            if run is not None:
                messages = list(
                    (
                        await session.scalars(
                            select(DigestOutboxMessage)
                            .where(DigestOutboxMessage.run_id == run.id)
                            .order_by(DigestOutboxMessage.ordinal)
                        )
                    ).all()
                )
                root = await session.scalar(
                    select(OutboxEvent).where(
                        OutboxEvent.aggregate_type == "digest_run",
                        OutboxEvent.aggregate_id == str(run.id),
                    )
                )
                dead_letter = await session.scalar(
                    select(DeadLetterRecord)
                    .where(DeadLetterRecord.run_id == run.id)
                    .order_by(DeadLetterRecord.created_at.desc())
                )
                dlq = await session.get(OutboxEvent, dead_letter.dlq_outbox_event_id) if dead_letter and dead_letter.dlq_outbox_event_id else None
                last = {
                    "run_state": run.state,
                    "message_states": [message.state for message in messages],
                    "message_attempts": [message.attempt_count for message in messages],
                    "root_state": root.state if root else None,
                    "root_publication_attempts": root.publication_attempt_count if root else None,
                    "dead_letter": str(dead_letter.id) if dead_letter else None,
                    "dlq_state": dlq.state if dlq else None,
                }
                matched = {
                    "root-published": root is not None and root.state == "published",
                    "outbox-failed": root is not None
                    and root.state == "pending"
                    and root.publication_attempt_count >= 1
                    and root.last_error is not None,
                    "completed": run.state == "completed",
                    "three-parts": len(messages) == 3 and run.state in {"delivering", "completed"},
                    "partial": [message.state for message in messages].count("sent") == 2
                    and [message.state for message in messages].count("retry_wait") == 1,
                    "retrying": [message.state for message in messages].count("retry_wait") == 1,
                    "ambiguous-retry": any(
                        message.state == "retry_wait" and message.ambiguous_send for message in messages
                    ),
                    "terminal": dead_letter is not None and dlq is not None and dlq.state == "published",
                    "lease-recovered": run.state == "completed" and run.render_attempt_count == 2,
                }.get(condition, False)
                if matched:
                    last["subscription_id"] = subscription_id
                    if dead_letter is not None:
                        last["dead_letter_id"] = str(dead_letter.id)
                    _json(last)
                    return
        await asyncio.sleep(0.2)
    raise RuntimeError(f"Timed out waiting for {condition}: {last}")


async def _snapshot(factory: async_sessionmaker, subscription_id: int) -> None:
    async with factory() as session:
        run = await _run_for_subscription(session, subscription_id)
        if run is None:
            raise RuntimeError("run not found")
        messages = list(
            (
                await session.scalars(
                    select(DigestOutboxMessage)
                    .where(DigestOutboxMessage.run_id == run.id)
                    .order_by(DigestOutboxMessage.ordinal)
                )
            ).all()
        )
        root = await session.scalar(
            select(OutboxEvent).where(OutboxEvent.aggregate_type == "digest_run", OutboxEvent.aggregate_id == str(run.id))
        )
        inbox = await session.scalar(
            select(InboxEvent).where(InboxEvent.consumer_name == DIGEST_CONSUMER_GROUP, InboxEvent.event_id == root.event_id)
        )
        subscription = await session.get(Subscription, subscription_id)
        run_count = await session.scalar(
            select(func.count()).select_from(DigestRun).where(DigestRun.subscription_id == subscription_id)
        )
        root_count = await session.scalar(
            select(func.count())
            .select_from(OutboxEvent)
            .where(OutboxEvent.aggregate_type == "digest_run", OutboxEvent.aggregate_id == str(run.id))
        )
        _json(
            {
                "run_id": str(run.id),
                "correlation_id": str(run.correlation_id),
                "run_state": run.state,
                "render_attempts": run.render_attempt_count,
                "root_event_id": str(root.event_id),
                "root_state": root.state,
                "root_partition": root.published_partition,
                "root_offset": root.published_offset,
                "root_publication_attempts": root.publication_attempt_count,
                "root_sha256": hashlib.sha256(OutboxRepository(max_event_bytes=65_536).serialize(root)).hexdigest(),
                "inbox_processing_attempts": inbox.processing_attempt_count if inbox else 0,
                "message_count": len(messages),
                "message_ids": [str(message.id) for message in messages],
                "message_states": [message.state for message in messages],
                "message_attempts": [message.attempt_count for message in messages],
                "message_ambiguous": [message.ambiguous_send for message in messages],
                "telegram_message_ids": [message.telegram_message_id for message in messages],
                "run_count": run_count,
                "root_event_count": root_count,
                "delivery_count": await session.scalar(
                    select(func.count()).select_from(DigestDelivery).where(DigestDelivery.digest_run_id == run.id)
                ),
                "processing_log_count": await session.scalar(
                    select(func.count()).select_from(DigestProcessingLog).where(DigestProcessingLog.digest_run_id == run.id)
                ),
                "legacy_delivery_count": await session.scalar(
                    select(func.count())
                    .select_from(DigestDelivery)
                    .where(
                        DigestDelivery.subscription_id == subscription_id,
                        DigestDelivery.digest_run_id.is_(None),
                    )
                ),
                "last_digest_at_set": subscription.last_digest_at is not None,
            }
        )


async def _inject_expired_render_lease(factory: async_sessionmaker, subscription_id: int) -> None:
    now = datetime.now(timezone.utc)
    async with factory() as session, session.begin():
        run = await _run_for_subscription(session, subscription_id)
        root = await session.scalar(
            select(OutboxEvent).where(OutboxEvent.aggregate_type == "digest_run", OutboxEvent.aggregate_id == str(run.id))
        )
        run.state = "rendering"
        run.lease_owner = "dead-e2e-renderer"
        run.lease_until = now - timedelta(seconds=1)
        run.render_attempt_count = 1
        run.updated_at = now - timedelta(seconds=2)
        session.add(
            InboxEvent(
                consumer_name=DIGEST_CONSUMER_GROUP,
                event_id=root.event_id,
                attempt=root.attempt,
                generation=root.generation,
                state="processing",
                lease_owner="dead-e2e-renderer",
                lease_until=now - timedelta(seconds=1),
                processing_attempt_count=1,
                created_at=now - timedelta(seconds=2),
                updated_at=now - timedelta(seconds=2),
            )
        )
    _json({"run_id": str(run.id), "event_id": str(root.event_id)})


async def _expedite_retry(factory: async_sessionmaker, subscription_id: int) -> None:
    now = datetime.now(timezone.utc)
    async with factory() as session, session.begin():
        run = await _run_for_subscription(session, subscription_id)
        message = await session.scalar(
            select(DigestOutboxMessage).where(
                DigestOutboxMessage.run_id == run.id,
                DigestOutboxMessage.state == "retry_wait",
            )
        )
        if message is None:
            raise RuntimeError("retry_wait message not found")
        message.next_attempt_at = now
        await session.execute(
            update(OutboxEvent)
            .where(
                OutboxEvent.aggregate_id == str(message.id),
                OutboxEvent.attempt == message.attempt_count + 1,
                OutboxEvent.state == "pending",
            )
            .values(next_attempt_at=now)
        )
    _json({"message_id": str(message.id), "attempt": message.attempt_count + 1})


async def _expedite_root_retry(factory: async_sessionmaker, subscription_id: int) -> None:
    now = datetime.now(timezone.utc)
    async with factory() as session, session.begin():
        run = await _run_for_subscription(session, subscription_id)
        root = await session.scalar(
            select(OutboxEvent).where(
                OutboxEvent.aggregate_type == "digest_run",
                OutboxEvent.aggregate_id == str(run.id),
            )
        )
        if (
            root is None
            or root.state != "pending"
            or root.publication_attempt_count < 1
            or root.last_error is None
        ):
            raise RuntimeError("failed pending root event not found")
        root.next_attempt_at = now
    _json({"event_id": str(root.event_id), "publication_attempts": root.publication_attempt_count})


async def _fake(plan: list[int | str] | None) -> None:
    async with httpx.AsyncClient(base_url="http://fake-telegram:8081", timeout=5) as client:
        if plan is not None:
            response = await client.post("/control/reset", json={"plan": plan})
        else:
            response = await client.get("/control/state")
        response.raise_for_status()
        _json(response.json())


async def _metrics(factory: async_sessionmaker) -> None:
    from src.admin.service import AdminDashboardService

    _json(await AdminDashboardService(factory).reliability_metrics())


async def _audit_kafka(factory: async_sessionmaker, dead_letter_id: UUID, timeout: float) -> None:
    settings = get_settings()
    topics = (
        DIGEST_RUN_REQUESTED_TOPIC,
        TELEGRAM_DELIVERY_REQUESTED_TOPIC,
        DIGEST_RUN_REQUESTED_DLQ_TOPIC,
        TELEGRAM_DELIVERY_REQUESTED_DLQ_TOPIC,
    )
    consumer = AIOKafkaConsumer(
        *topics,
        bootstrap_servers=settings.kafka.bootstrap_servers,
        security_protocol=settings.kafka.security_protocol,
        group_id=f"bl22-stage6-audit-{uuid4()}",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    found = None
    seen = 0
    await consumer.start()
    try:
        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline and found is None:
            batches = await consumer.getmany(timeout_ms=500)
            for records in batches.values():
                for record in records:
                    if _PRIVATE_MARKER.encode() in record.value:
                        raise AssertionError("private content marker found in Kafka")
                    event = json.loads(record.value)
                    validate_event(event)
                    seen += 1
                    if str(event.get("payload", {}).get("dead_letter_id")) == str(dead_letter_id):
                        found = {
                            "event_id": event["event_id"],
                            "topic": record.topic,
                            "partition": record.partition,
                            "offset": record.offset,
                        }
        if found is None:
            raise RuntimeError("matching Kafka DLQ event was not observed")
    finally:
        await consumer.stop()

    async with factory() as session:
        record = await session.get(DeadLetterRecord, dead_letter_id)
        if record is None or _PRIVATE_MARKER in json.dumps(
            {
                "entity_ref": record.entity_ref,
                "terminal_reason": record.terminal_reason,
                "error_code": record.error_code,
                "attempt_summary": record.attempt_summary,
            }
        ):
            raise AssertionError("dead-letter metadata is missing or contains private content")
    _json({"seen_valid_events": seen, "dlq": found})


async def _audit_root(factory: async_sessionmaker, subscription_id: int, expected_count: int, timeout: float) -> None:
    settings = get_settings()
    async with factory() as session:
        run = await _run_for_subscription(session, subscription_id)
        root = await session.scalar(
            select(OutboxEvent).where(
                OutboxEvent.aggregate_type == "digest_run",
                OutboxEvent.aggregate_id == str(run.id),
            )
        )
        event_id = str(root.event_id)
    consumer = AIOKafkaConsumer(
        DIGEST_RUN_REQUESTED_TOPIC,
        bootstrap_servers=settings.kafka.bootstrap_servers,
        security_protocol=settings.kafka.security_protocol,
        group_id=f"bl22-stage6-root-audit-{uuid4()}",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    matches = []
    await consumer.start()
    try:
        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline and len(matches) < expected_count:
            batches = await consumer.getmany(timeout_ms=500)
            for records in batches.values():
                for record in records:
                    if _PRIVATE_MARKER.encode() in record.value:
                        raise AssertionError("private content marker found in Kafka")
                    event = json.loads(record.value)
                    validate_event(event)
                    if event["event_id"] == event_id:
                        matches.append(
                            {
                                "offset": record.offset,
                                "sha256": hashlib.sha256(record.value).hexdigest(),
                            }
                        )
        if len(matches) != expected_count:
            raise RuntimeError(f"expected {expected_count} root publications, observed {len(matches)}")
        if len({item["sha256"] for item in matches}) != 1:
            raise AssertionError("republished root event bytes changed")
    finally:
        await consumer.stop()
    _json({"event_id": event_id, "count": len(matches), "publications": matches})


async def _assert_replay(factory: async_sessionmaker, dead_letter_id: UUID) -> None:
    async with factory() as session:
        record = await session.get(DeadLetterRecord, dead_letter_id)
        replays = list(
            (
                await session.scalars(
                    select(DeadLetterReplay).where(DeadLetterReplay.dead_letter_id == dead_letter_id)
                )
            ).all()
        )
        if record is None or record.status != "replayed" or len(replays) != 1:
            raise AssertionError("admin replay was not singular and durable")
        replay = replays[0]
        if replay.result != "replayed" or replay.generation != record.generation + 1:
            raise AssertionError("admin replay generation is invalid")
        outbox = await session.get(OutboxEvent, replay.outbox_event_id)
        if outbox is None:
            raise AssertionError("admin replay outbox event is missing")
        _json(
            {
                "dead_letter_id": str(record.id),
                "record_status": record.status,
                "replay_id": str(replay.id),
                "generation": replay.generation,
                "outbox_event_id": str(outbox.event_id),
                "outbox_state": outbox.state,
            }
        )


async def _main(args: argparse.Namespace) -> None:
    settings = get_settings()
    if isolated_e2e_context(settings) is None:
        raise RuntimeError("BL-22 control commands require isolated E2E capability")
    engine = create_async_engine(settings.database.url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        if args.command == "seed":
            await _seed(factory, args.name, args.posts)
        elif args.command == "legacy-cycle":
            await _legacy_cycle(factory, args.subscription_id, settings)
        elif args.command == "wait":
            await _wait(factory, args.subscription_id, args.condition, args.timeout)
        elif args.command == "snapshot":
            await _snapshot(factory, args.subscription_id)
        elif args.command == "inject-expired-render-lease":
            await _inject_expired_render_lease(factory, args.subscription_id)
        elif args.command == "expedite-retry":
            await _expedite_retry(factory, args.subscription_id)
        elif args.command == "expedite-root-retry":
            await _expedite_root_retry(factory, args.subscription_id)
        elif args.command == "fake-plan":
            await _fake(args.status)
        elif args.command == "fake-state":
            await _fake(None)
        elif args.command == "metrics":
            await _metrics(factory)
        elif args.command == "audit-kafka":
            await _audit_kafka(factory, args.dead_letter_id, args.timeout)
        elif args.command == "audit-root":
            await _audit_root(factory, args.subscription_id, args.expected_count, args.timeout)
        elif args.command == "assert-replay":
            await _assert_replay(factory, args.dead_letter_id)
    finally:
        # A stale transient asyncpg connection must not keep a completed
        # one-shot control container alive past the harness timeout.
        try:
            await asyncio.wait_for(engine.dispose(), timeout=2)
        except TimeoutError:
            pass


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="BL-22 isolated stage-6 E2E control")
    subparsers = parser.add_subparsers(dest="command", required=True)
    seed = subparsers.add_parser("seed")
    seed.add_argument("name")
    seed.add_argument("--posts", type=int, required=True)
    legacy = subparsers.add_parser("legacy-cycle")
    legacy.add_argument("subscription_id", type=int)
    wait = subparsers.add_parser("wait")
    wait.add_argument("subscription_id", type=int)
    wait.add_argument(
        "condition",
        choices=(
            "root-published",
            "outbox-failed",
            "completed",
            "three-parts",
            "partial",
            "retrying",
            "ambiguous-retry",
            "terminal",
            "lease-recovered",
        ),
    )
    wait.add_argument("--timeout", type=float, default=30)
    snapshot = subparsers.add_parser("snapshot")
    snapshot.add_argument("subscription_id", type=int)
    inject = subparsers.add_parser("inject-expired-render-lease")
    inject.add_argument("subscription_id", type=int)
    expedite = subparsers.add_parser("expedite-retry")
    expedite.add_argument("subscription_id", type=int)
    expedite_root = subparsers.add_parser("expedite-root-retry")
    expedite_root.add_argument("subscription_id", type=int)
    fake_plan = subparsers.add_parser("fake-plan")
    fake_plan.add_argument("status", type=_fake_status, nargs="*")
    subparsers.add_parser("fake-state")
    subparsers.add_parser("metrics")
    audit = subparsers.add_parser("audit-kafka")
    audit.add_argument("dead_letter_id", type=UUID)
    audit.add_argument("--timeout", type=float, default=20)
    root_audit = subparsers.add_parser("audit-root")
    root_audit.add_argument("subscription_id", type=int)
    root_audit.add_argument("--expected-count", type=int, required=True)
    root_audit.add_argument("--timeout", type=float, default=20)
    replay = subparsers.add_parser("assert-replay")
    replay.add_argument("dead_letter_id", type=UUID)
    return parser


def main() -> None:
    asyncio.run(_main(_parser().parse_args()))


if __name__ == "__main__":
    main()
