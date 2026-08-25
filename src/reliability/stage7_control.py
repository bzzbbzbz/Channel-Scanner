"""Content-free control plane for the isolated BL-22 stage-7 acceptance project."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse
from uuid import UUID, uuid4

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.config.settings import get_settings
from src.models.channel import Channel, ChannelStatus
from src.models.chat_message import ChatMessage
from src.models.digest_delivery import DigestDelivery
from src.models.digest_processing_log import DigestProcessingLog
from src.models.outbox_event import OutboxEvent
from src.models.post import Post
from src.models.reliable_digest import DigestOutboxMessage, DigestRun, InboxEvent
from src.models.subscription import Subscription, SubscriptionChannel
from src.models.user import DeliveryFrequency, DigestFormat, SummaryMode, User
from src.repository.outbox import OutboxRepository
from src.reliability.contracts import DIGEST_RUN_REQUESTED_TOPIC
from src.reliability.kafka_consumer import DELIVERY_CONSUMER_GROUP, DIGEST_CONSUMER_GROUP

_SENTINEL = Path("/run/bl22-stage7/isolated.guard")
_SENTINEL_CONTENT = "telegram-parser-bot BL-22 stage-7 isolated real Telegram E2E only\n"
_MIGRATION_HEAD = "0024_reliable_delete_cascades"


def _json(value: dict) -> None:
    print(json.dumps(value, sort_keys=True))


def _require_isolated_context(settings) -> UUID:
    try:
        run_id = UUID(os.environ["BL22_STAGE7_RUN_ID"])
    except (KeyError, ValueError) as exc:
        raise RuntimeError("BL22_STAGE7_RUN_ID must be a UUID") from exc
    if os.environ.get("BL22_STAGE7_E2E") != "1":
        raise RuntimeError("BL22_STAGE7_E2E capability is required")
    try:
        tester_id = int(os.environ["BL22_STAGE7_TESTER_ID"])
    except (KeyError, ValueError) as exc:
        raise RuntimeError("BL22_STAGE7_TESTER_ID must be a positive integer") from exc
    if tester_id <= 0:
        raise RuntimeError("BL22_STAGE7_TESTER_ID must be a positive integer")
    if os.environ.get("BL22_STAGE7_CHAT_TYPE") not in {"group", "supergroup"}:
        raise RuntimeError("BL22_STAGE7_CHAT_TYPE must be group or supergroup")
    parsed = urlparse(settings.database.url.replace("postgresql+asyncpg", "postgresql", 1))
    if parsed.hostname != "postgres" or parsed.path != "/telegram_bot":
        raise RuntimeError("stage-7 control requires isolated PostgreSQL")
    if settings.kafka.bootstrap_servers != "kafka:9092":
        raise RuntimeError("stage-7 control requires isolated Kafka")
    if settings.bot.api_base_url != "https://api.telegram.org":
        raise RuntimeError("stage-7 requires the default real Telegram Bot API")
    if not _SENTINEL.is_file() or _SENTINEL.read_text(encoding="ascii") != _SENTINEL_CONTENT:
        raise RuntimeError("stage-7 isolation sentinel is missing")
    return run_id


async def _migration(factory: async_sessionmaker) -> None:
    async with factory() as session:
        revision = await session.scalar(text("select version_num from alembic_version"))
    if revision != _MIGRATION_HEAD:
        raise AssertionError("clean migration chain did not reach revision 0024")
    _json({"migration_head": revision})


async def _seed(factory: async_sessionmaker, chat_id: int, chat_type: str, tester_id: int, run_id: UUID) -> None:
    now = datetime.now(timezone.utc)
    suffix = run_id.hex[:12]
    marker = f"BL22S7-{uuid4().hex}"
    async with factory() as session, session.begin():
        existing_users = int((await session.scalar(select(func.count()).select_from(User))) or 0)
        if existing_users:
            raise AssertionError("stage-7 isolated user table must be empty before identity mirroring")
        user = User(
            telegram_user_id=tester_id,
            chat_id=chat_id,
            chat_type=chat_type,
            username=None,
            first_name=None,
            timezone="UTC",
            language="en",
        )
        session.add(user)
        await session.flush()
        subscription = Subscription(
            user_id=user.id,
            name=f"BL22-stage7-{suffix}",
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
            telegram_id=7_000_000_000 + int(run_id.hex[:8], 16),
            username=f"bl22s7_{suffix}",
            name="BL-22 stage 7",
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
        session.add(
            Post(
                post_id=1,
                channel_id=channel.id,
                content=marker,
                datetime=now - timedelta(minutes=1),
            )
        )
        await session.flush()
        subscription_id = subscription.id
    _json({"subscription_id": subscription_id, "seeded_posts": 1, "routing_identity_mirrored": True})


async def _run_for_subscription(session, subscription_id: int) -> DigestRun | None:
    return await session.scalar(
        select(DigestRun)
        .where(DigestRun.subscription_id == subscription_id)
        .order_by(DigestRun.created_at.desc())
    )


async def _wait_completed(factory: async_sessionmaker, subscription_id: int, timeout: float) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    last_state = None
    while asyncio.get_running_loop().time() < deadline:
        async with factory() as session:
            run = await _run_for_subscription(session, subscription_id)
            last_state = run.state if run else None
            if run is not None and run.state == "completed":
                _json({"completed": True, "run_id": str(run.id), "state": run.state})
                return
        await asyncio.sleep(0.25)
    raise RuntimeError(f"timed out waiting for reliable completion; state={last_state}")


async def _snapshot(
    factory: async_sessionmaker,
    subscription_id: int,
    chat_id: int,
    chat_type: str,
    tester_id: int,
) -> None:
    async with factory() as session:
        runs = list((await session.scalars(select(DigestRun).where(DigestRun.subscription_id == subscription_id))).all())
        if len(runs) != 1:
            raise AssertionError("stage-7 subscription must have exactly one digest run")
        run = runs[0]
        messages = list(
            (await session.scalars(select(DigestOutboxMessage).where(DigestOutboxMessage.run_id == run.id))).all()
        )
        roots = list(
            (
                await session.scalars(
                    select(OutboxEvent).where(
                        OutboxEvent.aggregate_type == "digest_run",
                        OutboxEvent.aggregate_id == str(run.id),
                    )
                )
            ).all()
        )
        delivery_events = list(
            (
                await session.scalars(
                    select(OutboxEvent).where(
                        OutboxEvent.aggregate_type == "digest_message",
                        OutboxEvent.correlation_id == run.correlation_id,
                    )
                )
            ).all()
        )
        root = roots[0] if len(roots) == 1 else None
        message = messages[0] if len(messages) == 1 else None
        delivery_event = delivery_events[0] if len(delivery_events) == 1 else None
        posts = list(
            (
                await session.scalars(
                    select(Post)
                    .join(SubscriptionChannel, SubscriptionChannel.channel_id == Post.channel_id)
                    .where(SubscriptionChannel.subscription_id == subscription_id)
                )
            ).all()
        )
        root_inbox = (
            await session.scalar(
                select(InboxEvent).where(
                    InboxEvent.consumer_name == DIGEST_CONSUMER_GROUP,
                    InboxEvent.event_id == root.event_id,
                )
            )
            if root else None
        )
        delivery_inbox = (
            await session.scalar(
                select(InboxEvent).where(
                    InboxEvent.consumer_name == DELIVERY_CONSUMER_GROUP,
                    InboxEvent.event_id == delivery_event.event_id,
                )
            )
            if delivery_event else None
        )
        deliveries = list(
            (await session.scalars(select(DigestDelivery).where(DigestDelivery.digest_run_id == run.id))).all()
        )
        logs = list(
            (await session.scalars(select(DigestProcessingLog).where(DigestProcessingLog.digest_run_id == run.id))).all()
        )
        digest_chat_rows = list(
            (await session.scalars(select(ChatMessage).where(ChatMessage.role == "digest"))).all()
        )
        subscription = await session.get(Subscription, subscription_id)
        mirrored_users = list((await session.scalars(select(User))).all())
        routing_identity_matches = (
            len(mirrored_users) == 1
            and mirrored_users[0].telegram_user_id == tester_id
            and mirrored_users[0].chat_id == chat_id
            and mirrored_users[0].chat_type == chat_type
        )
        marker_present = bool(message and len(posts) == 1 and posts[0].content in message.text)
        _json(
            {
                "run_id": str(run.id),
                "correlation_id": str(run.correlation_id),
                "root_event_id": str(root.event_id) if root else None,
                "run_count": len(runs),
                "run_state": run.state,
                "root_event_count": len(roots),
                "root_state": root.state if root else None,
                "root_offset": root.published_offset if root else None,
                "root_inbox_count": int(root_inbox is not None),
                "root_inbox_state": root_inbox.state if root_inbox else None,
                "root_inbox_attempts": root_inbox.processing_attempt_count if root_inbox else None,
                "message_count": len(messages),
                "message_states": [item.state for item in messages],
                "message_attempts": [item.attempt_count for item in messages],
                "telegram_message_id_set": bool(message and message.telegram_message_id),
                "distinct_telegram_message_ids": len({item.telegram_message_id for item in messages if item.telegram_message_id}),
                "marker_in_persisted_part": marker_present,
                "delivery_event_count": len(delivery_events),
                "delivery_event_state": delivery_event.state if delivery_event else None,
                "delivery_inbox_count": int(delivery_inbox is not None),
                "delivery_inbox_state": delivery_inbox.state if delivery_inbox else None,
                "post_delivery_count": len(deliveries),
                "post_delivery_states": [item.status for item in deliveries],
                "processing_log_count": len(logs),
                "processing_totals": [
                    [item.found_count, item.filtered_count, item.included_count] for item in logs
                ],
                "digest_chat_record_count": len(digest_chat_rows),
                "last_digest_at_set": bool(subscription and subscription.last_digest_at),
                "routing_identity_matches": routing_identity_matches,
                "isolated_user_count": len(mirrored_users),
            }
        )


async def _republish(factory: async_sessionmaker, subscription_id: int, settings) -> None:
    async with factory() as session:
        run = await _run_for_subscription(session, subscription_id)
        if run is None:
            raise RuntimeError("run not found")
        root = await session.scalar(
            select(OutboxEvent).where(
                OutboxEvent.aggregate_type == "digest_run",
                OutboxEvent.aggregate_id == str(run.id),
            )
        )
        if root is None or root.state != "published":
            raise RuntimeError("published root event not found")
        envelope = OutboxRepository(max_event_bytes=settings.kafka.max_event_bytes).serialize(root)
        event_id = str(root.event_id)
        event_key = root.event_key
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka.bootstrap_servers,
        security_protocol=settings.kafka.security_protocol,
        client_id="bl22-stage7-exact-republish-control",
        enable_idempotence=True,
        acks="all",
    )
    await producer.start()
    try:
        metadata = await producer.send_and_wait(
            DIGEST_RUN_REQUESTED_TOPIC,
            value=envelope,
            key=event_key.encode("utf-8"),
        )
    finally:
        await producer.stop()
    _json({"event_id": event_id, "partition": int(metadata.partition), "offset": int(metadata.offset)})


async def _audit_root(factory: async_sessionmaker, subscription_id: int, settings, timeout: float) -> None:
    async with factory() as session:
        run = await _run_for_subscription(session, subscription_id)
        if run is None:
            raise RuntimeError("run not found")
        root = await session.scalar(
            select(OutboxEvent).where(
                OutboxEvent.aggregate_type == "digest_run",
                OutboxEvent.aggregate_id == str(run.id),
            )
        )
        if root is None or root.published_partition is None:
            raise RuntimeError("published root event not found")
        expected_value = OutboxRepository(max_event_bytes=settings.kafka.max_event_bytes).serialize(root)
        expected_key = root.event_key.encode("utf-8")
        event_id = str(root.event_id)
        expected_partition = root.published_partition

    consumer = AIOKafkaConsumer(
        DIGEST_RUN_REQUESTED_TOPIC,
        bootstrap_servers=settings.kafka.bootstrap_servers,
        security_protocol=settings.kafka.security_protocol,
        group_id=f"bl22-stage7-root-audit-{uuid4()}",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    matches: list[dict] = []
    await consumer.start()
    try:
        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            batches = await consumer.getmany(timeout_ms=500)
            for records in batches.values():
                for record in records:
                    try:
                        envelope = json.loads(record.value)
                    except (TypeError, json.JSONDecodeError):
                        continue
                    if envelope.get("event_id") != event_id:
                        continue
                    matches.append(
                        {
                            "partition": record.partition,
                            "offset": record.offset,
                            "key_matches": record.key == expected_key,
                            "value_matches": record.value == expected_value,
                            "sha256": hashlib.sha256(record.value).hexdigest(),
                        }
                    )
    finally:
        await consumer.stop()

    if len(matches) != 2:
        raise AssertionError(f"expected exactly two root Kafka records, observed {len(matches)}")
    if any(not item["key_matches"] or not item["value_matches"] for item in matches):
        raise AssertionError("root Kafka key or envelope differs from the persisted original")
    if {item["partition"] for item in matches} != {expected_partition}:
        raise AssertionError("root Kafka records were not published to the same partition")
    if len({item["sha256"] for item in matches}) != 1:
        raise AssertionError("root Kafka record bytes differ")
    _json(
        {
            "event_id": event_id,
            "count": 2,
            "partition": expected_partition,
            "offsets": sorted(item["offset"] for item in matches),
            "key_matches": True,
            "bytes_identical": True,
            "envelopes_match_db": True,
            "sha256": matches[0]["sha256"],
        }
    )


async def _main(args: argparse.Namespace) -> None:
    settings = get_settings()
    run_id = _require_isolated_context(settings)
    chat_id = int(os.environ["E2E_CHAT_ID"])
    chat_type = os.environ["BL22_STAGE7_CHAT_TYPE"]
    tester_id = int(os.environ["BL22_STAGE7_TESTER_ID"])
    engine = create_async_engine(settings.database.url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        if args.command == "migration":
            await _migration(factory)
        elif args.command == "seed":
            await _seed(factory, chat_id, chat_type, tester_id, run_id)
        elif args.command == "wait-completed":
            await _wait_completed(factory, args.subscription_id, args.timeout)
        elif args.command == "snapshot":
            await _snapshot(factory, args.subscription_id, chat_id, chat_type, tester_id)
        elif args.command == "republish-root":
            await _republish(factory, args.subscription_id, settings)
        elif args.command == "audit-root":
            await _audit_root(factory, args.subscription_id, settings, args.timeout)
    finally:
        await engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser(description="BL-22 stage-7 isolated control")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("migration")
    subparsers.add_parser("seed")
    completed = subparsers.add_parser("wait-completed")
    completed.add_argument("subscription_id", type=int)
    completed.add_argument("--timeout", type=float, default=90)
    snapshot = subparsers.add_parser("snapshot")
    snapshot.add_argument("subscription_id", type=int)
    republish = subparsers.add_parser("republish-root")
    republish.add_argument("subscription_id", type=int)
    audit = subparsers.add_parser("audit-root")
    audit.add_argument("subscription_id", type=int)
    audit.add_argument("--timeout", type=float, default=5)
    asyncio.run(_main(parser.parse_args()))


if __name__ == "__main__":
    main()
