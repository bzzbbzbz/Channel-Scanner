"""BL-22 shadow roles: stage-2 outbox relay and stage-1 readiness roles."""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import socket
import sys
from uuid import uuid4

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.config.settings import Settings
from src.reliability.cli import run_checks
from src.reliability.heartbeat import RoleHeartbeatReporter
from src.reliability.readiness import RoleReadiness
from src.reliability.e2e_faults import isolated_e2e_context

logger = logging.getLogger(__name__)


async def run_bootstrap_role(role: str, settings: Settings) -> None:
    """Run the selected isolated role after common dependency checks."""
    readiness = RoleReadiness(role)
    log_level = getattr(logging, settings.logging.level.upper(), logging.INFO)
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )
    if not settings.kafka.enabled:
        raise RuntimeError(f"BL-22 role {role} requires KAFKA_ENABLED=1")
    e2e_context = isolated_e2e_context(settings)

    heartbeat_engine = create_async_engine(
        settings.database.url,
        pool_size=settings.database.pool_size,
        pool_recycle=settings.database.pool_recycle,
    )
    heartbeat = RoleHeartbeatReporter(
        role,
        async_sessionmaker(heartbeat_engine, expire_on_commit=False),
    )
    await heartbeat.start()

    try:
        result = await run_checks(settings, database=True, kafka=True, topics=True)
        if not result["ok"]:
            logger.error("BL-22 role dependency check failed: role=%s status=%s", role, result)
            raise RuntimeError(f"BL-22 role {role} is not ready")

        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop_event.set)
            except NotImplementedError:
                pass
        if role == "outbox-relay":
            await _run_outbox_relay(
                settings,
                stop_event,
                e2e_context=e2e_context,
                readiness=readiness,
                heartbeat=heartbeat,
            )
            logger.info("BL-22 stage-2 outbox relay stopped")
        elif role in {"scheduler", "digest-worker", "telegram-delivery-worker"} and settings.reliable_delivery.enabled:
            await _run_reliable_role(
                role,
                settings,
                stop_event,
                e2e_context=e2e_context,
                readiness=readiness,
                heartbeat=heartbeat,
            )
        else:
            readiness.mark_ready()
            await heartbeat.ready()
            logger.info(
                "BL-22 stage-1 bootstrap role ready: role=%s postgres=ok kafka=ok topics=ok business_processing=disabled",
                role,
            )
            await stop_event.wait()
            logger.info("BL-22 stage-1 bootstrap role stopped: role=%s", role)
    except asyncio.CancelledError:
        await heartbeat.stopped()
        raise
    except Exception as exc:
        await heartbeat.failed(type(exc).__name__)
        raise
    else:
        await heartbeat.stopped()
    finally:
        readiness.clear()
        await heartbeat.close()
        await heartbeat_engine.dispose()


async def _run_reliable_role(
    role: str,
    settings: Settings,
    stop_event: asyncio.Event,
    *,
    e2e_context=None,
    readiness: RoleReadiness,
    heartbeat: RoleHeartbeatReporter,
) -> None:
    from src.repository.outbox import OutboxRepository

    engine = create_async_engine(
        settings.database.url,
        pool_size=settings.database.pool_size,
        pool_recycle=settings.database.pool_recycle,
    )
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    outbox = OutboxRepository(max_event_bytes=settings.kafka.max_event_bytes)
    owner = f"{socket.gethostname()}:{os.getpid()}:{uuid4().hex[:12]}"[-128:]
    try:
        if role == "scheduler":
            from src.reliability.scheduler import ReliableDigestScheduler

            readiness.mark_ready()
            await heartbeat.ready()
            logger.info("BL-22 reliable scheduler active: telegram_token_required=false")
            await ReliableDigestScheduler(session_factory, outbox, settings.reliable_delivery).run(stop_event)
            return

        if role == "telegram-delivery-worker":
            from src.reliability.kafka_consumer import KafkaDeliveryConsumer
            from src.reliability.telegram_delivery_worker import TelegramDeliveryWorker
            from src.reliability.telegram_sender import AiogramReliableTelegramSender

            sender = AiogramReliableTelegramSender(
                settings.bot.token,
                api_base_url=settings.bot.api_base_url,
                allow_isolated_e2e=e2e_context is not None,
            )
            worker = TelegramDeliveryWorker(
                session_factory,
                sender,
                settings.reliable_delivery,
                owner=owner,
                outbox=outbox,
            )
            consumer = KafkaDeliveryConsumer(
                settings.kafka,
                settings.reliable_delivery,
                worker.handle,
                rejection_handler=worker.handle_rejected,
            )
            await consumer.start()
            readiness.mark_ready()
            await heartbeat.ready()
            logger.info("BL-22 stage-4 Telegram delivery worker active")
            try:
                await consumer.run(stop_event)
            finally:
                await consumer.stop()
                await sender.close()
            return

        from src.reliability.digest_worker import DigestWorker
        from src.reliability.kafka_consumer import KafkaDigestConsumer

        worker = DigestWorker(
            session_factory,
            outbox,
            settings.reliable_delivery,
            settings.llm,
            owner=owner,
        )
        consumer = KafkaDigestConsumer(
            settings.kafka,
            settings.reliable_delivery,
            worker.handle,
            rejection_handler=worker.handle_rejected,
            after_database_commit=(
                (lambda: e2e_context.crash_once("digest_after_db_before_offset", 87))
                if e2e_context is not None else None
            ),
        )
        await consumer.start()
        readiness.mark_ready()
        await heartbeat.ready()
        logger.info("BL-22 digest worker active: memory=false telegram_sends=false")
        try:
            await consumer.run(stop_event)
        finally:
            await consumer.stop()
    finally:
        await engine.dispose()


async def _run_outbox_relay(
    settings: Settings,
    stop_event: asyncio.Event,
    *,
    e2e_context=None,
    readiness: RoleReadiness,
    heartbeat: RoleHeartbeatReporter,
) -> None:
    from src.reliability.kafka_producer import KafkaEventProducer
    from src.reliability.outbox_relay import OutboxRelay
    from src.repository.outbox import OutboxRepository

    engine = create_async_engine(
        settings.database.url,
        pool_size=settings.database.pool_size,
        pool_recycle=settings.database.pool_recycle,
    )
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    repository = OutboxRepository(max_event_bytes=settings.kafka.max_event_bytes)
    producer = KafkaEventProducer(settings.kafka)
    owner = f"{socket.gethostname()}:{os.getpid()}:{uuid4().hex[:12]}"[-128:]
    relay = OutboxRelay(
        session_factory,
        repository,
        producer,
        settings.kafka,
        owner=owner,
        after_broker_ack=(
            (lambda: e2e_context.crash_once("relay_after_ack_before_db", 86))
            if e2e_context is not None else None
        ),
        **({"random_uniform": lambda _low, high: high} if e2e_context is not None else {}),
    )
    await producer.start()
    readiness.mark_ready()
    await heartbeat.ready()
    logger.info("BL-22 stage-2 outbox relay ready: postgres=ok kafka=ok topics=ok domain_producers=disabled")
    try:
        await relay.run(stop_event)
    finally:
        await producer.stop()
        await engine.dispose()
