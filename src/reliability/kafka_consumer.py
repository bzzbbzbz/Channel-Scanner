"""Manual-offset Kafka consumer for durable database-backed outcomes."""

from __future__ import annotations

import asyncio
import json
import logging
from collections import namedtuple
from dataclasses import dataclass
from enum import Enum
from typing import Any, Awaitable, Callable, Mapping

try:
    from aiokafka import AIOKafkaConsumer
    from aiokafka.structs import TopicPartition
except ModuleNotFoundError:  # Portable repository tests do not require Kafka extras.
    AIOKafkaConsumer = None  # type: ignore[assignment,misc]
    TopicPartition = namedtuple("TopicPartition", "topic partition")  # type: ignore[misc]

from src.config.settings import KafkaSettings, ReliableDeliverySettings
from src.reliability.contracts import (
    DIGEST_RUN_REQUESTED_EVENT,
    DIGEST_RUN_REQUESTED_TOPIC,
    TELEGRAM_DELIVERY_REQUESTED_EVENT,
    TELEGRAM_DELIVERY_REQUESTED_TOPIC,
    validate_event,
)

logger = logging.getLogger(__name__)
DIGEST_CONSUMER_GROUP = "digest-renderer-v1"
DELIVERY_CONSUMER_GROUP = "telegram-delivery-v1"


class ConsumerOutcome(str, Enum):
    COMMIT = "commit"
    RETRY = "retry"


@dataclass(frozen=True)
class RejectedKafkaEvent:
    """Content-free source coordinates plus an optional parsed envelope."""

    topic: str
    partition: int
    offset: int
    error_code: str
    event: Mapping[str, Any] | None


def _new_consumer(settings: KafkaSettings, reliable_settings: ReliableDeliverySettings) -> AIOKafkaConsumer:
    if AIOKafkaConsumer is None:
        raise RuntimeError("aiokafka is required for the digest-worker role")
    return AIOKafkaConsumer(
        DIGEST_RUN_REQUESTED_TOPIC,
        bootstrap_servers=settings.bootstrap_servers,
        security_protocol=settings.security_protocol,
        client_id=f"{settings.client_id_prefix}-digest-worker",
        group_id=DIGEST_CONSUMER_GROUP,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        request_timeout_ms=settings.request_timeout_ms,
        max_poll_interval_ms=int((reliable_settings.render_lease_seconds + 60) * 1000),
        max_partition_fetch_bytes=max(131_072, settings.max_event_bytes + 1024),
    )


def _new_delivery_consumer(settings: KafkaSettings, reliable_settings: ReliableDeliverySettings) -> AIOKafkaConsumer:
    if AIOKafkaConsumer is None:
        raise RuntimeError("aiokafka is required for the telegram-delivery-worker role")
    return AIOKafkaConsumer(
        TELEGRAM_DELIVERY_REQUESTED_TOPIC,
        bootstrap_servers=settings.bootstrap_servers,
        security_protocol=settings.security_protocol,
        client_id=f"{settings.client_id_prefix}-telegram-delivery-worker",
        group_id=DELIVERY_CONSUMER_GROUP,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        request_timeout_ms=settings.request_timeout_ms,
        max_poll_interval_ms=int((reliable_settings.delivery_lease_seconds + 60) * 1000),
        max_partition_fetch_bytes=max(131_072, settings.max_event_bytes + 1024),
    )


class KafkaEventConsumer:
    """Validate one event contract and advance Kafka only after a durable DB outcome."""

    def __init__(
        self,
        reliable_settings: ReliableDeliverySettings,
        handler: Callable[[dict[str, Any]], Awaitable[ConsumerOutcome]],
        *,
        consumer: Any,
        expected_topic: str,
        expected_event_type: str,
        max_event_bytes: int,
        worker_name: str,
        rejection_handler: Callable[[RejectedKafkaEvent], Awaitable[ConsumerOutcome]] | None = None,
        after_database_commit: Callable[[], None] | None = None,
    ) -> None:
        self._consumer = consumer
        self._handler = handler
        self._expected_topic = expected_topic
        self._expected_event_type = expected_event_type
        self._worker_name = worker_name
        self._max_event_bytes = max_event_bytes
        self._rejection_handler = rejection_handler
        self._after_database_commit = after_database_commit
        self._poll_timeout = reliable_settings.consumer_poll_timeout_ms / 1000
        self._retry_delay = reliable_settings.poll_interval_seconds

    async def start(self) -> None:
        await self._consumer.start()

    async def stop(self) -> None:
        await self._consumer.stop()

    async def consume_one(self) -> bool:
        try:
            message = await asyncio.wait_for(self._consumer.getone(), timeout=self._poll_timeout)
        except TimeoutError:
            return False
        partition = TopicPartition(message.topic, message.partition)
        parsed_event: Mapping[str, Any] | None = None
        try:
            if len(message.value) > self._max_event_bytes:
                raise ValueError("event_too_large")
            if message.topic != self._expected_topic:
                raise ValueError("unexpected_topic")
            event = json.loads(message.value.decode("utf-8"))
            parsed_event = event if isinstance(event, Mapping) else None
            validate_event(event)
            if event["event_type"] != self._expected_event_type:
                raise ValueError("unexpected_event_type")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            error_code = _rejection_code(exc, parsed_event)
            logger.warning(
                "%s event rejected: topic=%s partition=%d offset=%d error=%s",
                self._worker_name,
                message.topic,
                message.partition,
                message.offset,
                error_code,
            )
            if self._rejection_handler is None:
                outcome = ConsumerOutcome.RETRY
            else:
                try:
                    outcome = await self._rejection_handler(
                        RejectedKafkaEvent(
                            topic=message.topic,
                            partition=message.partition,
                            offset=message.offset,
                            error_code=error_code,
                            event=parsed_event,
                        )
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception(
                        "%s rejected event could not be persisted: topic=%s partition=%d offset=%d",
                        self._worker_name,
                        message.topic,
                        message.partition,
                        message.offset,
                    )
                    outcome = ConsumerOutcome.RETRY
        else:
            try:
                outcome = await self._handler(event)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "%s event processing will retry: topic=%s partition=%d offset=%d error=%s",
                    self._worker_name,
                    message.topic,
                    message.partition,
                    message.offset,
                    type(exc).__name__,
                )
                outcome = ConsumerOutcome.RETRY

        if outcome == ConsumerOutcome.COMMIT:
            if self._after_database_commit is not None:
                self._after_database_commit()
            await self._consumer.commit({partition: message.offset + 1})
            return True
        self._consumer.seek(partition, message.offset)
        await asyncio.sleep(self._retry_delay)
        return False

    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            await self.consume_one()


class KafkaDigestConsumer(KafkaEventConsumer):
    def __init__(
        self,
        kafka_settings: KafkaSettings,
        reliable_settings: ReliableDeliverySettings,
        handler: Callable[[dict[str, Any]], Awaitable[ConsumerOutcome]],
        *,
        rejection_handler: Callable[[RejectedKafkaEvent], Awaitable[ConsumerOutcome]] | None = None,
        consumer_factory: Callable[[KafkaSettings, ReliableDeliverySettings], Any] = _new_consumer,
        after_database_commit: Callable[[], None] | None = None,
    ) -> None:
        super().__init__(
            reliable_settings,
            handler,
            consumer=consumer_factory(kafka_settings, reliable_settings),
            expected_topic=DIGEST_RUN_REQUESTED_TOPIC,
            expected_event_type=DIGEST_RUN_REQUESTED_EVENT,
            max_event_bytes=kafka_settings.max_event_bytes,
            worker_name="Digest",
            rejection_handler=rejection_handler,
            after_database_commit=after_database_commit,
        )


class KafkaDeliveryConsumer(KafkaEventConsumer):
    def __init__(
        self,
        kafka_settings: KafkaSettings,
        reliable_settings: ReliableDeliverySettings,
        handler: Callable[[dict[str, Any]], Awaitable[ConsumerOutcome]],
        *,
        rejection_handler: Callable[[RejectedKafkaEvent], Awaitable[ConsumerOutcome]] | None = None,
        consumer_factory: Callable[[KafkaSettings, ReliableDeliverySettings], Any] = _new_delivery_consumer,
    ) -> None:
        super().__init__(
            reliable_settings,
            handler,
            consumer=consumer_factory(kafka_settings, reliable_settings),
            expected_topic=TELEGRAM_DELIVERY_REQUESTED_TOPIC,
            expected_event_type=TELEGRAM_DELIVERY_REQUESTED_EVENT,
            max_event_bytes=kafka_settings.max_event_bytes,
            worker_name="Telegram delivery",
            rejection_handler=rejection_handler,
        )


def _rejection_code(exc: Exception, event: Mapping[str, Any] | None) -> str:
    if event is not None and event.get("event_version") != 1:
        return "UnsupportedEventVersion"
    if isinstance(exc, (UnicodeDecodeError, json.JSONDecodeError)):
        return "UnreadablePayload"
    if isinstance(exc, ValueError) and str(exc) == "event_too_large":
        return "EventTooLarge"
    return "InvalidEventSchema"
