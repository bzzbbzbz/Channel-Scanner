"""Idempotent Kafka producer used by the transactional outbox relay."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

from aiokafka import AIOKafkaProducer

from src.config.settings import KafkaSettings
from src.reliability.contracts import EventContractError


@dataclass(frozen=True)
class PublicationResult:
    partition: int
    offset: int
    published_at: datetime


def _new_producer(settings: KafkaSettings) -> AIOKafkaProducer:
    return AIOKafkaProducer(
        bootstrap_servers=settings.bootstrap_servers,
        security_protocol=settings.security_protocol,
        client_id=f"{settings.client_id_prefix}-outbox-relay",
        request_timeout_ms=settings.request_timeout_ms,
        max_request_size=max(131_072, settings.max_event_bytes + 1024),
        enable_idempotence=True,
        acks="all",
    )


class KafkaEventProducer:
    """Publish already validated bytes and return broker acknowledgement metadata."""

    def __init__(
        self,
        settings: KafkaSettings,
        *,
        producer_factory: Callable[[KafkaSettings], Any] = _new_producer,
    ) -> None:
        self._producer = producer_factory(settings)
        self._max_event_bytes = settings.max_event_bytes

    async def start(self) -> None:
        await self._producer.start()

    async def stop(self) -> None:
        await self._producer.stop()

    async def publish(self, *, topic: str, event_key: str, value: bytes) -> PublicationResult:
        if len(value) > self._max_event_bytes:
            raise EventContractError(f"Serialized event is {len(value)} bytes; limit is {self._max_event_bytes}")
        metadata = await self._producer.send_and_wait(topic, value=value, key=event_key.encode("utf-8"))
        timestamp_ms = getattr(metadata, "timestamp", None)
        published_at = (
            datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
            if timestamp_ms is not None and timestamp_ms >= 0
            else datetime.now(timezone.utc)
        )
        return PublicationResult(
            partition=int(metadata.partition),
            offset=int(metadata.offset),
            published_at=published_at,
        )
