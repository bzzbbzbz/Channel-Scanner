from __future__ import annotations

from datetime import timezone
from types import SimpleNamespace

import pytest

from src.config.settings import KafkaSettings
from src.reliability import kafka_producer
from src.reliability.kafka_producer import KafkaEventProducer
from src.reliability.contracts import EventContractError


def test_real_producer_factory_requires_idempotence_and_all_acknowledgements(monkeypatch) -> None:
    captured = {}
    sentinel = object()

    def factory(**kwargs):
        captured.update(kwargs)
        return sentinel

    monkeypatch.setattr(kafka_producer, "AIOKafkaProducer", factory)

    producer = kafka_producer._new_producer(KafkaSettings(max_event_bytes=32_768))

    assert producer is sentinel
    assert captured["enable_idempotence"] is True
    assert captured["acks"] == "all"
    assert captured["max_request_size"] >= 32_768


@pytest.mark.asyncio
async def test_producer_uses_utf8_key_and_returns_broker_metadata() -> None:
    class FakeProducer:
        async def send_and_wait(self, topic, *, value, key):
            assert topic == "topic.v1"
            assert value == b"{}"
            assert key == "подписка-42".encode()
            return SimpleNamespace(partition=2, offset=91, timestamp=1_787_486_400_000)

        async def start(self):
            pass

        async def stop(self):
            pass

    producer = KafkaEventProducer(KafkaSettings(), producer_factory=lambda _: FakeProducer())

    result = await producer.publish(topic="topic.v1", event_key="подписка-42", value=b"{}")

    assert result.partition == 2
    assert result.offset == 91
    assert result.published_at.tzinfo == timezone.utc


@pytest.mark.asyncio
async def test_producer_rejects_value_over_configured_event_limit_before_send() -> None:
    class FakeProducer:
        async def send_and_wait(self, *args, **kwargs):
            raise AssertionError("oversized value reached Kafka client")

    producer = KafkaEventProducer(
        KafkaSettings(max_event_bytes=1024),
        producer_factory=lambda _: FakeProducer(),
    )

    with pytest.raises(EventContractError, match="limit is 1024"):
        await producer.publish(topic="topic.v1", event_key="42", value=b"x" * 1025)
