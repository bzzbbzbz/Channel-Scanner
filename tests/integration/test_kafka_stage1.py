from __future__ import annotations

import os
from copy import deepcopy
from uuid import uuid4

import pytest

from src.config.settings import KafkaSettings
from src.reliability.kafka_admin import check_topics, ensure_topics
from src.reliability.contracts import (
    DIGEST_RUN_REQUESTED_EVENT,
    DIGEST_RUN_REQUESTED_TOPIC,
    load_event_example,
    serialize_event,
)
from src.reliability.kafka_producer import KafkaEventProducer


@pytest.mark.skipif(
    os.getenv("KAFKA_INTEGRATION") != "1",
    reason="requires an explicitly isolated Kafka broker",
)
@pytest.mark.asyncio
async def test_stage1_real_kafka_provisions_and_rechecks_topics() -> None:
    settings = KafkaSettings(
        enabled=True,
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )

    ensured = await ensure_topics(settings)
    checked = await check_topics(settings)

    assert ensured.ok
    assert checked.ok


@pytest.mark.skipif(
    os.getenv("KAFKA_INTEGRATION") != "1",
    reason="requires an explicitly isolated Kafka broker",
)
@pytest.mark.asyncio
async def test_stage2_real_kafka_idempotent_producer_publishes_stable_event_twice() -> None:
    settings = KafkaSettings(
        enabled=True,
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    await ensure_topics(settings)
    event = deepcopy(load_event_example(DIGEST_RUN_REQUESTED_EVENT))
    event["event_id"] = str(uuid4())
    event["correlation_id"] = str(uuid4())
    value = serialize_event(event, max_bytes=settings.max_event_bytes)
    producer = KafkaEventProducer(settings)
    await producer.start()
    try:
        first = await producer.publish(topic=DIGEST_RUN_REQUESTED_TOPIC, event_key="42", value=value)
        second = await producer.publish(topic=DIGEST_RUN_REQUESTED_TOPIC, event_key="42", value=value)
    finally:
        await producer.stop()

    assert first.partition == second.partition
    assert second.offset > first.offset
