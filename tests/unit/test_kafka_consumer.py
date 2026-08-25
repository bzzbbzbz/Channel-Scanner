from __future__ import annotations

import json
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.config.settings import KafkaSettings, ReliableDeliverySettings
from src.reliability.contracts import (
    DIGEST_RUN_REQUESTED_EVENT,
    TELEGRAM_DELIVERY_REQUESTED_EVENT,
    load_event_example,
)
from src.reliability.kafka_consumer import (
    ConsumerOutcome,
    KafkaDeliveryConsumer,
    KafkaDigestConsumer,
    _new_consumer,
    _new_delivery_consumer,
)


def test_digest_consumer_disables_auto_commit_for_fixed_group() -> None:
    settings = KafkaSettings()
    with patch("src.reliability.kafka_consumer.AIOKafkaConsumer") as consumer_cls:
        _new_consumer(settings, ReliableDeliverySettings())

    assert consumer_cls.call_args.kwargs["enable_auto_commit"] is False
    assert consumer_cls.call_args.kwargs["group_id"] == "digest-renderer-v1"
    assert consumer_cls.call_args.kwargs["max_poll_interval_ms"] > 900_000


def test_delivery_consumer_disables_auto_commit_for_fixed_group() -> None:
    with patch("src.reliability.kafka_consumer.AIOKafkaConsumer") as consumer_cls:
        _new_delivery_consumer(KafkaSettings(), ReliableDeliverySettings())

    assert consumer_cls.call_args.kwargs["enable_auto_commit"] is False
    assert consumer_cls.call_args.kwargs["group_id"] == "telegram-delivery-v1"


@pytest.mark.asyncio
async def test_consumer_commits_offset_only_after_database_outcome() -> None:
    event = deepcopy(load_event_example(DIGEST_RUN_REQUESTED_EVENT))
    message = SimpleNamespace(topic="tpb.digest.run.requested.v1", partition=0, offset=41, value=json.dumps(event).encode())
    raw_consumer = MagicMock()
    raw_consumer.getone = AsyncMock(return_value=message)
    raw_consumer.commit = AsyncMock()
    handler = AsyncMock(return_value=ConsumerOutcome.COMMIT)
    consumer = KafkaDigestConsumer(
        KafkaSettings(),
        ReliableDeliverySettings(),
        handler,
        consumer_factory=lambda settings, reliable_settings: raw_consumer,
    )

    assert await consumer.consume_one() is True
    handler.assert_awaited_once()
    raw_consumer.commit.assert_awaited_once()
    raw_consumer.seek.assert_not_called()


@pytest.mark.asyncio
async def test_consumer_fault_boundary_runs_after_database_outcome_before_offset_commit() -> None:
    event = deepcopy(load_event_example(DIGEST_RUN_REQUESTED_EVENT))
    message = SimpleNamespace(topic="tpb.digest.run.requested.v1", partition=0, offset=41, value=json.dumps(event).encode())
    raw_consumer = MagicMock()
    raw_consumer.getone = AsyncMock(return_value=message)
    raw_consumer.commit = AsyncMock()
    sequence = []

    async def handler(_event):
        sequence.append("database")
        return ConsumerOutcome.COMMIT

    consumer = KafkaDigestConsumer(
        KafkaSettings(),
        ReliableDeliverySettings(),
        handler,
        consumer_factory=lambda settings, reliable_settings: raw_consumer,
        after_database_commit=lambda: sequence.append("fault-boundary"),
    )
    raw_consumer.commit.side_effect = lambda *_args, **_kwargs: sequence.append("offset")

    assert await consumer.consume_one() is True
    assert sequence == ["database", "fault-boundary", "offset"]


@pytest.mark.asyncio
async def test_consumer_does_not_commit_invalid_event() -> None:
    event = deepcopy(load_event_example(DIGEST_RUN_REQUESTED_EVENT))
    event["payload"]["rendered_html"] = "forbidden"
    message = SimpleNamespace(topic="tpb.digest.run.requested.v1", partition=0, offset=2, value=json.dumps(event).encode())
    raw_consumer = MagicMock()
    raw_consumer.getone = AsyncMock(return_value=message)
    raw_consumer.commit = AsyncMock()
    consumer = KafkaDigestConsumer(
        KafkaSettings(),
        ReliableDeliverySettings(poll_interval_seconds=0.001),
        AsyncMock(),
        consumer_factory=lambda settings, reliable_settings: raw_consumer,
    )

    assert await consumer.consume_one() is False
    raw_consumer.commit.assert_not_awaited()
    raw_consumer.seek.assert_called_once()


@pytest.mark.asyncio
async def test_delivery_consumer_validates_delivery_contract_before_commit() -> None:
    event = deepcopy(load_event_example(TELEGRAM_DELIVERY_REQUESTED_EVENT))
    message = SimpleNamespace(
        topic="tpb.telegram.delivery.requested.v1",
        partition=1,
        offset=5,
        value=json.dumps(event).encode(),
    )
    raw_consumer = MagicMock()
    raw_consumer.getone = AsyncMock(return_value=message)
    raw_consumer.commit = AsyncMock()
    handler = AsyncMock(return_value=ConsumerOutcome.COMMIT)
    consumer = KafkaDeliveryConsumer(
        KafkaSettings(),
        ReliableDeliverySettings(),
        handler,
        consumer_factory=lambda settings, reliable_settings: raw_consumer,
    )

    assert await consumer.consume_one() is True
    handler.assert_awaited_once_with(event)
    raw_consumer.commit.assert_awaited_once()
