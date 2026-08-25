"""Bounded mapping coverage for the aiokafka 0.14 operations probe."""

import asyncio
import time
from types import SimpleNamespace

import pytest
from aiokafka.structs import TopicPartition
from aiokafka.errors import for_code

from src.config.settings import KafkaSettings
from src.reliability.contracts import DIGEST_RUN_REQUESTED_TOPIC
from src.reliability.kafka_admin import expected_topics
from src.reliability.kafka_consumer import DIGEST_CONSUMER_GROUP
from src.reliability.kafka_operations import KafkaOperationsProbe


class FakeAdmin:
    def __init__(
        self,
        settings: KafkaSettings,
        *,
        groups=(),
        committed_offsets=None,
        group_error_codes=None,
        members: bool = True,
        offsets_error: Exception | None = None,
        configs_error: Exception | None = None,
        config_error_code: int = 0,
    ) -> None:
        self.settings = settings
        self.groups = list(groups)
        self.committed_offsets = committed_offsets
        self.group_error_codes = group_error_codes or {}
        self.members = members
        self.offsets_error = offsets_error
        self.configs_error = configs_error
        self.config_error_code = config_error_code
        self.closed = False

    async def start(self) -> None:
        return None

    async def close(self) -> None:
        self.closed = True

    async def describe_cluster(self):
        return {"controller_id": 1}

    async def describe_topics(self, names):
        return [
            {
                "topic": spec.name,
                "error_code": 0,
                "partitions": [
                    {"partition": index, "replicas": list(range(spec.replication_factor))}
                    for index in range(spec.partitions)
                ],
            }
            for spec in expected_topics(self.settings)
        ]

    async def describe_configs(self, resources):
        if self.configs_error is not None:
            raise self.configs_error
        return [
            {
                "resources": [
                    {
                        "resource_name": spec.name,
                        "error_code": self.config_error_code if index == 0 else 0,
                        "config_entries": [
                            {"config_name": key, "config_value": value}
                            for key, value in spec.configs.items()
                        ],
                    }
                    for index, spec in enumerate(expected_topics(self.settings))
                ]
            }
        ]

    async def list_consumer_groups(self):
        return [(group, "consumer") for group in self.groups]

    async def describe_consumer_groups(self, groups):
        return [
            {
                "groups": [
                    {
                        "group": group,
                        "error_code": self.group_error_codes.get(group, 0),
                        "state": "Stable" if self.members else "Empty",
                        "members": [{"member_id": "redacted"}] if self.members else [],
                    }
                    for group in groups
                ]
            }
        ]

    async def list_consumer_group_offsets(self, group):
        assert group == DIGEST_CONSUMER_GROUP
        if self.offsets_error is not None:
            raise self.offsets_error
        return self.committed_offsets if self.committed_offsets is not None else {
            TopicPartition(DIGEST_RUN_REQUESTED_TOPIC, 0): SimpleNamespace(offset=7)
        }


class FakeOffsetConsumer:
    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def end_offsets(self, partitions):
        return {partition: (10 if partition.topic == DIGEST_RUN_REQUESTED_TOPIC else 0) for partition in partitions}


@pytest.mark.asyncio
async def test_kafka_operations_probe_maps_topics_active_lag_and_shadow_missing_group() -> None:
    settings = KafkaSettings(enabled=True)
    admin = FakeAdmin(settings, groups=[DIGEST_CONSUMER_GROUP])
    probe = KafkaOperationsProbe(
        settings,
        admin_factory=lambda _: admin,
        consumer_factory=lambda _: FakeOffsetConsumer(),
    )

    result = await probe.probe()

    assert result["broker"]["status"] == "available"
    assert len(result["topics"]) == 4
    assert all(topic["drift"] is False for topic in result["topics"])
    assert [topic["name"] for topic in result["topics"]] == [spec.name for spec in expected_topics(settings)]
    assert result["consumer_groups"][0] == {
        "group_id": "digest-renderer-v1",
        "topic": DIGEST_RUN_REQUESTED_TOPIC,
        "status": "active",
        "lag": 3,
        "error_code": None,
    }
    assert result["consumer_groups"][1]["status"] == "inactive"
    assert result["consumer_groups"][1]["lag"] is None
    assert admin.closed is True


@pytest.mark.asyncio
async def test_kafka_operations_probe_reports_both_missing_shadow_groups_as_inactive() -> None:
    settings = KafkaSettings(enabled=True)
    probe = KafkaOperationsProbe(settings, admin_factory=lambda _: FakeAdmin(settings))

    result = await probe.probe()

    assert [group["status"] for group in result["consumer_groups"]] == ["inactive", "inactive"]


@pytest.mark.asyncio
async def test_topic_config_probe_failure_is_degraded_not_healthy() -> None:
    settings = KafkaSettings(enabled=True)
    probe = KafkaOperationsProbe(
        settings,
        admin_factory=lambda _: FakeAdmin(settings, configs_error=PermissionError()),
    )

    result = await probe.probe()

    assert {topic["status"] for topic in result["topics"]} == {"degraded"}
    assert {topic["drift"] for topic in result["topics"]} == {None}
    assert {topic["error_code"] for topic in result["topics"]} == {"PermissionError"}


@pytest.mark.asyncio
async def test_per_topic_config_error_is_degraded_not_healthy() -> None:
    settings = KafkaSettings(enabled=True)
    probe = KafkaOperationsProbe(
        settings,
        admin_factory=lambda _: FakeAdmin(settings, config_error_code=29),
    )

    result = await probe.probe()

    assert result["topics"][0]["status"] == "degraded"
    assert result["topics"][0]["drift"] is None
    assert result["topics"][0]["error_code"] == for_code(29).__name__
    assert all(topic["status"] == "available" for topic in result["topics"][1:])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "committed_offsets",
    [
        {TopicPartition(DIGEST_RUN_REQUESTED_TOPIC, 0): SimpleNamespace(offset=7)},
        {
            TopicPartition(DIGEST_RUN_REQUESTED_TOPIC, 0): SimpleNamespace(offset=7),
            TopicPartition(DIGEST_RUN_REQUESTED_TOPIC, 1): SimpleNamespace(offset=-1),
        },
    ],
    ids=("missing", "negative"),
)
async def test_kafka_operations_probe_returns_unknown_lag_when_any_partition_has_no_offset(
    committed_offsets,
) -> None:
    settings = KafkaSettings(enabled=True, topic_partitions=2)
    admin = FakeAdmin(
        settings,
        groups=[DIGEST_CONSUMER_GROUP],
        committed_offsets=committed_offsets,
    )
    probe = KafkaOperationsProbe(
        settings,
        admin_factory=lambda _: admin,
        consumer_factory=lambda _: FakeOffsetConsumer(),
    )

    result = await probe.probe()

    assert result["consumer_groups"][0]["lag"] is None
    assert result["consumer_groups"][0]["status"] == "unavailable"
    assert result["consumer_groups"][0]["error_code"] == "OffsetNotInitialized"


@pytest.mark.asyncio
async def test_uninitialized_offsets_are_inactive_for_group_without_members() -> None:
    settings = KafkaSettings(enabled=True)
    admin = FakeAdmin(
        settings,
        groups=[DIGEST_CONSUMER_GROUP],
        committed_offsets={},
        members=False,
    )
    probe = KafkaOperationsProbe(
        settings,
        admin_factory=lambda _: admin,
        consumer_factory=lambda _: FakeOffsetConsumer(),
    )

    result = await probe.probe()

    assert result["consumer_groups"][0]["status"] == "inactive"
    assert result["consumer_groups"][0]["error_code"] == "OffsetNotInitialized"


@pytest.mark.asyncio
async def test_group_description_and_offset_api_errors_are_preserved_per_group() -> None:
    settings = KafkaSettings(enabled=True)
    broker_error = 15
    described = await KafkaOperationsProbe(
        settings,
        admin_factory=lambda _: FakeAdmin(
            settings,
            groups=[DIGEST_CONSUMER_GROUP],
            group_error_codes={DIGEST_CONSUMER_GROUP: broker_error},
        ),
        consumer_factory=lambda _: FakeOffsetConsumer(),
    ).probe()
    assert described["consumer_groups"][0]["status"] == "unavailable"
    assert described["consumer_groups"][0]["error_code"] == for_code(broker_error).__name__

    offsets = await KafkaOperationsProbe(
        settings,
        admin_factory=lambda _: FakeAdmin(
            settings,
            groups=[DIGEST_CONSUMER_GROUP],
            offsets_error=ConnectionError(),
        ),
        consumer_factory=lambda _: FakeOffsetConsumer(),
    ).probe()
    assert offsets["consumer_groups"][0]["status"] == "unavailable"
    assert offsets["consumer_groups"][0]["error_code"] == "ConnectionError"

    class FailedEndOffsets(FakeOffsetConsumer):
        async def end_offsets(self, partitions):
            raise TimeoutError

    end_offsets = await KafkaOperationsProbe(
        settings,
        admin_factory=lambda _: FakeAdmin(settings, groups=[DIGEST_CONSUMER_GROUP]),
        consumer_factory=lambda _: FailedEndOffsets(),
    ).probe()
    assert end_offsets["consumer_groups"][0]["status"] == "unavailable"
    assert end_offsets["consumer_groups"][0]["error_code"] == "TimeoutError"


@pytest.mark.asyncio
async def test_kafka_operations_probe_timeout_and_broker_failure_are_safe() -> None:
    settings = KafkaSettings(enabled=True)

    class SlowAdmin(FakeAdmin):
        async def start(self) -> None:
            await asyncio.sleep(60)

    timed_out = await KafkaOperationsProbe(
        settings,
        timeout_seconds=0.01,
        admin_factory=lambda _: SlowAdmin(settings),
    ).probe()
    assert timed_out["broker"] == {"status": "unavailable", "latency_ms": None, "error_code": "ProbeTimeout"}

    class FailedAdmin(FakeAdmin):
        async def start(self) -> None:
            raise ConnectionError

    failed = await KafkaOperationsProbe(settings, admin_factory=lambda _: FailedAdmin(settings)).probe()
    assert failed["broker"]["status"] == "unavailable"
    assert failed["broker"]["error_code"] == "ConnectionError"


@pytest.mark.asyncio
async def test_kafka_operations_probe_timeout_is_single_flight_across_repeated_refreshes() -> None:
    settings = KafkaSettings(enabled=True)
    release = asyncio.Event()
    created = 0

    class StubbornAdmin(FakeAdmin):
        async def start(self) -> None:
            while not release.is_set():
                try:
                    await release.wait()
                except asyncio.CancelledError:
                    continue

    def factory(_: KafkaSettings):
        nonlocal created
        created += 1
        return StubbornAdmin(settings)

    probe = KafkaOperationsProbe(
        settings,
        timeout_seconds=0.01,
        cleanup_timeout_seconds=0.01,
        admin_factory=factory,
    )
    first = await probe.probe()
    repeated = await asyncio.gather(*(probe.probe() for _ in range(5)))

    assert first["broker"]["error_code"] == "ProbeTimeout"
    assert {result["broker"]["error_code"] for result in repeated} == {"ProbeBusy"}
    assert created == 1

    release.set()
    active = probe._active_task
    assert active is not None
    await asyncio.wait_for(asyncio.shield(active), timeout=1)


@pytest.mark.asyncio
async def test_hanging_cancellation_resistant_cleanup_is_bounded_and_blocks_new_clients() -> None:
    settings = KafkaSettings(enabled=True)
    release = asyncio.Event()
    created = 0

    class HangingCloseAdmin(FakeAdmin):
        async def close(self) -> None:
            while not release.is_set():
                try:
                    await release.wait()
                except asyncio.CancelledError:
                    continue
            self.closed = True

    def factory(_: KafkaSettings):
        nonlocal created
        created += 1
        return HangingCloseAdmin(settings)

    probe = KafkaOperationsProbe(
        settings,
        timeout_seconds=0.2,
        cleanup_timeout_seconds=0.01,
        admin_factory=factory,
    )
    started = time.monotonic()
    first = await probe.probe()
    elapsed = time.monotonic() - started
    repeated = await asyncio.gather(*(probe.probe() for _ in range(5)))

    assert elapsed < 0.15
    assert first["broker"]["status"] == "available"
    assert {result["broker"]["error_code"] for result in repeated} == {"ProbeBusy"}
    assert created == 1
    assert len(probe._cleanup_tasks) == 1

    release.set()
    cleanup = next(iter(probe._cleanup_tasks))
    await asyncio.wait_for(asyncio.shield(cleanup), timeout=1)
    await asyncio.sleep(0)
    assert not probe._cleanup_tasks

    recovered = await probe.probe()
    assert recovered["broker"]["status"] == "available"
    assert created == 2
