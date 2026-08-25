"""Idempotent Kafka topic provisioning and strict drift checks."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Callable

from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.admin.config_resource import ConfigResource, ConfigResourceType
from aiokafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError, for_code

from src.config.settings import KafkaSettings
from src.reliability.contracts import (
    DIGEST_RUN_REQUESTED_DLQ_TOPIC,
    DIGEST_RUN_REQUESTED_TOPIC,
    TELEGRAM_DELIVERY_REQUESTED_DLQ_TOPIC,
    TELEGRAM_DELIVERY_REQUESTED_TOPIC,
)


class TopicConfigurationError(RuntimeError):
    """Kafka topics are absent or differ from the configured contract."""


@dataclass(frozen=True)
class TopicSpec:
    name: str
    partitions: int
    replication_factor: int
    configs: dict[str, str]


@dataclass(frozen=True)
class TopicCheckReport:
    missing_topics: tuple[str, ...] = ()
    mismatches: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        return not self.missing_topics and not self.mismatches

    def require_ok(self) -> None:
        if not self.ok:
            details = [*(f"missing topic {name}" for name in self.missing_topics), *self.mismatches]
            raise TopicConfigurationError("; ".join(details))


def expected_topics(settings: KafkaSettings) -> tuple[TopicSpec, ...]:
    """Build the four fixed v1 topic definitions from environment tuning."""
    common = {
        "cleanup.policy": "delete",
        "max.message.bytes": "131072",
        "segment.bytes": "134217728",
    }

    def spec(name: str, *, dlq: bool) -> TopicSpec:
        configs = {
            **common,
            "retention.ms": str(settings.dlq_retention_ms if dlq else settings.topic_retention_ms),
            "retention.bytes": str(settings.dlq_retention_bytes if dlq else settings.topic_retention_bytes),
        }
        return TopicSpec(
            name=name,
            partitions=settings.topic_partitions,
            replication_factor=settings.topic_replication_factor,
            configs=configs,
        )

    return (
        spec(DIGEST_RUN_REQUESTED_TOPIC, dlq=False),
        spec(TELEGRAM_DELIVERY_REQUESTED_TOPIC, dlq=False),
        spec(DIGEST_RUN_REQUESTED_DLQ_TOPIC, dlq=True),
        spec(TELEGRAM_DELIVERY_REQUESTED_DLQ_TOPIC, dlq=True),
    )


def _new_admin(settings: KafkaSettings, client_suffix: str) -> AIOKafkaAdminClient:
    return AIOKafkaAdminClient(
        bootstrap_servers=settings.bootstrap_servers,
        security_protocol=settings.security_protocol,
        client_id=f"{settings.client_id_prefix}-{client_suffix}",
        request_timeout_ms=settings.request_timeout_ms,
    )


async def ensure_topics(
    settings: KafkaSettings,
    *,
    admin_factory: Callable[[KafkaSettings, str], Any] = _new_admin,
) -> TopicCheckReport:
    """Create missing topics, never mutate existing topics, then verify drift."""
    specs = expected_topics(settings)
    admin = admin_factory(settings, "topic-init")
    await admin.start()
    try:
        existing = set(await admin.list_topics())
        missing_specs = [spec for spec in specs if spec.name not in existing]
        if missing_specs:
            response = await admin.create_topics(
                [
                    NewTopic(
                        name=spec.name,
                        num_partitions=spec.partitions,
                        replication_factor=spec.replication_factor,
                        topic_configs=spec.configs,
                    )
                    for spec in missing_specs
                ],
                timeout_ms=settings.request_timeout_ms,
            )
            _raise_create_errors(response)

        deadline = asyncio.get_running_loop().time() + settings.startup_timeout_seconds
        while True:
            report = await _check_topics_with_admin(admin, specs)
            if report.ok or report.mismatches or asyncio.get_running_loop().time() >= deadline:
                report.require_ok()
                return report
            await asyncio.sleep(1)
    finally:
        await admin.close()


async def check_topics(
    settings: KafkaSettings,
    *,
    admin_factory: Callable[[KafkaSettings, str], Any] = _new_admin,
) -> TopicCheckReport:
    """Check broker connectivity and exact topic metadata/configuration."""
    admin = admin_factory(settings, "topic-check")
    await admin.start()
    try:
        return await _check_topics_with_admin(admin, expected_topics(settings))
    finally:
        await admin.close()


async def check_cluster(
    settings: KafkaSettings,
    *,
    admin_factory: Callable[[KafkaSettings, str], Any] = _new_admin,
) -> None:
    """Verify that the Kafka broker answers a metadata request."""
    admin = admin_factory(settings, "cluster-check")
    await admin.start()
    try:
        await admin.describe_cluster()
    finally:
        await admin.close()


def _raise_create_errors(response: Any) -> None:
    for topic_error in response.topic_errors:
        topic, error_code, *rest = topic_error
        if error_code == 0:
            continue
        error_type = for_code(error_code)
        if issubclass(error_type, TopicAlreadyExistsError):
            continue
        message = rest[0] if rest else "topic creation failed"
        raise error_type(f"{topic}: {message}")


async def _check_topics_with_admin(admin: Any, specs: tuple[TopicSpec, ...]) -> TopicCheckReport:
    names = [spec.name for spec in specs]
    metadata = await admin.describe_topics(names)
    metadata_by_name: dict[str, dict[str, Any]] = {}
    metadata_errors: list[str] = []
    for item in metadata:
        name = item.get("topic", item.get("name"))
        error_code = item.get("error_code", 0)
        if error_code:
            error_type = for_code(error_code)
            if not issubclass(error_type, UnknownTopicOrPartitionError):
                metadata_errors.append(f"{name}: metadata error code {error_code}")
            continue
        metadata_by_name[name] = item
    missing = tuple(sorted(name for name in names if name not in metadata_by_name))
    mismatches: list[str] = metadata_errors

    for spec in specs:
        item = metadata_by_name.get(spec.name)
        if item is None:
            continue
        partitions = item.get("partitions", [])
        if len(partitions) != spec.partitions:
            mismatches.append(f"{spec.name}: partitions={len(partitions)}, expected={spec.partitions}")
        replica_counts = {len(partition.get("replicas", [])) for partition in partitions}
        if replica_counts and replica_counts != {spec.replication_factor}:
            mismatches.append(
                f"{spec.name}: replication_factors={sorted(replica_counts)}, expected={spec.replication_factor}"
            )

    if not missing:
        config_keys = sorted({key for spec in specs for key in spec.configs})
        resources_to_check = [
            ConfigResource(ConfigResourceType.TOPIC, spec.name, {key: None for key in config_keys})
            for spec in specs
        ]
        responses = await admin.describe_configs(resources_to_check)
        actual_configs = _parse_topic_configs(responses)
        for spec in specs:
            topic_config = actual_configs.get(spec.name)
            if topic_config is None:
                mismatches.append(f"{spec.name}: configuration response missing")
                continue
            for key, expected in spec.configs.items():
                actual = topic_config.get(key)
                if actual != expected:
                    mismatches.append(f"{spec.name}: {key}={actual!r}, expected={expected!r}")

    return TopicCheckReport(missing_topics=missing, mismatches=tuple(sorted(mismatches)))


def _parse_topic_configs(responses: list[Any]) -> dict[str, dict[str, str | None]]:
    parsed: dict[str, dict[str, str | None]] = {}
    for response in responses:
        payload = response.to_object()
        for resource in payload.get("resources", []):
            name = resource.get("resource_name")
            if resource.get("error_code", 0):
                parsed[name] = {}
                continue
            entries: dict[str, str | None] = {}
            for entry in resource.get("config_entries", []):
                key = entry.get("config_name", entry.get("config_names"))
                entries[key] = entry.get("config_value")
            parsed[name] = entries
    return parsed
