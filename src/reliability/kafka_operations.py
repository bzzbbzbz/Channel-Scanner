"""Bounded content-free Kafka operations metadata probe."""

from __future__ import annotations

import asyncio
from contextlib import suppress
import time
from typing import Any, Callable

from aiokafka import AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.admin.config_resource import ConfigResource, ConfigResourceType
from aiokafka.errors import for_code
from aiokafka.structs import TopicPartition

from src.config.settings import KafkaSettings
from src.reliability.kafka_admin import expected_topics
from src.reliability.kafka_consumer import DELIVERY_CONSUMER_GROUP, DIGEST_CONSUMER_GROUP
from src.reliability.contracts import DIGEST_RUN_REQUESTED_TOPIC, TELEGRAM_DELIVERY_REQUESTED_TOPIC

_GROUP_TOPICS = (
    (DIGEST_CONSUMER_GROUP, DIGEST_RUN_REQUESTED_TOPIC),
    (DELIVERY_CONSUMER_GROUP, TELEGRAM_DELIVERY_REQUESTED_TOPIC),
)


def _new_admin(settings: KafkaSettings) -> AIOKafkaAdminClient:
    return AIOKafkaAdminClient(
        bootstrap_servers=settings.bootstrap_servers,
        security_protocol=settings.security_protocol,
        client_id=f"{settings.client_id_prefix}-operations-probe",
        request_timeout_ms=settings.request_timeout_ms,
    )


def _new_offset_consumer(settings: KafkaSettings) -> AIOKafkaConsumer:
    return AIOKafkaConsumer(
        bootstrap_servers=settings.bootstrap_servers,
        security_protocol=settings.security_protocol,
        client_id=f"{settings.client_id_prefix}-operations-offsets",
        group_id=None,
        enable_auto_commit=False,
        request_timeout_ms=settings.request_timeout_ms,
    )


class KafkaOperationsProbe:
    """Read broker, fixed-topic, and fixed-group state under one hard deadline."""

    def __init__(
        self,
        settings: KafkaSettings,
        *,
        timeout_seconds: float = 5.0,
        cleanup_timeout_seconds: float = 1.0,
        admin_factory: Callable[[KafkaSettings], Any] = _new_admin,
        consumer_factory: Callable[[KafkaSettings], Any] = _new_offset_consumer,
    ) -> None:
        self._settings = settings
        self._timeout_seconds = timeout_seconds
        self._cleanup_timeout_seconds = cleanup_timeout_seconds
        self._admin_factory = admin_factory
        self._consumer_factory = consumer_factory
        self._active_task: asyncio.Task | None = None
        self._cleanup_tasks: set[asyncio.Task] = set()
        self._task_lock = asyncio.Lock()

    async def probe(self) -> dict[str, Any]:
        if not self._settings.enabled:
            return unavailable_kafka_report("disabled", "KafkaDisabled", self._settings)

        async with self._task_lock:
            if self._cleanup_tasks:
                return unavailable_kafka_report("unavailable", "ProbeBusy", self._settings)
            if self._active_task is not None and not self._active_task.done():
                return unavailable_kafka_report("unavailable", "ProbeBusy", self._settings)
            if self._active_task is not None:
                _consume_task_result(self._active_task)
            task = asyncio.create_task(self._probe_once(), name="kafka-operations-probe")
            task.add_done_callback(self._active_done)
            self._active_task = task
        try:
            done, _ = await asyncio.wait({task}, timeout=self._timeout_seconds)
            if not done:
                task.cancel()
                return unavailable_kafka_report("unavailable", "ProbeTimeout", self._settings)
            return task.result()
        except asyncio.CancelledError:
            task.cancel()
            raise
        except Exception as exc:
            return unavailable_kafka_report("unavailable", type(exc).__name__[:128], self._settings)
        finally:
            if task.done():
                async with self._task_lock:
                    if self._active_task is task:
                        self._active_task = None

    async def _probe_once(self) -> dict[str, Any]:
        admin = self._admin_factory(self._settings)
        try:
            await admin.start()
            started = time.monotonic()
            await admin.describe_cluster()
            latency_ms = max(0, round((time.monotonic() - started) * 1000))
            specs = expected_topics(self._settings)
            topic_metadata = await admin.describe_topics([spec.name for spec in specs])
            topic_configs = None
            topic_config_error = None
            topic_config_errors: dict[str, str] = {}
            try:
                config_keys = sorted({key for spec in specs for key in spec.configs})
                topic_configs, topic_config_errors = _map_topic_configs(
                    await admin.describe_configs(
                        [
                            ConfigResource(
                                ConfigResourceType.TOPIC,
                                spec.name,
                                {key: None for key in config_keys},
                            )
                            for spec in specs
                        ]
                    )
                )
            except Exception as exc:
                topic_config_error = type(exc).__name__[:128]
            topics = _map_topics(
                self._settings,
                topic_metadata,
                topic_configs,
                topic_config_error,
                topic_config_errors,
            )
            groups = await self._groups(admin, topics)
            return {
                "broker": {"status": "available", "latency_ms": latency_ms, "error_code": None},
                "topics": topics,
                "consumer_groups": groups,
            }
        finally:
            await self._bounded_cleanup(admin.close)

    async def _groups(self, admin: Any, topics: list[dict[str, Any]]) -> list[dict[str, Any]]:
        try:
            listed = {str(item[0]) for item in await admin.list_consumer_groups()}
        except Exception as exc:
            return _unavailable_groups(type(exc).__name__[:128])

        present = [group for group, _ in _GROUP_TOPICS if group in listed]
        descriptions: dict[str, dict[str, Any]] = {}
        if present:
            try:
                responses = await admin.describe_consumer_groups(present)
                descriptions = _map_group_descriptions(responses)
            except Exception as exc:
                code = type(exc).__name__[:128]
                return [
                    _group_result(group, topic, "unavailable", None, code)
                    if group in listed else _group_result(group, topic, "inactive", None, None)
                    for group, topic in _GROUP_TOPICS
                ]

        lag_results = await self._group_lags(admin, present, topics)
        results = []
        for group, topic in _GROUP_TOPICS:
            if group not in listed:
                results.append(_group_result(group, topic, "inactive", None, None))
                continue
            description = descriptions.get(group, {})
            description_error = description.get("error_code")
            if description_error:
                results.append(_group_result(group, topic, "unavailable", None, description_error))
                continue
            state = str(description.get("state", "")).lower()
            member_count = len(description.get("members") or ())
            active = member_count > 0 and state not in {"dead", "empty"}
            lag, lag_error = lag_results.get(group, (None, "GroupOffsetsUnavailable"))
            status = "active" if active else "inactive"
            if lag_error and lag_error != "OffsetNotInitialized":
                status = "unavailable"
            elif lag_error == "OffsetNotInitialized" and active:
                status = "unavailable"
            results.append(_group_result(group, topic, status, lag, lag_error))
        return results

    async def _group_lags(
        self,
        admin: Any,
        groups: list[str],
        topics: list[dict[str, Any]],
    ) -> dict[str, tuple[int | None, str | None]]:
        if not groups:
            return {}
        partitions = [
            TopicPartition(topic["name"], partition)
            for topic in topics
            if topic["name"] in {value for _, value in _GROUP_TOPICS} and topic["status"] == "available"
            for partition in range(topic["partitions"])
        ]
        if not partitions:
            return {group: (None, "TopicMetadataUnavailable") for group in groups}

        consumer = None
        try:
            consumer = self._consumer_factory(self._settings)
            await consumer.start()
            end_offsets = await consumer.end_offsets(partitions)
        except Exception as exc:
            return {group: (None, type(exc).__name__[:128]) for group in groups}
        finally:
            if consumer is not None:
                await self._bounded_cleanup(consumer.stop)

        result: dict[str, tuple[int | None, str | None]] = {}
        topic_by_group = dict(_GROUP_TOPICS)
        for group in groups:
            try:
                committed = await admin.list_consumer_group_offsets(group)
                topic = topic_by_group[group]
                target_partitions = [partition for partition in partitions if partition.topic == topic]
                if not target_partitions or any(partition not in end_offsets for partition in target_partitions):
                    result[group] = (None, "EndOffsetUnavailable")
                    continue
                lag = 0
                for partition in target_partitions:
                    offset_value = committed.get(partition)
                    offset = getattr(offset_value, "offset", offset_value)
                    if offset is None or int(offset) < 0:
                        lag = -1
                        break
                    lag += max(0, int(end_offsets[partition]) - int(offset))
                result[group] = (lag, None) if lag >= 0 else (None, "OffsetNotInitialized")
            except Exception as exc:
                result[group] = (None, type(exc).__name__[:128])
        return result

    async def _bounded_cleanup(self, close: Callable[[], Any]) -> None:
        task = asyncio.create_task(close(), name="kafka-operations-cleanup")
        self._cleanup_tasks.add(task)
        task.add_done_callback(self._cleanup_done)
        try:
            done, _ = await asyncio.wait({task}, timeout=self._cleanup_timeout_seconds)
        except asyncio.CancelledError:
            task.cancel()
            raise
        if not done:
            task.cancel()

    def _active_done(self, task: asyncio.Task) -> None:
        _consume_task_result(task)
        if self._active_task is task:
            self._active_task = None

    def _cleanup_done(self, task: asyncio.Task) -> None:
        _consume_task_result(task)
        self._cleanup_tasks.discard(task)


def unavailable_kafka_report(
    status: str,
    error_code: str,
    settings: KafkaSettings | None = None,
) -> dict[str, Any]:
    settings = settings or KafkaSettings()
    return {
        "broker": {"status": status, "latency_ms": None, "error_code": error_code},
        "topics": [
            {
                "name": spec.name,
                "status": "unavailable",
                "partitions": None,
                "replication_factor": None,
                "drift": None,
                "error_code": error_code,
            }
            for spec in expected_topics(settings)
        ],
        "consumer_groups": _unavailable_groups(error_code),
    }


def _map_topics(
    settings: KafkaSettings,
    metadata: list[dict[str, Any]],
    topic_configs: dict[str, dict[str, str | None]] | None,
    config_error: str | None = None,
    config_errors: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    by_name = {str(item.get("topic", item.get("name"))): item for item in metadata}
    results = []
    for spec in expected_topics(settings):
        item = by_name.get(spec.name)
        if item is None or item.get("error_code", 0):
            results.append(
                {
                    "name": spec.name,
                    "status": "missing",
                    "partitions": None,
                    "replication_factor": None,
                    "drift": None,
                    "error_code": "TopicMetadataUnavailable",
                }
            )
            continue
        partitions = item.get("partitions") or []
        replica_counts = {len(partition.get("replicas") or ()) for partition in partitions}
        replication_factor = next(iter(replica_counts)) if len(replica_counts) == 1 else None
        metadata_drift = len(partitions) != spec.partitions or replica_counts != {spec.replication_factor}
        actual_configs = topic_configs.get(spec.name) if topic_configs is not None else None
        resource_config_error = (config_errors or {}).get(spec.name)
        effective_config_error = resource_config_error or config_error
        drift = (
            True
            if metadata_drift
            else (
                None
                if actual_configs is None
                else any(actual_configs.get(key) != expected for key, expected in spec.configs.items())
            )
        )
        results.append(
            {
                "name": spec.name,
                "status": "degraded" if effective_config_error else "available",
                "partitions": len(partitions),
                "replication_factor": replication_factor,
                "drift": drift,
                "error_code": effective_config_error,
            }
        )
    return results


def _map_topic_configs(responses: list[Any]) -> tuple[dict[str, dict[str, str | None]], dict[str, str]]:
    mapped: dict[str, dict[str, str | None]] = {}
    errors: dict[str, str] = {}
    for response in responses:
        payload = response.to_object() if hasattr(response, "to_object") else response
        for resource in payload.get("resources", []):
            name = str(resource.get("resource_name"))
            error_code = int(resource.get("error_code", 0) or 0)
            if error_code:
                errors[name] = _broker_error_code(error_code)
                continue
            mapped[name] = {
                str(entry.get("config_name", entry.get("config_names"))): entry.get("config_value")
                for entry in resource.get("config_entries", [])
            }
    return mapped, errors


def _map_group_descriptions(responses: list[Any]) -> dict[str, dict[str, Any]]:
    mapped: dict[str, dict[str, Any]] = {}
    for response in responses:
        payload = response.to_object() if hasattr(response, "to_object") else response
        for group in payload.get("groups", []):
            error_code = int(group.get("error_code", 0) or 0)
            mapped[str(group.get("group"))] = {
                **group,
                "error_code": _broker_error_code(error_code) if error_code else None,
            }
    return mapped


def _broker_error_code(error_code: int) -> str:
    return for_code(error_code).__name__[:128]


def _group_result(group: str, topic: str, status: str, lag: int | None, error_code: str | None) -> dict[str, Any]:
    return {"group_id": group, "topic": topic, "status": status, "lag": lag, "error_code": error_code}


def _unavailable_groups(error_code: str) -> list[dict[str, Any]]:
    return [_group_result(group, topic, "unavailable", None, error_code) for group, topic in _GROUP_TOPICS]


def _consume_task_result(task: asyncio.Task) -> None:
    with suppress(asyncio.CancelledError, Exception):
        task.result()
