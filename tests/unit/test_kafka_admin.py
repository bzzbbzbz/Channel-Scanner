from __future__ import annotations

from types import SimpleNamespace

import pytest

from src.config.settings import KafkaSettings
from src.reliability.kafka_admin import (
    TopicConfigurationError,
    check_topics,
    ensure_topics,
    expected_topics,
)


class _ConfigResponse:
    def __init__(self, resources):
        self._resources = resources

    def to_object(self):
        return {"resources": self._resources}


class _FakeAdmin:
    def __init__(self, settings: KafkaSettings, existing: bool = True):
        self.specs = {spec.name: spec for spec in expected_topics(settings)}
        self.topics = set(self.specs) if existing else set()
        self.configs = {name: dict(spec.configs) for name, spec in self.specs.items()}
        self.created: list[str] = []
        self.closed = False

    async def start(self):
        return None

    async def close(self):
        self.closed = True

    async def list_topics(self):
        return sorted(self.topics)

    async def create_topics(self, topics, timeout_ms):
        for topic in topics:
            self.topics.add(topic.name)
            self.created.append(topic.name)
            self.configs[topic.name] = dict(topic.topic_configs)
        return SimpleNamespace(topic_errors=[(name, 0, None) for name in self.created])

    async def describe_topics(self, names):
        result = []
        for name in names:
            if name not in self.topics:
                continue
            spec = self.specs[name]
            result.append(
                {
                    "topic": name,
                    "error_code": 0,
                    "partitions": [
                        {"partition": index, "replicas": list(range(spec.replication_factor))}
                        for index in range(spec.partitions)
                    ],
                }
            )
        return result

    async def describe_cluster(self):
        return {"controller_id": 1}

    async def describe_configs(self, resources):
        payload = []
        for resource in resources:
            payload.append(
                {
                    "error_code": 0,
                    "resource_name": resource.name,
                    "config_entries": [
                        {"config_names": key, "config_value": value}
                        for key, value in self.configs[resource.name].items()
                    ],
                }
            )
        return [_ConfigResponse(payload)]


@pytest.mark.asyncio
async def test_ensure_topics_creates_all_missing_topics_and_checks_configuration() -> None:
    settings = KafkaSettings()
    fake = _FakeAdmin(settings, existing=False)

    report = await ensure_topics(settings, admin_factory=lambda *_: fake)

    assert report.ok
    assert set(fake.created) == {spec.name for spec in expected_topics(settings)}
    assert fake.closed is True


@pytest.mark.asyncio
async def test_check_topics_reports_configuration_drift_without_mutating() -> None:
    settings = KafkaSettings()
    fake = _FakeAdmin(settings)
    first_topic = expected_topics(settings)[0].name
    fake.configs[first_topic]["retention.ms"] = "1"

    report = await check_topics(settings, admin_factory=lambda *_: fake)

    assert report.ok is False
    assert any("retention.ms='1'" in mismatch for mismatch in report.mismatches)
    assert fake.created == []


@pytest.mark.asyncio
async def test_ensure_topics_fails_on_existing_configuration_drift() -> None:
    settings = KafkaSettings()
    fake = _FakeAdmin(settings)
    first_topic = expected_topics(settings)[0].name
    fake.configs[first_topic]["cleanup.policy"] = "compact"

    with pytest.raises(TopicConfigurationError, match="cleanup.policy"):
        await ensure_topics(settings, admin_factory=lambda *_: fake)
