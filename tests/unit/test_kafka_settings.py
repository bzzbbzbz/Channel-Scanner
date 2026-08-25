from __future__ import annotations

import textwrap

import pytest
from pydantic import ValidationError

from src.config.settings import KafkaSettings, Settings


_KAFKA_ENV_NAMES = (
    "KAFKA_ENABLED",
    "KAFKA_BOOTSTRAP_SERVERS",
    "KAFKA_SECURITY_PROTOCOL",
    "KAFKA_CLIENT_ID_PREFIX",
    "KAFKA_REQUEST_TIMEOUT_MS",
    "KAFKA_STARTUP_TIMEOUT_SECONDS",
    "KAFKA_TOPIC_PARTITIONS",
    "KAFKA_TOPIC_REPLICATION_FACTOR",
    "KAFKA_TOPIC_RETENTION_MS",
    "KAFKA_DLQ_RETENTION_MS",
    "KAFKA_TOPIC_RETENTION_BYTES",
    "KAFKA_DLQ_RETENTION_BYTES",
    "KAFKA_MAX_EVENT_BYTES",
    "KAFKA_OUTBOX_LEASE_SECONDS",
    "KAFKA_OUTBOX_PUBLISH_TIMEOUT_SECONDS",
    "KAFKA_OUTBOX_POLL_INTERVAL_SECONDS",
    "KAFKA_OUTBOX_BATCH_SIZE",
    "KAFKA_OUTBOX_BACKOFF_BASE_SECONDS",
    "KAFKA_OUTBOX_BACKOFF_CAP_SECONDS",
)


def _clear_kafka_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in _KAFKA_ENV_NAMES:
        monkeypatch.delenv(name, raising=False)


def test_kafka_settings_load_from_toml(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_kafka_env(monkeypatch)
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        textwrap.dedent(
            """
            [kafka]
            enabled = true
            bootstrap_servers = "broker-a:19092"
            security_protocol = "PLAINTEXT"
            client_id_prefix = "stage-one"
            request_timeout_ms = 12000
            startup_timeout_seconds = 75
            topic_partitions = 3
            topic_replication_factor = 1
            topic_retention_ms = 700000
            dlq_retention_ms = 2600000
            topic_retention_bytes = 10485760
            dlq_retention_bytes = 20971520
            max_event_bytes = 32768
            outbox_lease_seconds = 45
            outbox_publish_timeout_seconds = 12
            outbox_poll_interval_seconds = 0.5
            outbox_batch_size = 25
            outbox_backoff_base_seconds = 2
            outbox_backoff_cap_seconds = 90
            """
        ),
        encoding="utf-8",
    )

    settings = Settings.from_toml(config_path)

    assert settings.kafka.enabled is True
    assert settings.kafka.bootstrap_servers == "broker-a:19092"
    assert settings.kafka.client_id_prefix == "stage-one"
    assert settings.kafka.topic_partitions == 3
    assert settings.kafka.max_event_bytes == 32768
    assert settings.kafka.outbox_lease_seconds == 45
    assert settings.kafka.outbox_publish_timeout_seconds == 12
    assert settings.kafka.outbox_poll_interval_seconds == 0.5
    assert settings.kafka.outbox_batch_size == 25
    assert settings.kafka.outbox_backoff_base_seconds == 2
    assert settings.kafka.outbox_backoff_cap_seconds == 90


def test_kafka_environment_overrides_toml(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_kafka_env(monkeypatch)
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        "[kafka]\nenabled = false\nbootstrap_servers = \"from-toml:9092\"\ntopic_partitions = 1\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("KAFKA_ENABLED", "yes")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "from-env:9092")
    monkeypatch.setenv("KAFKA_TOPIC_PARTITIONS", "4")
    monkeypatch.setenv("KAFKA_OUTBOX_BATCH_SIZE", "17")
    monkeypatch.setenv("KAFKA_OUTBOX_PUBLISH_TIMEOUT_SECONDS", "8")

    settings = Settings.from_toml(config_path)

    assert settings.kafka.enabled is True
    assert settings.kafka.bootstrap_servers == "from-env:9092"
    assert settings.kafka.topic_partitions == 4
    assert settings.kafka.outbox_batch_size == 17
    assert settings.kafka.outbox_publish_timeout_seconds == 8


def test_kafka_settings_reject_unsafe_event_limit() -> None:
    with pytest.raises(ValidationError):
        KafkaSettings(max_event_bytes=1_000_000)


@pytest.mark.parametrize("publish_timeout", [30, 31])
def test_kafka_settings_require_publish_timeout_shorter_than_lease(publish_timeout: float) -> None:
    with pytest.raises(ValidationError, match="must be less than"):
        KafkaSettings(outbox_lease_seconds=30, outbox_publish_timeout_seconds=publish_timeout)
