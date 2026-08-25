from __future__ import annotations

import pytest
from pydantic import ValidationError

from src.config.settings import KafkaSettings, MemorySettings, ReliableDeliverySettings, Settings


def test_reliable_delivery_is_disabled_and_owns_nothing_by_default() -> None:
    settings = ReliableDeliverySettings()

    assert settings.enabled is False
    assert settings.owns_subscription(1) is False
    assert settings.render_memory_enabled is False


def test_allowlist_is_the_single_ownership_policy() -> None:
    settings = ReliableDeliverySettings(enabled=True, subscription_ids=[7])

    assert settings.owns_subscription(7) is True
    assert settings.owns_subscription(8) is False


def test_allowlisted_rollout_fails_closed_while_memory_is_enabled() -> None:
    with pytest.raises(ValidationError, match="memory.enabled=false"):
        Settings(
            kafka=KafkaSettings(enabled=True),
            memory=MemorySettings(enabled=True),
            reliable_delivery=ReliableDeliverySettings(enabled=True, subscription_ids=[1]),
        )


def test_reliable_delivery_requires_kafka_and_explicit_scope() -> None:
    with pytest.raises(ValidationError, match="subscription_ids"):
        ReliableDeliverySettings(enabled=True)
    with pytest.raises(ValidationError, match="kafka.enabled=true"):
        Settings(reliable_delivery=ReliableDeliverySettings(enabled=True, subscription_ids=[1]))


def test_inbox_lease_cannot_expire_before_render_lease() -> None:
    with pytest.raises(ValidationError, match="inbox_lease_seconds"):
        ReliableDeliverySettings(inbox_lease_seconds=10, render_lease_seconds=11)


def test_delivery_timeout_and_backoff_are_bounded_by_delivery_lease_and_cap() -> None:
    with pytest.raises(ValidationError, match="delivery_send_timeout_seconds"):
        ReliableDeliverySettings(delivery_lease_seconds=10, delivery_send_timeout_seconds=10)
    with pytest.raises(ValidationError, match="delivery_backoff_cap_seconds"):
        ReliableDeliverySettings(delivery_backoff_base_seconds=3, delivery_backoff_cap_seconds=2)


def test_delivery_settings_load_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RELIABLE_DIGEST_SUBSCRIPTION_IDS", "[3, 5]")
    monkeypatch.setenv("RELIABLE_DIGEST_DELIVERY_LEASE_SECONDS", "80")
    monkeypatch.setenv("RELIABLE_DIGEST_DELIVERY_SEND_TIMEOUT_SECONDS", "25")
    monkeypatch.setenv("RELIABLE_DIGEST_DELIVERY_MAX_ATTEMPTS", "7")
    monkeypatch.setenv("RELIABLE_DIGEST_DELIVERY_BACKOFF_BASE_SECONDS", "3")
    monkeypatch.setenv("RELIABLE_DIGEST_DELIVERY_BACKOFF_CAP_SECONDS", "120")

    settings = ReliableDeliverySettings()

    assert settings.delivery_lease_seconds == 80
    assert settings.delivery_send_timeout_seconds == 25
    assert settings.delivery_max_attempts == 7
    assert settings.delivery_backoff_base_seconds == 3
    assert settings.delivery_backoff_cap_seconds == 120
    assert settings.subscription_ids == [3, 5]


def test_empty_json_subscription_allowlist_loads_from_toml_environment(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    monkeypatch.setenv("RELIABLE_DIGEST_SUBSCRIPTION_IDS", "[]")

    settings = Settings.from_toml(tmp_path / "missing.toml")

    assert settings.reliable_delivery.subscription_ids == []


@pytest.mark.parametrize("value", ('[1.9]', '[true]', '["7"]'))
def test_subscription_allowlist_rejects_non_integer_json_values(
    monkeypatch: pytest.MonkeyPatch, tmp_path, value: str
) -> None:
    monkeypatch.setenv("RELIABLE_DIGEST_SUBSCRIPTION_IDS", value)

    with pytest.raises(ValueError, match="only integers"):
        Settings.from_toml(tmp_path / "missing.toml")
