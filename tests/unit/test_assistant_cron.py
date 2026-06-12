"""Tests for assistant cron scheduling helpers."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.assistant.cron import InvalidCronError, describe_cron, is_cron_due, validate_notification_cron
from src.models.subscription import Subscription
from src.models.user import DeliveryFrequency, DigestFormat, SummaryMode, User


def _user() -> User:
    return User(telegram_user_id=1, chat_id=2, chat_type="private", timezone="UTC", language="ru")


def _subscription(**overrides: object) -> Subscription:
    params = {
        "user_id": 1,
        "name": "AI",
        "digest_format": DigestFormat.SHORT,
        "summary_mode": SummaryMode.BRIEF,
        "frequency": DeliveryFrequency.DAILY,
        "enabled": True,
        "created_at": datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc),
    }
    params.update(overrides)
    return Subscription(**params)


def test_validate_notification_cron_rejects_too_frequent_schedule() -> None:
    with pytest.raises(InvalidCronError):
        validate_notification_cron("*/10 * * * *")


def test_validate_notification_cron_accepts_15_minute_schedule() -> None:
    assert validate_notification_cron("*/15 * * * *") == "*/15 * * * *"


def test_describe_cron_formats_daily_time() -> None:
    description = describe_cron("30 9 * * *")
    assert description.ru == "каждый день в 09:30"


def test_describe_cron_formats_interval_hours_without_crashing() -> None:
    description = describe_cron("0 */3 * * *")
    assert description.ru == "каждые 3 часа"
    assert description.en == "every 3 hours"


def test_is_cron_due_uses_notification_cron_when_present() -> None:
    subscription = _subscription(
        notification_cron="0 10 * * *",
        last_digest_at=datetime(2026, 1, 1, 9, 0, tzinfo=timezone.utc),
    )

    assert is_cron_due(subscription, _user(), datetime(2026, 1, 1, 9, 55, tzinfo=timezone.utc)) is False
    assert is_cron_due(subscription, _user(), datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)) is True


def test_is_cron_due_falls_back_to_legacy_frequency_without_cron() -> None:
    subscription = _subscription(
        frequency=DeliveryFrequency.HOURLY,
        last_digest_at=datetime(2026, 1, 1, 10, 10, tzinfo=timezone.utc),
    )

    assert is_cron_due(subscription, _user(), datetime(2026, 1, 1, 10, 50, tzinfo=timezone.utc)) is False
    assert is_cron_due(subscription, _user(), datetime(2026, 1, 1, 11, 0, tzinfo=timezone.utc)) is True
