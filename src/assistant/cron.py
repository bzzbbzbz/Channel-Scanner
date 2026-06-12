"""Cron helpers for subscription notification schedules."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from croniter import croniter

from src.models.subscription import Subscription
from src.models.user import DeliveryFrequency, User

MIN_NOTIFICATION_INTERVAL = timedelta(minutes=15)
HOURLY_CRON = "0 * * * *"
DAILY_CRON = "0 10 * * *"


class InvalidCronError(ValueError):
    """Raised when an assistant-provided cron schedule is invalid or unsafe."""


@dataclass(slots=True)
class CronDescription:
    cron: str
    ru: str
    en: str


def cron_for_frequency(frequency: DeliveryFrequency) -> str:
    return HOURLY_CRON if frequency == DeliveryFrequency.HOURLY else DAILY_CRON


def validate_notification_cron(expression: str) -> str:
    cron = " ".join((expression or "").strip().split())
    if len(cron.split()) != 5:
        raise InvalidCronError("Use a standard 5-field cron expression: minute hour day month weekday.")
    if not croniter.is_valid(cron):
        raise InvalidCronError("Invalid cron expression.")

    base = datetime(2026, 1, 1, 0, 0)
    iterator = croniter(cron, base)
    previous = iterator.get_next(datetime)
    for _ in range(200):
        current = iterator.get_next(datetime)
        if current - previous < MIN_NOTIFICATION_INTERVAL:
            raise InvalidCronError("Notification interval must be at least 15 minutes.")
        previous = current
    return cron


def describe_cron(expression: str) -> CronDescription:
    cron = validate_notification_cron(expression)
    parts = cron.split()
    minute, hour, day, month, weekday = parts

    if cron == HOURLY_CRON:
        return CronDescription(cron=cron, ru="каждый час", en="hourly")
    if day == month == weekday == "*" and hour.isdigit() and minute.isdigit():
        return CronDescription(cron=cron, ru=f"каждый день в {int(hour):02d}:{int(minute):02d}", en=f"daily at {int(hour):02d}:{int(minute):02d}")
    if day == month == weekday == "*" and hour == "*" and minute.startswith("*/"):
        return CronDescription(cron=cron, ru=f"каждые {minute[2:]} минут", en=f"every {minute[2:]} minutes")
    if day == month == weekday == "*" and minute.isdigit() and hour.startswith("*/"):
        return CronDescription(cron=cron, ru=f"каждые {hour[2:]} часа", en=f"every {hour[2:]} hours")
    return CronDescription(cron=cron, ru=f"по расписанию `{cron}`", en=f"on schedule `{cron}`")


def is_cron_due(subscription: Subscription, user: User, now: datetime) -> bool:
    if not subscription.notification_cron:
        return _is_legacy_frequency_due(subscription, user, now)

    user_tz = _timezone_info(user.timezone)
    now_local = now.astimezone(user_tz).replace(tzinfo=None)
    anchor_source = subscription.last_digest_at or subscription.created_at
    if anchor_source is None:
        return croniter.match(subscription.notification_cron, now_local)
    anchor_local = anchor_source.astimezone(user_tz).replace(tzinfo=None)
    next_due = croniter(subscription.notification_cron, anchor_local).get_next(datetime)
    return next_due <= now_local


def _is_legacy_frequency_due(subscription: Subscription, user: User, now: datetime) -> bool:
    if subscription.last_digest_at is None:
        return True

    user_tz = _timezone_info(user.timezone)
    now_local = now.astimezone(user_tz)
    last_local = subscription.last_digest_at.astimezone(user_tz)

    if subscription.frequency == DeliveryFrequency.HOURLY:
        return now_local.strftime("%Y-%m-%d %H") != last_local.strftime("%Y-%m-%d %H")

    return now_local.date() != last_local.date()


def _timezone_info(timezone_name: str):
    if timezone_name == "UTC":
        return timezone.utc
    if timezone_name.startswith("UTC") and len(timezone_name) > 3:
        sign = timezone_name[3:4]
        hours_raw = timezone_name[4:]
        if sign in {"+", "-"} and hours_raw.isdigit():
            hours = int(hours_raw) * (1 if sign == "+" else -1)
            return timezone(timedelta(hours=hours))
    try:
        return ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        return timezone.utc
