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
    return latest_due_slot(subscription, user, now) is not None


def latest_due_slot(subscription: Subscription, user: User, now: datetime) -> datetime | None:
    """Return only the latest elapsed logical slot, never historical catch-up slots."""
    now = _as_utc(now)
    user_tz = _timezone_info(user.timezone)
    now_local = now.astimezone(user_tz)
    created_at = _as_utc(subscription.created_at) if subscription.created_at is not None else None
    last_digest_at = _as_utc(subscription.last_digest_at) if subscription.last_digest_at is not None else None

    if subscription.notification_cron:
        local_minute = now_local.replace(second=0, microsecond=0, tzinfo=None)
        if croniter.match(subscription.notification_cron, local_minute):
            candidate_local = local_minute
        else:
            candidate_local = croniter(subscription.notification_cron, local_minute).get_prev(datetime)
        candidate = candidate_local.replace(tzinfo=user_tz).astimezone(timezone.utc)
        if created_at is not None and candidate < created_at:
            return None
    elif last_digest_at is None:
        # Preserve legacy immediate first-run behavior while using the stable
        # creation timestamp as the idempotency slot.
        return created_at if created_at is not None and created_at <= now else now
    elif subscription.frequency == DeliveryFrequency.HOURLY:
        candidate = now_local.replace(minute=0, second=0, microsecond=0).astimezone(timezone.utc)
    else:
        candidate = now_local.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc)

    if last_digest_at is not None and candidate <= last_digest_at:
        return None
    return candidate


def next_digest_at(subscription: Subscription, user: User, now: datetime) -> datetime | None:
    if not subscription.enabled:
        return None
    if latest_due_slot(subscription, user, now) is not None:
        return now
    if not subscription.notification_cron:
        return _next_legacy_frequency_at(subscription, user, now)

    user_tz = _timezone_info(user.timezone)
    now_local = now.astimezone(user_tz).replace(tzinfo=None)
    next_local = croniter(subscription.notification_cron, now_local).get_next(datetime)
    return next_local.replace(tzinfo=user_tz).astimezone(timezone.utc)


def _is_legacy_frequency_due(subscription: Subscription, user: User, now: datetime) -> bool:
    return latest_due_slot(subscription, user, now) is not None


def _next_legacy_frequency_at(subscription: Subscription, user: User, now: datetime) -> datetime:
    if subscription.last_digest_at is None:
        return now

    user_tz = _timezone_info(user.timezone)
    now_local = now.astimezone(user_tz)
    last_local = subscription.last_digest_at.astimezone(user_tz)

    if subscription.frequency == DeliveryFrequency.HOURLY:
        next_local = last_local.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    else:
        next_local = last_local.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)

    if next_local <= now_local:
        return now
    return next_local.astimezone(timezone.utc)


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


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
