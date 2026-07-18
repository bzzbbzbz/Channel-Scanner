"""Bot domain helpers and service layer."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from html import escape
import logging
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from sqlalchemy.ext.asyncio import async_sessionmaker

from src.assistant.cron import cron_for_frequency, describe_cron, next_digest_at, validate_notification_cron
from src.config.settings import BotSettings
from src.models.channel import Channel
from src.models.subscription import Subscription
from src.models.user import DeliveryFrequency, DigestFormat, SummaryMode, User
from src.prompt_defaults import default_filter_task_prompt, default_summary_task_prompt
from src.repository.channel import ChannelRepository
from src.repository.digest_delivery import DigestDeliveryRepository, DigestProcessingStats
from src.repository.subscription import SubscriptionRepository
from src.repository.user import UserRepository
from src.scraper.client import ChannelNotFoundError, TelegramClient

_CHANNEL_RE = re.compile(r"^(?:@|(?:https?://)?t\.me/)?([A-Za-z][A-Za-z0-9_]{3,31})/?$")
_UTC_OFFSET_RE = re.compile(r"^(?:UTC)?([+-])(0|[1-9]|1[0-4])$", re.IGNORECASE)
logger = logging.getLogger(__name__)

SUPPORTED_LANGUAGES = {"ru", "en"}
PRESET_NOTIFICATION_CRON = "0 */4 * * *"


class InvalidChannelReferenceError(ValueError):
    """Raised when a user-provided channel reference cannot be normalized."""


class InvalidTimezoneError(ValueError):
    """Raised when a timezone string is not a supported timezone."""


class ProductLimitExceededError(ValueError):
    """Raised when a configured product limit would be exceeded."""

    def __init__(self, code: str, limit: int) -> None:
        super().__init__(code)
        self.code = code
        self.limit = limit


@dataclass(slots=True)
class BulkSubscribeResult:
    added: list[str]
    already_subscribed: list[str]
    invalid: list[str]
    not_found: list[str]
    limit_exceeded: list[str]


@dataclass(slots=True)
class BulkUnsubscribeResult:
    removed: list[str]
    not_subscribed: list[str]
    invalid: list[str]


@dataclass(frozen=True, slots=True)
class ChannelPreset:
    id: str
    name: str
    channels: tuple[str, ...]


@dataclass(slots=True)
class PresetCreateResult:
    subscription: Subscription | None
    added: list[str]
    not_found: list[str]
    limit_exceeded: list[str]


CHANNEL_PRESETS: tuple[ChannelPreset, ...] = (
    ChannelPreset(
        id="news",
        name="Новости",
        channels=("rbc_news", "kommersant", "tass_agency", "rian_ru"),
    ),
    ChannelPreset(
        id="ai",
        name="AI",
        channels=(
            "aiwizards",
            "automatisator",
            "dealerAI",
            "max_about_ai",
            "nobilix",
            "oestick",
            "silent_ai_cto",
            "turboproject",
            "neuraldeep",
        ),
    ),
)


@dataclass(slots=True)
class TelegramIdentity:
    telegram_user_id: int
    chat_id: int
    chat_type: str
    username: str | None
    first_name: str | None
    last_name: str | None
    language_code: str | None


def normalize_channel_reference(raw_value: str) -> str:
    candidate = raw_value.strip()
    match = _CHANNEL_RE.fullmatch(candidate)
    if match is None:
        raise InvalidChannelReferenceError(
            "Send a public channel username like @durov or https://t.me/durov."
        )
    return match.group(1)


def split_channel_references(raw_value: str) -> list[str]:
    candidates = re.split(r"[\n,]+", raw_value)
    return [candidate.strip() for candidate in candidates if candidate.strip()]


def list_channel_presets() -> tuple[ChannelPreset, ...]:
    return CHANNEL_PRESETS


def get_channel_preset(preset_id: str) -> ChannelPreset | None:
    return next((preset for preset in CHANNEL_PRESETS if preset.id == preset_id), None)


def unique_subscription_name(base_name: str, existing_names: set[str]) -> str:
    if base_name not in existing_names:
        return base_name
    suffix = 2
    while f"{base_name} {suffix}" in existing_names:
        suffix += 1
    return f"{base_name} {suffix}"


def normalize_language(language_code: str | None) -> str:
    if not language_code:
        return "ru"
    primary = language_code.strip().lower().split("-", 1)[0]
    return primary if primary in SUPPORTED_LANGUAGES else "ru"


def normalize_timezone(raw_value: str) -> str:
    candidate = raw_value.strip()
    if candidate.upper() == "UTC":
        return "UTC"
    offset_match = _UTC_OFFSET_RE.fullmatch(candidate)
    if offset_match is not None:
        sign, hours = offset_match.groups()
        return f"UTC{sign}{int(hours)}"
    try:
        ZoneInfo(candidate)
    except ZoneInfoNotFoundError as exc:
        raise InvalidTimezoneError(
            "Send a timezone like UTC, Europe/Berlin, Asia/Tbilisi, UTC+3, or -5."
        ) from exc
    return candidate


def timezone_info(timezone_name: str):
    normalized = normalize_timezone(timezone_name)
    if normalized == "UTC":
        return timezone.utc

    offset_match = _UTC_OFFSET_RE.fullmatch(normalized)
    if offset_match is not None:
        sign, hours = offset_match.groups()
        offset_hours = int(hours) * (1 if sign == "+" else -1)
        return timezone(timedelta(hours=offset_hours))

    return ZoneInfo(normalized)


def _summary_mode_label(summary_mode: SummaryMode, language: str) -> str:
    if language == "ru":
        return {
            SummaryMode.BRIEF: "Кратко",
            SummaryMode.DETAILED: "Подробно",
            SummaryMode.CUSTOM: "Свой вариант",
        }[summary_mode]
    return {
        SummaryMode.BRIEF: "Brief",
        SummaryMode.DETAILED: "Detailed",
        SummaryMode.CUSTOM: "Custom",
    }[summary_mode]


def digest_format_label(subscription: Subscription, language: str) -> str:
    if subscription.digest_format == DigestFormat.SHORT:
        return "200 символов" if language == "ru" else "200 chars"
    summary_type = _summary_mode_label(subscription.summary_mode, language)
    return (
        f"Пересказ ({summary_type})"
        if language == "ru"
        else f"Summary ({summary_type})"
    )


def frequency_label(frequency: DeliveryFrequency, language: str) -> str:
    if language == "ru":
        return "каждый час" if frequency == DeliveryFrequency.HOURLY else "раз в день"
    return "hourly" if frequency == DeliveryFrequency.HOURLY else "daily"


def notification_schedule_label(subscription: Subscription, language: str) -> str:
    if not subscription.notification_cron:
        return frequency_label(subscription.frequency, language)
    description = describe_cron(subscription.notification_cron)
    return description.ru if language == "ru" else description.en


def subscription_button_label(subscription: Subscription, user: User, now: datetime | None = None) -> str:
    current_time = now or datetime.now(timezone.utc)
    state = "✅" if subscription.enabled else "⏸"
    label = f"{state} {subscription.name}"
    next_at = next_digest_at(subscription, user, current_time)
    if next_at is None:
        return label
    return f"{label} · {_countdown_label(next_at, current_time, user.language)}"


def format_digest_processing_stats(
    subscription: Subscription,
    user: User,
    stats: DigestProcessingStats,
    period_start: datetime,
    period_end: datetime,
) -> str:
    """Render a Telegram-safe aggregate processing report for one subscription."""
    user_tz = timezone_info(user.timezone)
    start_label = period_start.astimezone(user_tz).strftime("%Y-%m-%d %H:%M")
    end_label = period_end.astimezone(user_tz).strftime("%Y-%m-%d %H:%M")
    name = escape(subscription.name)
    if user.language == "ru":
        lines = [
            "<b>Обработка за последние 24 часа</b>",
            f"Подписка: <b>{name}</b>",
            f"Период: {start_label} - {end_label}",
            "",
            f"Найдено новых постов: {stats.found_count}",
            f"Отфильтровано: {stats.filtered_count}",
            f"Включено в дайджест: {stats.included_count}",
        ]
        if not stats.run_count:
            lines.append("\nЗа этот период обработок ещё не было.")
        return "\n".join(lines)
    lines = [
        "<b>Processing in the last 24 hours</b>",
        f"Subscription: <b>{name}</b>",
        f"Period: {start_label} - {end_label}",
        "",
        f"New posts found: {stats.found_count}",
        f"Filtered out: {stats.filtered_count}",
        f"Included in digest: {stats.included_count}",
    ]
    if not stats.run_count:
        lines.append("\nNo processing has been recorded for this period yet.")
    return "\n".join(lines)


def _countdown_label(target_at: datetime, now: datetime, language: str) -> str:
    total_minutes = max(0, int((target_at - now).total_seconds() // 60))
    days, remainder = divmod(total_minutes, 24 * 60)
    hours, minutes = divmod(remainder, 60)

    parts: list[str] = []
    if days:
        parts.append(f"{days}д" if language == "ru" else f"{days}d")
    if hours or days:
        parts.append(f"{hours}ч" if language == "ru" else f"{hours}h")
    if not days:
        parts.append(f"{minutes}м" if language == "ru" else f"{minutes}m")
    prefix = "через" if language == "ru" else "in"
    return f"{prefix} {' '.join(parts)}"


class BotService:
    """Persist bot-facing user and subscription operations."""

    def __init__(
        self,
        session_factory: async_sessionmaker,
        scraper_client: TelegramClient,
        bot_settings: BotSettings,
    ) -> None:
        self._session_factory = session_factory
        self._scraper_client = scraper_client
        self._bot_settings = bot_settings

    @property
    def max_channels_per_subscription(self) -> int:
        return self._bot_settings.max_channels_per_subscription

    @property
    def max_subscriptions_per_user(self) -> int:
        return self._bot_settings.max_subscriptions_per_user

    async def ensure_user(self, identity: TelegramIdentity) -> User:
        async with self._session_factory() as session:
            user_repo = UserRepository(session)
            user = await user_repo.upsert_user(
                telegram_user_id=identity.telegram_user_id,
                chat_id=identity.chat_id,
                chat_type=identity.chat_type,
                username=identity.username,
                first_name=identity.first_name,
                last_name=identity.last_name,
                default_timezone=self._bot_settings.default_timezone,
                default_language=normalize_language(identity.language_code),
            )
            await session.commit()
            return user

    async def get_user(self, telegram_user_id: int) -> User | None:
        async with self._session_factory() as session:
            return await UserRepository(session).get_by_telegram_user_id(telegram_user_id)

    async def update_timezone(self, telegram_user_id: int, timezone_name: str) -> User:
        normalized = normalize_timezone(timezone_name)
        async with self._session_factory() as session:
            user_repo = UserRepository(session)
            user = await user_repo.get_by_telegram_user_id(telegram_user_id)
            if user is None:
                raise LookupError("User not found")
            await user_repo.update_timezone(user, normalized)
            await session.commit()
            return user

    async def update_language(self, telegram_user_id: int, language: str) -> User:
        normalized = normalize_language(language)
        async with self._session_factory() as session:
            user_repo = UserRepository(session)
            user = await user_repo.get_by_telegram_user_id(telegram_user_id)
            if user is None:
                raise LookupError("User not found")
            await user_repo.update_language(user, normalized)
            await session.commit()
            return user

    async def list_subscriptions(self, telegram_user_id: int) -> list[Subscription]:
        async with self._session_factory() as session:
            user = await UserRepository(session).get_by_telegram_user_id(telegram_user_id)
            if user is None:
                return []
            return await SubscriptionRepository(session).list_for_user(user.id)

    async def get_subscription(self, telegram_user_id: int, subscription_id: int) -> Subscription | None:
        async with self._session_factory() as session:
            user = await UserRepository(session).get_by_telegram_user_id(telegram_user_id)
            if user is None:
                return None
            return await SubscriptionRepository(session).get_for_user(user.id, subscription_id)

    async def get_digest_processing_stats(
        self,
        telegram_user_id: int,
        subscription_id: int,
        period_start: datetime,
        period_end: datetime,
    ) -> DigestProcessingStats:
        async with self._session_factory() as session:
            user = await UserRepository(session).get_by_telegram_user_id(telegram_user_id)
            if user is None:
                raise LookupError("User not found")
            subscription = await SubscriptionRepository(session).get_for_user(user.id, subscription_id)
            if subscription is None:
                raise LookupError("Subscription not found")
            return await DigestDeliveryRepository(session).get_processing_stats_for_period(
                user.id,
                subscription.id,
                period_start,
                period_end,
            )

    async def create_subscription(self, telegram_user_id: int, name: str | None = None) -> Subscription:
        async with self._session_factory() as session:
            user_repo = UserRepository(session)
            subscription_repo = SubscriptionRepository(session)
            user = await user_repo.get_by_telegram_user_id(telegram_user_id)
            if user is None:
                raise LookupError("User not found")
            count = await subscription_repo.count_for_user(user.id)
            if count >= self._bot_settings.max_subscriptions_per_user:
                raise ProductLimitExceededError("max_subscriptions_per_user", self._bot_settings.max_subscriptions_per_user)
            if name is None:
                prefix = "Подписка" if user.language == "ru" else "Subscription"
                name = f"{prefix} {count + 1}"
            subscription = await subscription_repo.create_subscription(user.id, name.strip())
            await session.commit()
            return subscription

    async def create_subscription_from_preset(
        self,
        telegram_user_id: int,
        preset_id: str,
    ) -> PresetCreateResult:
        preset = get_channel_preset(preset_id)
        if preset is None:
            raise ValueError("Unknown preset")

        async with self._session_factory() as session:
            user_repo = UserRepository(session)
            subscription_repo = SubscriptionRepository(session)
            user = await user_repo.get_by_telegram_user_id(telegram_user_id)
            if user is None:
                raise LookupError("User not found")
            count = await subscription_repo.count_for_user(user.id)
            if count >= self._bot_settings.max_subscriptions_per_user:
                raise ProductLimitExceededError("max_subscriptions_per_user", self._bot_settings.max_subscriptions_per_user)
            existing_names = {subscription.name for subscription in await subscription_repo.list_for_user(user.id)}

        available: list[str] = []
        not_found: list[str] = []
        limit_exceeded: list[str] = []
        for username in preset.channels:
            if len(available) >= self._bot_settings.max_channels_per_subscription:
                limit_exceeded.append(f"@{username}")
                continue
            try:
                await self._scraper_client.fetch_page(username)
            except ChannelNotFoundError:
                not_found.append(f"@{username}")
            except Exception:
                logger.warning("Preset channel validation failed for %s", username, exc_info=True)
                not_found.append(f"@{username}")
            else:
                available.append(username)

        if not available:
            return PresetCreateResult(subscription=None, added=[], not_found=not_found, limit_exceeded=limit_exceeded)

        async with self._session_factory() as session:
            user_repo = UserRepository(session)
            channel_repo = ChannelRepository(session)
            subscription_repo = SubscriptionRepository(session)
            user = await user_repo.get_by_telegram_user_id(telegram_user_id)
            if user is None:
                raise LookupError("User not found")
            count = await subscription_repo.count_for_user(user.id)
            if count >= self._bot_settings.max_subscriptions_per_user:
                raise ProductLimitExceededError("max_subscriptions_per_user", self._bot_settings.max_subscriptions_per_user)
            existing_names = {subscription.name for subscription in await subscription_repo.list_for_user(user.id)}

            subscription = await subscription_repo.create_subscription(
                user.id,
                unique_subscription_name(preset.name, existing_names),
                digest_format=DigestFormat.SUMMARY,
                summary_mode=SummaryMode.BRIEF,
                notification_cron=PRESET_NOTIFICATION_CRON,
                frequency=DeliveryFrequency.HOURLY,
                enabled=True,
            )

            added: list[str] = []
            for username in available:
                channel = await channel_repo.upsert_by_username(username, name=username)
                _, created = await subscription_repo.add_channel(subscription.id, channel.id)
                if created:
                    added.append(f"@{username}")

            created_subscription = await subscription_repo.get_for_user(user.id, subscription.id)
            await session.commit()
            return PresetCreateResult(subscription=created_subscription, added=added, not_found=not_found, limit_exceeded=limit_exceeded)

    async def rename_subscription(self, telegram_user_id: int, subscription_id: int, name: str) -> Subscription:
        normalized = name.strip()
        if not normalized:
            raise ValueError("Name must not be empty")
        async with self._session_factory() as session:
            user = await UserRepository(session).get_by_telegram_user_id(telegram_user_id)
            if user is None:
                raise LookupError("User not found")
            repo = SubscriptionRepository(session)
            subscription = await repo.get_for_user(user.id, subscription_id)
            if subscription is None:
                raise LookupError("Subscription not found")
            await repo.rename(subscription, normalized)
            await session.commit()
            return subscription

    async def delete_subscription(self, telegram_user_id: int, subscription_id: int) -> bool:
        async with self._session_factory() as session:
            user = await UserRepository(session).get_by_telegram_user_id(telegram_user_id)
            if user is None:
                raise LookupError("User not found")
            repo = SubscriptionRepository(session)
            subscription = await repo.get_for_user(user.id, subscription_id)
            if subscription is None:
                return False
            deleted = await repo.delete(subscription.id)
            await session.commit()
            return deleted

    async def update_subscription_digest_format(
        self,
        telegram_user_id: int,
        subscription_id: int,
        digest_format: DigestFormat,
    ) -> Subscription:
        async with self._session_factory() as session:
            user = await UserRepository(session).get_by_telegram_user_id(telegram_user_id)
            if user is None:
                raise LookupError("User not found")
            repo = SubscriptionRepository(session)
            subscription = await repo.get_for_user(user.id, subscription_id)
            if subscription is None:
                raise LookupError("Subscription not found")
            if digest_format == DigestFormat.SHORT:
                digest_format = DigestFormat.SUMMARY
            await repo.update_digest_format(subscription, digest_format)
            await session.commit()
            return subscription

    async def update_subscription_summary_mode(
        self,
        telegram_user_id: int,
        subscription_id: int,
        summary_mode: SummaryMode,
    ) -> Subscription:
        async with self._session_factory() as session:
            user = await UserRepository(session).get_by_telegram_user_id(telegram_user_id)
            if user is None:
                raise LookupError("User not found")
            repo = SubscriptionRepository(session)
            subscription = await repo.get_for_user(user.id, subscription_id)
            if subscription is None:
                raise LookupError("Subscription not found")
            await repo.update_summary_mode(subscription, summary_mode)
            await repo.update_digest_format(subscription, DigestFormat.SUMMARY)
            await session.commit()
            return subscription

    async def update_subscription_custom_prompt(
        self,
        telegram_user_id: int,
        subscription_id: int,
        custom_prompt: str,
    ) -> Subscription:
        prompt = custom_prompt.strip()
        if not prompt:
            raise ValueError("Prompt must not be empty")
        async with self._session_factory() as session:
            user = await UserRepository(session).get_by_telegram_user_id(telegram_user_id)
            if user is None:
                raise LookupError("User not found")
            repo = SubscriptionRepository(session)
            subscription = await repo.get_for_user(user.id, subscription_id)
            if subscription is None:
                raise LookupError("Subscription not found")
            await repo.update_custom_prompt(subscription, prompt)
            await repo.update_summary_mode(subscription, SummaryMode.CUSTOM)
            await repo.update_digest_format(subscription, DigestFormat.SUMMARY)
            await session.commit()
            return subscription

    async def update_subscription_filter_prompt(
        self,
        telegram_user_id: int,
        subscription_id: int,
        filter_prompt: str,
    ) -> Subscription:
        prompt = filter_prompt.strip()
        if not prompt:
            raise ValueError("Prompt must not be empty")
        async with self._session_factory() as session:
            user = await UserRepository(session).get_by_telegram_user_id(telegram_user_id)
            if user is None:
                raise LookupError("User not found")
            repo = SubscriptionRepository(session)
            subscription = await repo.get_for_user(user.id, subscription_id)
            if subscription is None:
                raise LookupError("Subscription not found")
            await repo.update_filter_prompt(subscription, prompt)
            await repo.update_digest_format(subscription, DigestFormat.SUMMARY)
            await session.commit()
            return subscription

    async def reset_subscription_prompts(self, telegram_user_id: int, subscription_id: int) -> Subscription:
        async with self._session_factory() as session:
            user = await UserRepository(session).get_by_telegram_user_id(telegram_user_id)
            if user is None:
                raise LookupError("User not found")
            repo = SubscriptionRepository(session)
            subscription = await repo.get_for_user(user.id, subscription_id)
            if subscription is None:
                raise LookupError("Subscription not found")
            await repo.reset_prompts(subscription)
            await session.commit()
            return subscription

    async def update_subscription_frequency(
        self,
        telegram_user_id: int,
        subscription_id: int,
        frequency: DeliveryFrequency,
    ) -> Subscription:
        async with self._session_factory() as session:
            user = await UserRepository(session).get_by_telegram_user_id(telegram_user_id)
            if user is None:
                raise LookupError("User not found")
            repo = SubscriptionRepository(session)
            subscription = await repo.get_for_user(user.id, subscription_id)
            if subscription is None:
                raise LookupError("Subscription not found")
            await repo.update_frequency(subscription, frequency, cron_for_frequency(frequency))
            await session.commit()
            return subscription

    async def update_subscription_notification_cron(
        self,
        telegram_user_id: int,
        subscription_id: int,
        notification_cron: str,
    ) -> Subscription:
        cron = validate_notification_cron(notification_cron)
        async with self._session_factory() as session:
            user = await UserRepository(session).get_by_telegram_user_id(telegram_user_id)
            if user is None:
                raise LookupError("User not found")
            repo = SubscriptionRepository(session)
            subscription = await repo.get_for_user(user.id, subscription_id)
            if subscription is None:
                raise LookupError("Subscription not found")
            await repo.update_notification_cron(subscription, cron)
            await session.commit()
            return subscription

    async def toggle_subscription_enabled(self, telegram_user_id: int, subscription_id: int) -> Subscription:
        async with self._session_factory() as session:
            user = await UserRepository(session).get_by_telegram_user_id(telegram_user_id)
            if user is None:
                raise LookupError("User not found")
            repo = SubscriptionRepository(session)
            subscription = await repo.get_for_user(user.id, subscription_id)
            if subscription is None:
                raise LookupError("Subscription not found")
            await repo.update_enabled(subscription, not subscription.enabled)
            await session.commit()
            return subscription

    async def list_channels(self, telegram_user_id: int, subscription_id: int) -> list[Channel]:
        subscription = await self.get_subscription(telegram_user_id, subscription_id)
        if subscription is None:
            return []
        return sorted(
            [link.channel for link in subscription.channel_links if link.channel is not None],
            key=lambda channel: channel.username or "",
        )

    async def subscribe_many(
        self,
        telegram_user_id: int,
        subscription_id: int,
        raw_channels: str,
    ) -> BulkSubscribeResult:
        added: list[str] = []
        already_subscribed: list[str] = []
        invalid: list[str] = []
        not_found: list[str] = []
        limit_exceeded: list[str] = []

        usernames: list[str] = []
        seen: set[str] = set()
        for candidate in split_channel_references(raw_channels):
            try:
                username = normalize_channel_reference(candidate)
            except InvalidChannelReferenceError:
                invalid.append(candidate)
                continue
            if username not in seen:
                seen.add(username)
                usernames.append(username)

        async with self._session_factory() as session:
            user_repo = UserRepository(session)
            channel_repo = ChannelRepository(session)
            subscription_repo = SubscriptionRepository(session)
            user = await user_repo.get_by_telegram_user_id(telegram_user_id)
            if user is None:
                raise LookupError("User not found")
            subscription = await subscription_repo.get_for_user(user.id, subscription_id)
            if subscription is None:
                raise LookupError("Subscription not found")
            existing_channels = await subscription_repo.list_channels(subscription.id)
            existing_usernames = {channel.username for channel in existing_channels if channel.username}
            channel_count = len(existing_usernames)

            for username in usernames:
                if username in existing_usernames:
                    already_subscribed.append(f"@{username}")
                    continue
                if channel_count >= self._bot_settings.max_channels_per_subscription:
                    limit_exceeded.append(f"@{username}")
                    continue
                try:
                    await self._scraper_client.fetch_page(username)
                except ChannelNotFoundError:
                    not_found.append(f"@{username}")
                    continue

                channel = await channel_repo.upsert_by_username(username, name=username)
                _, created = await subscription_repo.add_channel(subscription.id, channel.id)
                if created:
                    added.append(f"@{username}")
                    existing_usernames.add(username)
                    channel_count += 1
                else:
                    already_subscribed.append(f"@{username}")

            await session.commit()

        return BulkSubscribeResult(
            added=added,
            already_subscribed=already_subscribed,
            invalid=invalid,
            not_found=not_found,
            limit_exceeded=limit_exceeded,
        )

    async def unsubscribe_many(
        self,
        telegram_user_id: int,
        subscription_id: int,
        raw_channels: str,
    ) -> BulkUnsubscribeResult:
        removed: list[str] = []
        not_subscribed: list[str] = []
        invalid: list[str] = []

        usernames: list[str] = []
        seen: set[str] = set()
        for candidate in split_channel_references(raw_channels):
            try:
                username = normalize_channel_reference(candidate)
            except InvalidChannelReferenceError:
                invalid.append(candidate)
                continue
            if username not in seen:
                seen.add(username)
                usernames.append(username)

        async with self._session_factory() as session:
            user_repo = UserRepository(session)
            subscription_repo = SubscriptionRepository(session)
            user = await user_repo.get_by_telegram_user_id(telegram_user_id)
            if user is None:
                raise LookupError("User not found")
            subscription = await subscription_repo.get_for_user(user.id, subscription_id)
            if subscription is None:
                raise LookupError("Subscription not found")

            channels = await subscription_repo.list_channels(subscription.id)
            channels_by_username = {channel.username: channel for channel in channels if channel.username}

            for username in usernames:
                channel = channels_by_username.get(username)
                if channel is None:
                    not_subscribed.append(f"@{username}")
                    continue
                await subscription_repo.remove_channel(subscription.id, channel.id)
                removed.append(f"@{username}")

            await session.commit()

        return BulkUnsubscribeResult(removed=removed, not_subscribed=not_subscribed, invalid=invalid)


def format_settings_text(user: User, language: str = "en") -> str:
    if language == "ru":
        return (
            "Настройки\n\n"
            f"Часовой пояс: {user.timezone}\n"
            f"Язык: {'Русский' if user.language == 'ru' else 'English'}"
        )

    return (
        "Settings\n\n"
        f"Timezone: {user.timezone}\n"
        f"Language: {'Russian' if user.language == 'ru' else 'English'}"
    )


def format_subscriptions_text(
    subscriptions: list[Subscription],
    language: str = "en",
    *,
    max_subscriptions: int | None = None,
    max_channels: int | None = None,
) -> str:
    title = "Подписки" if language == "ru" else "Subscriptions"
    if max_subscriptions is not None:
        title = f"{title} ({len(subscriptions)}/{max_subscriptions})"
    if not subscriptions:
        return (
            f"{title}\n\nПока пусто. Создайте первую подписку."
            if language == "ru"
            else f"{title}\n\nNo subscriptions yet. Create your first one."
        )

    lines = []
    for index, subscription in enumerate(subscriptions, start=1):
        channel_count = len(subscription.channel_links)
        state = "вкл" if subscription.enabled and language == "ru" else "выкл" if language == "ru" else "on" if subscription.enabled else "off"
        channels_label = "каналов" if language == "ru" else "channels"
        channel_count_text = f"{channel_count}/{max_channels}" if max_channels is not None else str(channel_count)
        lines.append(f"{index}. {subscription.name} [{state}] - {channel_count_text} {channels_label}")
    return f"{title}\n\n" + "\n".join(lines)


def format_subscription_detail_text(subscription: Subscription, user: User) -> str:
    channels = sorted(
        [link.channel for link in subscription.channel_links if link.channel is not None],
        key=lambda channel: channel.username or "",
    )
    channels_text = "\n".join(f"- @{channel.username}" for channel in channels if channel.username)
    if not channels_text:
        channels_text = "-"

    if user.language == "ru":
        return (
            f"Подписка: {subscription.name}\n\n"
            f"Статус: {'включена' if subscription.enabled else 'выключена'}\n"
            f"Расписание: {notification_schedule_label(subscription, user.language)}\n\n"
            f"Каналы:\n{channels_text}"
        )

    return (
        f"Subscription: {subscription.name}\n\n"
        f"State: {'enabled' if subscription.enabled else 'disabled'}\n"
        f"Schedule: {notification_schedule_label(subscription, user.language)}\n\n"
        f"Channels:\n{channels_text}"
    )


def _effective_filter_prompt(subscription: Subscription, language: str) -> str:
    return subscription.filter_prompt or default_filter_task_prompt(language)


def _effective_summary_prompt(subscription: Subscription, language: str) -> str:
    return subscription.custom_prompt or default_summary_task_prompt(language)


def _html_code_block(text: str) -> str:
    return f"<pre>{escape(text)}</pre>"


def format_digest_prompt_settings_text(subscription: Subscription, user: User) -> str:
    channels = sorted(
        [link.channel for link in subscription.channel_links if link.channel is not None],
        key=lambda channel: channel.username or "",
    )
    channels_text = "\n".join(f"- @{escape(channel.username)}" for channel in channels if channel.username) or "-"
    filter_prompt = _html_code_block(_effective_filter_prompt(subscription, user.language))
    summary_prompt = _html_code_block(_effective_summary_prompt(subscription, user.language))

    if user.language == "ru":
        return (
            f"Подписка: {escape(subscription.name)}\n\n"
            f"Статус: {'включена' if subscription.enabled else 'выключена'}\n"
            f"Расписание: {escape(notification_schedule_label(subscription, user.language))}\n\n"
            f"Каналы:\n{channels_text}\n\n"
            f"Промпт для AI-фильтра:\n{filter_prompt}\n\n"
            f"Промпт для AI-пересказа:\n{summary_prompt}"
        )

    return (
        f"Subscription: {escape(subscription.name)}\n\n"
        f"State: {'enabled' if subscription.enabled else 'disabled'}\n"
        f"Schedule: {escape(notification_schedule_label(subscription, user.language))}\n\n"
        f"Channels:\n{channels_text}\n\n"
        f"AI filter prompt:\n{filter_prompt}\n\n"
        f"AI summary prompt:\n{summary_prompt}"
    )
