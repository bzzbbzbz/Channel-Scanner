"""Internal MCP-like tools exposed to the natural-language assistant."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from html import escape
from typing import Any

from sqlalchemy.ext.asyncio import async_sessionmaker

from src.assistant.cron import describe_cron
from src.bot.service import BotService, digest_format_label, notification_schedule_label
from src.models.user import DigestFormat, User
from src.repository.chat_message import ChatMessageRepository
from src.scraper.client import TelegramClient


@dataclass(slots=True)
class ToolExecutionResult:
    name: str
    payload: dict[str, Any]
    system_message: str | None = None


def assistant_tool_schemas() -> list[dict[str, Any]]:
    return [
        _tool_schema(
            "getSubscriptions",
            "List subscriptions owned by the current Telegram user.",
            {},
        ),
        _tool_schema(
            "getSubscription",
            "Get one subscription by id, including channels and settings.",
            {"subscription_id": {"type": "integer"}},
            required=["subscription_id"],
        ),
        _tool_schema(
            "createSubscription",
            "Create a new named subscription after the user explicitly confirms creation.",
            {"name": {"type": "string"}, "confirmed": {"type": "boolean"}},
            required=["name", "confirmed"],
        ),
        _tool_schema(
            "setNotification",
            "Set a validated 5-field cron notification schedule for a subscription.",
            {"subscription_id": {"type": "integer"}, "cron": {"type": "string"}},
            required=["subscription_id", "cron"],
        ),
        _tool_schema(
            "setDigestFormat",
            "Set digest format to AI summary. The old short mode is no longer user-selectable.",
            {"subscription_id": {"type": "integer"}, "format": {"type": "string", "enum": ["summary"]}},
            required=["subscription_id", "format"],
        ),
        _tool_schema(
            "setFilterPrompt",
            "Set a custom AI filter Task prompt for a subscription. The app will still enforce memory preferences, JSON output, and post injection outside this task.",
            {"subscription_id": {"type": "integer"}, "prompt": {"type": "string"}},
            required=["subscription_id", "prompt"],
        ),
        _tool_schema(
            "setSummaryPrompt",
            "Set a custom summary prompt for a subscription.",
            {"subscription_id": {"type": "integer"}, "prompt": {"type": "string"}},
            required=["subscription_id", "prompt"],
        ),
        _tool_schema(
            "resetPrompts",
            "Reset both AI filter and AI summary prompts for a subscription to the bot defaults.",
            {"subscription_id": {"type": "integer"}},
            required=["subscription_id"],
        ),
        _tool_schema(
            "addChannels",
            "Add public Telegram channels to a subscription.",
            {"subscription_id": {"type": "integer"}, "channels": {"type": "string"}},
            required=["subscription_id", "channels"],
        ),
        _tool_schema(
            "removeChannels",
            "Remove public Telegram channels from a subscription.",
            {"subscription_id": {"type": "integer"}, "channels": {"type": "string"}},
            required=["subscription_id", "channels"],
        ),
        _tool_schema(
            "setSubscriptionEnabled",
            "Enable or disable a subscription by id. Pass enabled=True to activate, False to deactivate.",
            {"subscription_id": {"type": "integer"}, "enabled": {"type": "boolean"}},
            required=["subscription_id", "enabled"],
        ),
        _tool_schema(
            "getRecentDigests",
            "Return recent digest messages visible to the current user.",
            {"limit": {"type": "integer", "minimum": 1, "maximum": 30}},
        ),
    ]


def _tool_schema(name: str, description: str, properties: dict[str, Any], required: list[str] | None = None) -> dict[str, Any]:
    return {
        "type": "function",
        "function": {
            "name": name,
            "description": description,
            "parameters": {
                "type": "object",
                "properties": properties,
                "required": required or [],
                "additionalProperties": False,
            },
        },
    }


class AssistantToolRegistry:
    """Execute assistant tools with user-scoped authorization."""

    def __init__(self, session_factory: async_sessionmaker, scraper_client: TelegramClient, service: BotService) -> None:
        self._session_factory = session_factory
        self._scraper_client = scraper_client
        self._service = service

    async def execute(self, name: str, arguments: dict[str, Any], user: User) -> ToolExecutionResult:
        if name == "getSubscriptions":
            subscriptions = await self._service.list_subscriptions(user.telegram_user_id)
            return ToolExecutionResult(name, {"subscriptions": [_subscription_payload(item, user) for item in subscriptions]})

        if name == "getSubscription":
            subscription = await self._service.get_subscription(user.telegram_user_id, int(arguments["subscription_id"]))
            return ToolExecutionResult(name, {"subscription": _subscription_payload(subscription, user) if subscription else None})

        if name == "createSubscription":
            if not bool(arguments.get("confirmed")):
                return ToolExecutionResult(name, {"error": "creation_requires_explicit_confirmation"})
            subscription = await self._service.create_subscription(user.telegram_user_id, str(arguments["name"]))
            subscription = await self._service.get_subscription(user.telegram_user_id, subscription.id) or subscription
            subscription_name = _subscription_name_html(subscription)
            message = _system_text(user, f"Подписка {subscription_name} создана.", f"Subscription {subscription_name} created.")
            return ToolExecutionResult(name, {"subscription": _subscription_payload(subscription, user)}, message)

        if name == "setNotification":
            subscription = await self._service.update_subscription_notification_cron(
                user.telegram_user_id,
                int(arguments["subscription_id"]),
                str(arguments["cron"]),
            )
            description = describe_cron(subscription.notification_cron or str(arguments["cron"]))
            subscription_name = _subscription_name_html(subscription)
            message = _system_text(
                user,
                f"Время уведомлений в подписке {subscription_name} установлено: {description.ru}.",
                f"Notification schedule for subscription {subscription_name} set: {description.en}.",
            )
            return ToolExecutionResult(name, {"subscription": _subscription_payload(subscription, user)}, message)

        if name == "setDigestFormat":
            subscription = await self._service.update_subscription_digest_format(
                user.telegram_user_id,
                int(arguments["subscription_id"]),
                DigestFormat(str(arguments["format"])),
            )
            subscription_name = _subscription_name_html(subscription)
            message = _system_text(
                user,
                f"Формат дайджеста в подписке {subscription_name} обновлён: {digest_format_label(subscription, user.language)}.",
                f"Digest format for subscription {subscription_name} updated: {digest_format_label(subscription, user.language)}.",
            )
            return ToolExecutionResult(name, {"subscription": _subscription_payload(subscription, user)}, message)

        if name == "setSummaryPrompt":
            subscription = await self._service.update_subscription_custom_prompt(
                user.telegram_user_id,
                int(arguments["subscription_id"]),
                str(arguments["prompt"]),
            )
            subscription_name = _subscription_name_html(subscription)
            message = _system_text(
                user,
                f"Инструкция для пересказа в подписке {subscription_name} обновлена.",
                f"Summary instructions for subscription {subscription_name} updated.",
            )
            return ToolExecutionResult(name, {"subscription": _subscription_payload(subscription, user)}, message)

        if name == "setFilterPrompt":
            subscription = await self._service.update_subscription_filter_prompt(
                user.telegram_user_id,
                int(arguments["subscription_id"]),
                str(arguments["prompt"]),
            )
            subscription_name = _subscription_name_html(subscription)
            message = _system_text(
                user,
                f"Инструкция для AI-фильтра в подписке {subscription_name} обновлена.",
                f"AI filter instructions for subscription {subscription_name} updated.",
            )
            return ToolExecutionResult(name, {"subscription": _subscription_payload(subscription, user)}, message)

        if name == "resetPrompts":
            subscription = await self._service.reset_subscription_prompts(
                user.telegram_user_id,
                int(arguments["subscription_id"]),
            )
            subscription_name = _subscription_name_html(subscription)
            message = _system_text(
                user,
                f"Промпты AI-фильтра и AI-пересказа в подписке {subscription_name} сброшены по умолчанию.",
                f"AI filter and summary prompts for subscription {subscription_name} reset to defaults.",
            )
            return ToolExecutionResult(name, {"subscription": _subscription_payload(subscription, user)}, message)

        if name == "addChannels":
            subscription = await self._service.get_subscription(user.telegram_user_id, int(arguments["subscription_id"]))
            if subscription is None:
                raise LookupError("Subscription not found")
            result = await self._service.subscribe_many(user.telegram_user_id, int(arguments["subscription_id"]), str(arguments["channels"]))
            subscription_name = _subscription_name_html(subscription)
            message = _system_text(
                user,
                f"Каналы в подписке {subscription_name} обновлены.",
                f"Channels in subscription {subscription_name} updated.",
            )
            return ToolExecutionResult(name, asdict(result), message)

        if name == "removeChannels":
            subscription = await self._service.get_subscription(user.telegram_user_id, int(arguments["subscription_id"]))
            if subscription is None:
                raise LookupError("Subscription not found")
            result = await self._service.unsubscribe_many(user.telegram_user_id, int(arguments["subscription_id"]), str(arguments["channels"]))
            subscription_name = _subscription_name_html(subscription)
            message = _system_text(
                user,
                f"Каналы в подписке {subscription_name} обновлены.",
                f"Channels in subscription {subscription_name} updated.",
            )
            return ToolExecutionResult(name, asdict(result), message)

        if name == "setSubscriptionEnabled":
            subscription = await self._service.get_subscription(user.telegram_user_id, int(arguments["subscription_id"]))
            if subscription is None:
                raise LookupError("Subscription not found")
            enabled = bool(arguments["enabled"])
            async with self._session_factory() as session:
                from src.repository.subscription import SubscriptionRepository
                repo = SubscriptionRepository(session)
                subscription = await repo.get_for_user(user.id, subscription.id)
                if subscription is None:
                    raise LookupError("Subscription not found")
                await repo.update_enabled(subscription, enabled)
                await session.commit()
            subscription_name = _subscription_name_html(subscription)
            if enabled:
                message = _system_text(user, f"Подписка {subscription_name} включена.", f"Subscription {subscription_name} enabled.")
            else:
                message = _system_text(user, f"Подписка {subscription_name} отключена.", f"Subscription {subscription_name} disabled.")
            subscription = await self._service.get_subscription(user.telegram_user_id, int(arguments["subscription_id"]))
            return ToolExecutionResult(name, {"subscription": _subscription_payload(subscription, user)}, message)

        if name == "getRecentDigests":
            limit = int(arguments.get("limit") or 10)
            async with self._session_factory() as session:
                messages = await ChatMessageRepository(session).list_recent_digests(user.id, max(1, min(limit, 30)))
            return ToolExecutionResult(
                name,
                {"digests": [{"text": item.text, "metadata": item.message_metadata, "created_at": item.created_at.isoformat()} for item in messages]},
            )

        return ToolExecutionResult(name, {"error": f"unknown_tool:{name}"})


def _subscription_payload(subscription, user: User) -> dict[str, Any]:
    channels = [link.channel for link in subscription.channel_links if link.channel is not None]
    return {
        "id": subscription.id,
        "name": subscription.name,
        "enabled": subscription.enabled,
        "frequency": subscription.frequency.value,
        "notification_cron": subscription.notification_cron,
        "schedule_label": notification_schedule_label(subscription, user.language),
        "digest_format": subscription.digest_format.value,
        "summary_mode": subscription.summary_mode.value,
        "custom_prompt": subscription.custom_prompt,
        "filter_prompt": subscription.filter_prompt,
        "channels": [f"@{channel.username}" for channel in channels if channel.username],
    }


def _system_text(user: User, ru: str, en: str) -> str:
    return ru if user.language == "ru" else en


def _subscription_name_html(subscription) -> str:
    return f"<b>{escape(str(subscription.name))}</b>"
