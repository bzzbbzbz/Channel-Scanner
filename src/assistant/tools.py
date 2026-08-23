"""Internal MCP-like tools exposed to the natural-language assistant."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from html import escape
import hashlib
import json
import logging
from typing import Any

from sqlalchemy.ext.asyncio import async_sessionmaker

from src.assistant.cron import describe_cron
from src.bot.service import BotService, ProductLimitExceededError, digest_format_label, notification_schedule_label
from src.bot.texts import t
from src.config.settings import LlmSettings
from src.digest.service import build_digest_messages
from src.llm import OpenRouterModelPool
from src.knowledge.service import KnowledgeService
from src.models.user import DigestFormat, SummaryMode, User
from src.prompt_defaults import default_filter_task_prompt, default_summary_task_prompt
from src.repository.chat_message import ChatMessageRepository
from src.repository.digest_delivery import DigestDeliveryRepository
from src.repository.on_demand_digest import OnDemandDigestRepository
from src.repository.post import PostRepository
from src.repository.channel import ChannelRepository
from src.scraper.client import TelegramClient
from src.scraper.service import ScraperService

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ToolExecutionResult:
    name: str
    payload: dict[str, Any]
    system_message: str | None = None
    additional_system_messages: list[str] = field(default_factory=list)
    ends_turn: bool = False


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
        _tool_schema(
            "getDigestProcessingLogs",
            "Return aggregate found, filtered, and included post counts for completed processing runs of one user-owned subscription. "
            "Use ISO-8601 timezone-aware timestamps; the period is [period_start, period_end).",
            {
                "subscription_id": {"type": "integer"},
                "period_start": {"type": "string", "format": "date-time"},
                "period_end": {"type": "string", "format": "date-time"},
            },
            required=["subscription_id", "period_start", "period_end"],
        ),
        _tool_schema(
            "generateOnDemandDigest",
            "Scrape one user-owned subscription through the requested [period_start, period_end) interval, then send a manual digest. "
            "Use ISO-8601 timezone-aware timestamps. Reusing the same subscription, period, and prompts sends the stored result without scraping or LLM generation.",
            {
                "subscription_id": {"type": "integer"},
                "period_start": {"type": "string", "format": "date-time"},
                "period_end": {"type": "string", "format": "date-time"},
            },
            required=["subscription_id", "period_start", "period_end"],
        ),
        _tool_schema(
            "debugDigestPrompts",
            "Replay persisted posts through the digest filter and synthesis pipeline without sending or saving delivery state. "
            "Use ISO-8601 timezone-aware timestamps; the period is [period_start, period_end).",
            {
                "subscription_id": {"type": "integer"},
                "period_start": {"type": "string", "format": "date-time"},
                "period_end": {"type": "string", "format": "date-time"},
                "filter_prompt": {"type": "string", "minLength": 1},
                "summary_prompt": {"type": "string", "minLength": 1},
            },
            required=["subscription_id", "period_start", "period_end", "filter_prompt", "summary_prompt"],
        ),
        _tool_schema(
            "listKnowledgeChannels",
            "List administrator-approved public channels available to every user for knowledge search.",
            {},
        ),
        _tool_schema(
            "suggestKnowledgeChannels",
            "Rank up to three READY public catalog channels from username and catalog-description tokens only. It never searches posts.",
            {"question": {"type": "string"}},
            required=["question"],
        ),
        _tool_schema(
            "requestKnowledgeChannel",
            "Request that a public Telegram channel be added to the shared knowledge catalog. This only creates a pending administrator review request.",
            {"username": {"type": "string"}},
            required=["username"],
        ),
        _tool_schema(
            "searchKnowledge",
            "Search either one approved public catalog channel or one user-owned subscription. For a catalog use its channel_id from listKnowledgeChannels; the service also safely accepts its catalog record id. For a subscription use its subscription id. The grounded rendered result ends the turn.",
            {
                "scope_type": {"type": "string", "enum": ["catalog", "subscription"]},
                "scope_id": {"type": "integer"},
                "question": {"type": "string", "minLength": 2},
            },
            required=["scope_type", "scope_id", "question"],
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

    def __init__(
        self,
        session_factory: async_sessionmaker,
        scraper_client: TelegramClient,
        service: BotService,
        llm_settings: LlmSettings | None = None,
        model_pool: OpenRouterModelPool | None = None,
        memory_service=None,
        knowledge_service: KnowledgeService | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._scraper_client = scraper_client
        self._service = service
        self._llm_settings = llm_settings or LlmSettings()
        self._model_pool = model_pool
        self._memory_service = memory_service
        self._knowledge_service = knowledge_service

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
            try:
                subscription = await self._service.create_subscription(user.telegram_user_id, str(arguments["name"]))
            except ProductLimitExceededError as exc:
                return _limit_result(name, user, exc)
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
            if result.limit_exceeded:
                message = _system_text(
                    user,
                    f"Каналы в подписке {subscription_name} обновлены. {t(user.language, 'limit_channels', limit=str(self._service.max_channels_per_subscription))}",
                    f"Channels in subscription {subscription_name} updated. {t(user.language, 'limit_channels', limit=str(self._service.max_channels_per_subscription))}",
                )
            else:
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

        if name == "getDigestProcessingLogs":
            period_start, period_end, period_error = _parse_period(arguments)
            if period_error:
                return ToolExecutionResult(name, {"error": "invalid_period", "detail": period_error})
            subscription = await self._service.get_subscription(user.telegram_user_id, int(arguments["subscription_id"]))
            if subscription is None:
                raise LookupError("Subscription not found")
            async with self._session_factory() as session:
                stats = await DigestDeliveryRepository(session).get_processing_stats_for_period(
                    user.id,
                    subscription.id,
                    period_start,
                    period_end,
                )
            return ToolExecutionResult(
                name,
                {
                    "subscription_id": subscription.id,
                    "period_start": period_start.isoformat(),
                    "period_end": period_end.isoformat(),
                    "completed_run_count": stats.run_count,
                    "found_count": stats.found_count,
                    "filtered_count": stats.filtered_count,
                    "included_count": stats.included_count,
                },
            )

        if name == "generateOnDemandDigest":
            period_start, period_end, period_error = _parse_period(arguments)
            if period_error:
                return ToolExecutionResult(name, {"error": "invalid_period", "detail": period_error})
            period_start = period_start.astimezone(timezone.utc)
            period_end = period_end.astimezone(timezone.utc)
            subscription = await self._service.get_subscription(user.telegram_user_id, int(arguments["subscription_id"]))
            if subscription is None:
                raise LookupError("Subscription not found")
            prompt_fingerprint = _on_demand_prompt_fingerprint(subscription, user)

            async with self._session_factory() as session:
                cache_repo = OnDemandDigestRepository(session)
                cached = await cache_repo.get(user.id, subscription.id, period_start, period_end, prompt_fingerprint)
                if cached is not None and cached.status == "ready":
                    return _on_demand_result(name, subscription.id, period_start, period_end, cached.rendered_messages or [], cached=True)
                if cached is not None:
                    return ToolExecutionResult(name, {"error": "on_demand_digest_in_progress"})
                claimed = await cache_repo.claim(user.id, subscription.id, period_start, period_end, prompt_fingerprint)
                await session.commit()
                if not claimed:
                    cached = await cache_repo.get(user.id, subscription.id, period_start, period_end, prompt_fingerprint)
                    if cached is not None and cached.status == "ready":
                        return _on_demand_result(name, subscription.id, period_start, period_end, cached.rendered_messages or [], cached=True)
                    return ToolExecutionResult(name, {"error": "on_demand_digest_in_progress"})
                claim = await cache_repo.get(user.id, subscription.id, period_start, period_end, prompt_fingerprint)

            if claim is None:
                raise RuntimeError("On-demand digest claim was not persisted")

            try:
                await self._scrape_subscription_period(subscription, period_start, period_end)
                async with self._session_factory() as session:
                    current_subscription = await self._service.get_subscription(user.telegram_user_id, subscription.id)
                    if current_subscription is None:
                        raise LookupError("Subscription not found")
                    items = await DigestDeliveryRepository(session).get_posts_for_subscription_period(
                        current_subscription.id,
                        period_start,
                        period_end,
                    )
                    messages = await build_digest_messages(
                        current_subscription,
                        user,
                        items,
                        self._llm_settings,
                        self._model_pool,
                        self._memory_service,
                    )
                    rendered_messages = [message.text for message in messages] or [_empty_on_demand_digest_text(user)]
                    cache_repo = OnDemandDigestRepository(session)
                    await cache_repo.complete(claim.id, rendered_messages, datetime.now(timezone.utc))
                    await session.commit()
            except Exception:
                logger.exception("On-demand digest failed for subscription_id=%s", subscription.id)
                async with self._session_factory() as session:
                    await OnDemandDigestRepository(session).remove(claim.id)
                    await session.commit()
                raise

            return _on_demand_result(name, subscription.id, period_start, period_end, rendered_messages, cached=False)

        if name == "debugDigestPrompts":
            period_start, period_end, period_error = _parse_period(arguments)
            if period_error:
                return ToolExecutionResult(name, {"error": "invalid_period", "detail": period_error})
            subscription = await self._service.get_subscription(user.telegram_user_id, int(arguments["subscription_id"]))
            if subscription is None:
                raise LookupError("Subscription not found")
            async with self._session_factory() as session:
                items = await DigestDeliveryRepository(session).get_posts_for_subscription_period(
                    subscription.id,
                    period_start,
                    period_end,
                )
            messages = await build_digest_messages(
                subscription,
                user,
                items,
                self._llm_settings,
                self._model_pool,
                self._memory_service,
                filter_task_prompt=str(arguments["filter_prompt"]).strip(),
                summary_task_prompt=str(arguments["summary_prompt"]).strip(),
            )
            outcomes = {
                summary.post_id: {
                    "status": summary.status,
                    "skip_reason": summary.skip_reason,
                }
                for message in messages
                for summary in message.delivered_summaries
            }
            return ToolExecutionResult(
                name,
                {
                    "subscription_id": subscription.id,
                    "period_start": period_start.isoformat(),
                    "period_end": period_end.isoformat(),
                    "post_count": len(items),
                    "digest_messages": [message.text for message in messages],
                    "post_outcomes": outcomes,
                },
            )

        if name == "listKnowledgeChannels":
            if self._knowledge_service is None:
                return ToolExecutionResult(name, {"error": "knowledge_search_unavailable"})
            return ToolExecutionResult(name, {"channels": await self._knowledge_service.list_catalog()})

        if name == "suggestKnowledgeChannels":
            if self._knowledge_service is None:
                return ToolExecutionResult(name, {"error": "knowledge_search_unavailable"})
            return ToolExecutionResult(
                name,
                {"channels": await self._knowledge_service.suggest_catalog_channels(str(arguments["question"]))},
            )

        if name == "requestKnowledgeChannel":
            if self._knowledge_service is None:
                return ToolExecutionResult(name, {"error": "knowledge_search_unavailable"})
            request_id, created = await self._knowledge_service.request_channel(user, str(arguments["username"]))
            message = _system_text(
                user,
                "Запрос на добавление канала отправлен администраторам на проверку." if created else "Такой запрос уже ожидает проверки администратора.",
                "The channel request was sent to administrators for review." if created else "This channel request is already awaiting administrator review.",
            )
            return ToolExecutionResult(name, {"request_id": request_id, "created": created}, message)

        if name == "searchKnowledge":
            if self._knowledge_service is None:
                return ToolExecutionResult(name, {"error": "knowledge_search_unavailable"})
            logger.info(
                "Knowledge search requested: scope_type=%s scope_id=%s",
                arguments.get("scope_type"),
                arguments.get("scope_id"),
            )
            result = await self._knowledge_service.search(
                user,
                scope_type=str(arguments["scope_type"]),
                scope_id=int(arguments["scope_id"]),
                question=str(arguments["question"]),
            )
            return ToolExecutionResult(
                name,
                {
                    "query_id": result.query_id,
                    "mode": result.mode,
                    "source_post_ids": result.source_post_ids,
                    "evidence_sufficient": result.evidence_sufficient,
                },
                additional_system_messages=[result.rendered_html],
                ends_turn=True,
            )

        return ToolExecutionResult(name, {"error": f"unknown_tool:{name}"})

    async def _scrape_subscription_period(self, subscription, period_start: datetime, period_end: datetime) -> None:
        scraper = ScraperService(self._scraper_client)
        for link in subscription.channel_links:
            channel = link.channel
            if channel is None or not channel.username:
                continue
            posts = await scraper.scrape_channel_period(channel.username, period_start, period_end, max_posts=100)
            async with self._session_factory() as session:
                if posts:
                    await PostRepository(session).upsert_posts(channel.id, posts)
                await ChannelRepository(session).mark_scraped(channel.id)
                await session.commit()


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


def _parse_period(arguments: dict[str, Any]) -> tuple[datetime | None, datetime | None, str | None]:
    try:
        period_start = datetime.fromisoformat(str(arguments["period_start"]).replace("Z", "+00:00"))
        period_end = datetime.fromisoformat(str(arguments["period_end"]).replace("Z", "+00:00"))
    except (KeyError, TypeError, ValueError):
        return None, None, "period_start and period_end must be ISO-8601 timestamps"
    if period_start.tzinfo is None or period_start.utcoffset() is None:
        return None, None, "period_start must include a timezone offset"
    if period_end.tzinfo is None or period_end.utcoffset() is None:
        return None, None, "period_end must include a timezone offset"
    if period_start >= period_end:
        return None, None, "period_start must be before period_end"
    return period_start, period_end, None


def _on_demand_prompt_fingerprint(subscription, user: User) -> str:
    filter_prompt = subscription.filter_prompt or default_filter_task_prompt(user.language)
    summary_prompt = (
        subscription.custom_prompt
        if subscription.summary_mode == SummaryMode.CUSTOM and subscription.custom_prompt
        else default_summary_task_prompt(user.language)
    )
    inputs = {
        "digest_format": subscription.digest_format.value,
        "summary_mode": subscription.summary_mode.value,
        "filter_prompt": filter_prompt,
        "summary_prompt": summary_prompt,
        "language": user.language,
    }
    return hashlib.sha256(json.dumps(inputs, ensure_ascii=False, sort_keys=True).encode()).hexdigest()


def _empty_on_demand_digest_text(user: User) -> str:
    if user.language == "ru":
        return "За выбранный период новых постов для дайджеста нет."
    return "There are no posts for a digest in the selected period."


def _on_demand_result(
    name: str,
    subscription_id: int,
    period_start: datetime,
    period_end: datetime,
    messages: list[str],
    *,
    cached: bool,
) -> ToolExecutionResult:
    return ToolExecutionResult(
        name,
        {
            "subscription_id": subscription_id,
            "period_start": period_start.isoformat(),
            "period_end": period_end.isoformat(),
            "digest_messages": messages,
            "cached": cached,
        },
        additional_system_messages=messages,
        ends_turn=True,
    )


def _limit_result(name: str, user: User, exc: ProductLimitExceededError) -> ToolExecutionResult:
    if exc.code == "max_subscriptions_per_user":
        message = t(user.language, "limit_subscriptions", limit=str(exc.limit))
    elif exc.code == "max_channels_per_subscription":
        message = t(user.language, "limit_channels", limit=str(exc.limit))
    else:
        message = str(exc)
    return ToolExecutionResult(name, {"error": exc.code, "limit": exc.limit}, message)
