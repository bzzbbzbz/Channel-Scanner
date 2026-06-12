"""Digest generation and delivery with OpenRouter summary fallback."""

from __future__ import annotations

import logging
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from html import escape
from typing import Any, Callable, Protocol
from zoneinfo import ZoneInfo

from aiogram import Bot
from aiogram.enums import ParseMode
from sqlalchemy.ext.asyncio import async_sessionmaker

from src.assistant.memory import AssistantMemoryService
from src.assistant.cron import is_cron_due
from src.bot.service import timezone_info
from src.config.settings import LlmSettings
from src.llm import MODEL_FALLBACK_CHAIN, ModelUseCase, OpenRouterClient, OpenRouterModelPool
from src.models.subscription import Subscription
from src.models.user import DigestFormat, SummaryMode, User
from src.repository.digest_delivery import DeliveredSummary, DigestDeliveryRepository, PendingDigestPost
from src.repository.chat_message import ChatMessageRepository
from src.repository.subscription import SubscriptionRepository
from src.telegram_formatting import telegram_html_format_instructions, telegram_safe_html

logger = logging.getLogger(__name__)

TELEGRAM_TEXT_LIMIT = 4000
SHORT_ITEM_LIMIT = 200
LLM_INPUT_LIMIT = 12000
BATCH_PROMPT_LIMIT = 10000
DELIVERY_STATUS_DELIVERED = "delivered"
DELIVERY_STATUS_SKIPPED = "skipped"

FILTER_RESPONSE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "included_post_ids": {"type": "array", "items": {"type": "integer"}},
        "skipped_posts": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "post_id": {"type": "integer"},
                    "reason": {"type": "string"},
                },
                "required": ["post_id", "reason"],
                "additionalProperties": False,
            },
        },
    },
    "required": ["included_post_ids", "skipped_posts"],
    "additionalProperties": False,
}
TOPICS_RESPONSE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "topics": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "title": {"type": "string"},
                    "summary": {"type": "string"},
                    "source_post_ids": {"type": "array", "items": {"type": "integer"}},
                },
                "required": ["title", "summary", "source_post_ids"],
                "additionalProperties": False,
            },
        },
    },
    "required": ["topics"],
    "additionalProperties": False,
}


class DigestSender(Protocol):
    """Minimal sender interface for digest delivery."""

    async def send_message(self, chat_id: int, text: str, parse_mode: str | None = None) -> None:
        """Send a Telegram message."""

    async def close(self) -> None:
        """Release sender resources."""


class BotApiDigestSender:
    """Send digest messages through the Telegram Bot API."""

    def __init__(self, token: str) -> None:
        self._bot = Bot(token=token)

    async def send_message(self, chat_id: int, text: str, parse_mode: str | None = None) -> None:
        await self._bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode=parse_mode or ParseMode.HTML,
            disable_web_page_preview=True,
        )

    async def close(self) -> None:
        await self._bot.session.close()


@dataclass(slots=True)
class RenderedDigestPost:
    """Rendered post body plus persistence metadata."""

    text: str
    delivered_summary: DeliveredSummary


@dataclass(slots=True)
class PreparedDigestMessage:
    """A Telegram message plus the delivered summaries completed by it."""

    text: str
    delivered_summaries: list[DeliveredSummary]
    parse_mode: str | None = ParseMode.HTML


@dataclass(slots=True)
class SummaryResult:
    """Summary text and metadata for one post."""

    text: str
    mode: str
    model_name: str | None
    prompt_snapshot: str | None


@dataclass(slots=True)
class BatchTopic:
    """One synthesized digest topic and its source post ids."""

    title: str
    summary: str
    source_post_ids: list[int]


def is_digest_due(subscription: Subscription, user: User, now: datetime) -> bool:
    """Return whether the subscription is eligible for a new digest."""
    return is_cron_due(subscription, user, now)


def _built_in_prompt(summary_mode: SummaryMode, language: str) -> str:
    if language == "ru":
        if summary_mode == SummaryMode.DETAILED:
            return (
                "Сделай подробный пересказ поста на русском языке. "
                "Структура: 1-3 коротких абзаца без лишнего вступления. "
                "Сохрани факты, числа, имена, контекст, причинно-следственные связи и выводы. "
                "Не добавляй новую информацию."
            )
        return (
            "Сделай краткий пересказ поста на русском языке. "
            "Структура: 2-4 предложения без заголовка. "
            "Сохрани только ключевые факты, числа, имена и выводы. "
            "Не добавляй новую информацию."
        )

    if summary_mode == SummaryMode.DETAILED:
        return (
            "Write a detailed English summary of the post. "
            "Structure: 1-3 short paragraphs with no filler intro. "
            "Keep the facts, numbers, names, context, causal links, and conclusions. "
            "Do not add new information."
        )
    return (
        "Write a brief English summary of the post. "
        "Structure: 2-4 sentences with no heading. "
        "Keep only the key facts, numbers, names, and conclusions. "
        "Do not add new information."
    )


def _summary_format_instructions(language: str) -> str:
    return telegram_html_format_instructions(language)


def _build_summary_prompt(task_prompt: str, language: str, text: str) -> str:
    return "\n".join(
        [
            "<task>",
            task_prompt,
            "</task>",
            "<instructions>",
            _summary_format_instructions(language),
            "</instructions>",
            "<text>",
            text,
            "</text>",
        ]
    )


async def summarize_text(
    subscription: Subscription,
    user: User,
    text: str,
    llm_settings: LlmSettings,
    model_pool: OpenRouterModelPool | None = None,
) -> SummaryResult:
    """Summarize one post with the approved model fallback chain."""
    normalized = _normalize_text(text)
    if subscription.digest_format != DigestFormat.SUMMARY or not llm_settings.openrouter_api_key:
        return SummaryResult(text=_truncate_text(normalized, SHORT_ITEM_LIMIT), mode=DigestFormat.SHORT.value, model_name=None, prompt_snapshot=None)

    prompt_snapshot = subscription.custom_prompt if subscription.summary_mode == SummaryMode.CUSTOM else None
    truncated_input = normalized[:LLM_INPUT_LIMIT]
    task_prompt = prompt_snapshot or _built_in_prompt(subscription.summary_mode, user.language)
    summary_prompt = _build_summary_prompt(task_prompt, user.language, truncated_input)
    client = OpenRouterClient(
        api_key=llm_settings.openrouter_api_key,
        base_url=llm_settings.openrouter_base_url,
        timeout_seconds=llm_settings.timeout_seconds,
    )
    try:
        if model_pool is not None:
            await model_pool.refresh_if_due(client)
            models = model_pool.models_for(ModelUseCase.SUMMARY)
        else:
            models = MODEL_FALLBACK_CHAIN

        for model in models:
            try:
                summary = await client.generate_summary(model, summary_prompt, "")
            except Exception as exc:
                if model_pool is not None:
                    model_pool.record_failure(ModelUseCase.SUMMARY, model, exc)
                logger.warning("Summary generation failed for model=%s", model, exc_info=True)
                continue
            normalized_summary = _normalize_text(summary)
            if not normalized_summary:
                exc = ValueError("empty summary response")
                if model_pool is not None:
                    model_pool.record_failure(ModelUseCase.SUMMARY, model, exc)
                logger.warning("Summary generation returned empty text for model=%s", model)
                continue
            if model_pool is not None:
                model_pool.record_success(ModelUseCase.SUMMARY, model)
            return SummaryResult(
                text=normalized_summary,
                mode=subscription.summary_mode.value,
                model_name=model,
                prompt_snapshot=prompt_snapshot,
            )
    finally:
        await client.close()

    logger.warning("All summary models failed; falling back to 200-char mode")
    return SummaryResult(
        text=_truncate_text(normalized, SHORT_ITEM_LIMIT),
        mode=DigestFormat.SHORT.value,
        model_name=None,
        prompt_snapshot=prompt_snapshot,
    )


async def build_digest_messages(
    subscription: Subscription,
    user: User,
    items: list[PendingDigestPost],
    llm_settings: LlmSettings | None = None,
    model_pool: OpenRouterModelPool | None = None,
    memory_service: AssistantMemoryService | None = None,
) -> list[PreparedDigestMessage]:
    """Format posts into Telegram-safe digest messages."""
    if not items:
        return []

    user_tz = timezone_info(user.timezone)
    items, skipped_empty_summaries = _without_empty_posts(items, subscription)
    if not items:
        return _build_all_skipped_message(user, skipped_empty_summaries)

    if subscription.digest_format == DigestFormat.SUMMARY and (llm_settings or LlmSettings()).openrouter_api_key:
        return await _build_batch_summary_messages(
            subscription,
            user,
            items,
            user_tz,
            llm_settings or LlmSettings(),
            model_pool,
            memory_service,
            skipped_empty_summaries,
        )

    rendered_posts = [await _render_post(item, subscription, user, user_tz, llm_settings, model_pool) for item in items]

    messages: list[PreparedDigestMessage] = []
    current_parts: list[str] = []
    current_summaries: list[DeliveredSummary] = []
    current_length = 0

    for rendered in rendered_posts:
        for segment in _split_rendered_post(rendered, user_tz):
            separator = 2 if current_parts else 0
            next_length = current_length + separator + len(segment.text)

            if current_parts and next_length > TELEGRAM_TEXT_LIMIT:
                messages.append(PreparedDigestMessage(text="\n\n".join(current_parts), delivered_summaries=current_summaries))
                current_parts = [segment.text]
                current_summaries = list(segment.delivered_summaries)
                current_length = len(segment.text)
                continue

            current_parts.append(segment.text)
            current_summaries.extend(segment.delivered_summaries)
            current_length = next_length

    if current_parts:
        if skipped_empty_summaries:
            current_summaries.extend(skipped_empty_summaries)
        messages.append(PreparedDigestMessage(text="\n\n".join(current_parts), delivered_summaries=current_summaries))

    return messages


async def _build_batch_summary_messages(
    subscription: Subscription,
    user: User,
    items: list[PendingDigestPost],
    user_tz: ZoneInfo,
    llm_settings: LlmSettings,
    model_pool: OpenRouterModelPool | None,
    memory_service: AssistantMemoryService | None,
    initial_skipped_summaries: list[DeliveredSummary] | None = None,
) -> list[PreparedDigestMessage]:
    memories = await _retrieve_digest_memories(memory_service, user, subscription, items)
    included_by_id = {item.post_db_id: item for item in items}
    skipped_summaries: list[DeliveredSummary] = list(initial_skipped_summaries or [])

    try:
        included_ids, filtered_summaries, filter_model = await _filter_batch_posts(items, user, subscription, memories, llm_settings, model_pool)
        skipped_summaries.extend(filtered_summaries)
        included_by_id = {item.post_db_id: item for item in items if item.post_db_id in included_ids}
    except Exception:
        filter_model = None
        logger.warning("Batch digest filter failed; including all pending posts", exc_info=True)

    included_items = [item for item in items if item.post_db_id in included_by_id]
    if not included_items:
        return _build_all_skipped_message(user, skipped_summaries)

    prompt_snapshot = subscription.custom_prompt if subscription.summary_mode == SummaryMode.CUSTOM else None
    try:
        topics, digest_model = await _synthesize_batch_topics(included_items, user, subscription, memories, llm_settings, model_pool)
    except Exception:
        logger.warning("Batch digest synthesis failed; falling back to short mode", exc_info=True)
        return await _build_short_fallback_messages(included_items, skipped_summaries, subscription, user, user_tz, prompt_snapshot)

    if not topics:
        return await _build_short_fallback_messages(included_items, skipped_summaries, subscription, user, user_tz, prompt_snapshot)

    text = _render_batch_digest(topics, included_by_id, user_tz, user.language)
    delivered = [
        DeliveredSummary(
            post_id=item.post_db_id,
            summary_text=_summary_for_post_from_topics(item.post_db_id, topics),
            summary_mode=subscription.summary_mode.value,
            summary_model=digest_model or filter_model,
            prompt_snapshot=prompt_snapshot,
            status=DELIVERY_STATUS_DELIVERED,
        )
        for item in included_items
    ]
    return _split_batch_text(text, [*delivered, *skipped_summaries])


async def _render_post(
    item: PendingDigestPost,
    subscription: Subscription,
    user: User,
    user_tz: ZoneInfo,
    llm_settings: LlmSettings | None,
    model_pool: OpenRouterModelPool | None,
) -> RenderedDigestPost:
    summary = await summarize_text(subscription, user, item.content, llm_settings or LlmSettings(), model_pool)
    header = _build_header(item, user_tz)
    body_html = _markdown_to_html(summary.text)
    return RenderedDigestPost(
        text=f"{header}\n{body_html}",
        delivered_summary=DeliveredSummary(
            post_id=item.post_db_id,
            summary_text=summary.text,
            summary_mode=summary.mode,
            summary_model=summary.model_name,
            prompt_snapshot=summary.prompt_snapshot,
        ),
    )


def _split_rendered_post(rendered: RenderedDigestPost, user_tz: ZoneInfo) -> list[PreparedDigestMessage]:
    del user_tz
    if len(rendered.text) <= TELEGRAM_TEXT_LIMIT:
        return [PreparedDigestMessage(text=rendered.text, delivered_summaries=[rendered.delivered_summary])]

    header, _, body_html = rendered.text.partition("\n")
    available = TELEGRAM_TEXT_LIMIT - len(header) - 1
    chunks = _split_text(body_html, max(available - 10, 100))
    segments: list[PreparedDigestMessage] = []
    for index, chunk in enumerate(chunks):
        prefix = header if index == 0 else f"{header} (cont. {index + 1})"
        delivered = [rendered.delivered_summary] if index == len(chunks) - 1 else []
        segments.append(PreparedDigestMessage(text=f"{prefix}\n{chunk}", delivered_summaries=delivered))
    return segments


async def _retrieve_digest_memories(
    memory_service: AssistantMemoryService | None,
    user: User,
    subscription: Subscription,
    items: list[PendingDigestPost],
) -> list[str]:
    if memory_service is None:
        return []
    sample = "\n".join(_truncate_text(item.content, 180) for item in items[:10])
    query = f"Digest filtering preferences for subscription {subscription.name}. Recent posts:\n{sample}"
    return await memory_service.retrieve(user, query, limit=5)


async def _filter_batch_posts(
    items: list[PendingDigestPost],
    user: User,
    subscription: Subscription,
    memories: list[str],
    llm_settings: LlmSettings,
    model_pool: OpenRouterModelPool | None,
) -> tuple[set[int], list[DeliveredSummary], str | None]:
    included: set[int] = set()
    skipped: list[DeliveredSummary] = []
    last_model: str | None = None

    for chunk in _chunk_posts_for_prompt(items):
        prompt = _build_filter_prompt(chunk, user, subscription, memories)
        payload, last_model = await _generate_summary_json(
            prompt,
            llm_settings,
            model_pool,
            schema_name="digest_filter",
            schema=FILTER_RESPONSE_SCHEMA,
            validate_payload=_validate_filter_payload,
        )
        chunk_ids = {item.post_db_id for item in chunk}
        included_ids = {int(post_id) for post_id in payload.get("included_post_ids", []) if _is_int_like(post_id)} & chunk_ids
        skipped_ids: set[int] = set()
        for item in payload.get("skipped_posts", []):
            if not isinstance(item, dict) or not _is_int_like(item.get("post_id")):
                continue
            post_id = int(item["post_id"])
            if post_id not in chunk_ids:
                continue
            skipped_ids.add(post_id)
            reason = str(item.get("reason") or "filtered").strip()[:500]
            skipped.append(
                DeliveredSummary(
                    post_id=post_id,
                    summary_text=None,
                    summary_mode=subscription.summary_mode.value,
                    summary_model=last_model,
                    prompt_snapshot=None,
                    status=DELIVERY_STATUS_SKIPPED,
                    skip_reason=reason,
                )
            )
        included.update((included_ids - skipped_ids) | (chunk_ids - skipped_ids - included_ids))

    return included, skipped, last_model


async def _synthesize_batch_topics(
    items: list[PendingDigestPost],
    user: User,
    subscription: Subscription,
    memories: list[str],
    llm_settings: LlmSettings,
    model_pool: OpenRouterModelPool | None,
) -> tuple[list[BatchTopic], str | None]:
    chunk_topics: list[BatchTopic] = []
    last_model: str | None = None
    chunks = _chunk_posts_for_prompt(items)

    for chunk in chunks:
        prompt = _build_digest_json_prompt(chunk, user, subscription, memories)
        payload, last_model = await _generate_summary_json(
            prompt,
            llm_settings,
            model_pool,
            schema_name="digest_topics",
            schema=TOPICS_RESPONSE_SCHEMA,
            validate_payload=_validate_topics_payload,
        )
        chunk_topics.extend(_topics_from_payload(payload, {item.post_db_id for item in chunk}))

    if len(chunks) <= 1 or not chunk_topics:
        return chunk_topics, last_model

    merge_prompt = _build_merge_topics_prompt(chunk_topics, user.language)
    try:
        payload, last_model = await _generate_summary_json(
            merge_prompt,
            llm_settings,
            model_pool,
            schema_name="digest_topics",
            schema=TOPICS_RESPONSE_SCHEMA,
            validate_payload=_validate_topics_payload,
        )
        merged = _topics_from_payload(payload, {item.post_db_id for item in items})
        return merged or chunk_topics, last_model
    except Exception:
        logger.warning("Batch digest topic merge failed; using chunk topics", exc_info=True)
        return chunk_topics, last_model


async def _generate_summary_json(
    prompt: str,
    llm_settings: LlmSettings,
    model_pool: OpenRouterModelPool | None,
    *,
    schema_name: str,
    schema: dict[str, Any],
    validate_payload: Callable[[dict[str, Any]], None],
) -> tuple[dict[str, Any], str | None]:
    client = OpenRouterClient(
        api_key=llm_settings.openrouter_api_key,
        base_url=llm_settings.openrouter_base_url,
        timeout_seconds=llm_settings.timeout_seconds,
    )
    try:
        if model_pool is not None:
            await model_pool.refresh_if_due(client)
            models = model_pool.models_for(ModelUseCase.SUMMARY)
        else:
            models = MODEL_FALLBACK_CHAIN

        response_format = _json_schema_response_format(schema_name, schema)
        for model in models:
            try:
                text = await client.generate_summary(
                    model,
                    prompt,
                    "",
                    response_format=response_format,
                    require_parameters=True,
                )
            except Exception as exc:
                if model_pool is not None:
                    model_pool.record_failure(ModelUseCase.SUMMARY, model, exc)
                logger.warning("Batch summary JSON generation failed for model=%s", model, exc_info=True)
                continue
            try:
                payload = _parse_json_object(text)
                validate_payload(payload)
            except Exception as exc:
                if model_pool is not None:
                    model_pool.record_failure(ModelUseCase.SUMMARY, model, exc)
                logger.warning(
                    "Batch summary JSON validation failed for model=%s response_preview=%r",
                    model,
                    _log_preview(text),
                    exc_info=True,
                )
                continue
            if model_pool is not None:
                model_pool.record_success(ModelUseCase.SUMMARY, model)
            return payload, model
    finally:
        await client.close()

    raise RuntimeError("all summary models failed")


def _json_schema_response_format(name: str, schema: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "json_schema",
        "json_schema": {
            "name": name,
            "strict": True,
            "schema": schema,
        },
    }


def _build_filter_prompt(
    items: list[PendingDigestPost],
    user: User,
    subscription: Subscription,
    memories: list[str],
) -> str:
    language = "Russian" if user.language == "ru" else "English"
    return "\n".join(
        [
            "You are a strict Telegram digest pre-filter.",
            f"Subscription: {subscription.name}",
            f"Output language for reasons: {language}.",
            "Task: exclude ads, promos, low-signal reposts, boilerplate, and posts that contradict explicit user memory preferences.",
            "Do not use the subscription summary prompt here. Only use the stable filtering rules and memories below.",
            "Return only valid JSON with this shape:",
            '{"included_post_ids":[1],"skipped_posts":[{"post_id":2,"reason":"ad or low-signal"}]}',
            "If unsure, include the post.",
            "<memories>",
            "\n".join(f"- {memory}" for memory in memories) or "(none)",
            "</memories>",
            "<posts>",
            _serialize_posts_for_prompt(items),
            "</posts>",
        ]
    )


def _build_digest_json_prompt(
    items: list[PendingDigestPost],
    user: User,
    subscription: Subscription,
    memories: list[str],
) -> str:
    task_prompt = subscription.custom_prompt if subscription.summary_mode == SummaryMode.CUSTOM else _built_in_batch_prompt(subscription.summary_mode, user.language)
    return "\n".join(
        [
            "You synthesize a curated Telegram digest from filtered channel posts.",
            "Return only valid JSON with this shape:",
            '{"topics":[{"title":"short topic","summary":"bullet text","source_post_ids":[1,2]}]}',
            "Group related posts into topic bullets. Merge duplicates. Start each cluster from the earliest or most detailed source, then mention only new facts from later sources.",
            "Do not include ads or low-value content. Do not invent facts. Source ids must refer only to provided posts.",
            "The app will render Telegram HTML and source links, so do not output HTML.",
            "<task>",
            task_prompt,
            "</task>",
            "<memories>",
            "\n".join(f"- {memory}" for memory in memories) or "(none)",
            "</memories>",
            "<posts>",
            _serialize_posts_for_prompt(items),
            "</posts>",
        ]
    )


def _build_merge_topics_prompt(topics: list[BatchTopic], language: str) -> str:
    return "\n".join(
        [
            "Merge partial digest topics into one curated digest topic list.",
            "Return only valid JSON with this shape:",
            '{"topics":[{"title":"short topic","summary":"bullet text","source_post_ids":[1,2]}]}',
            "Merge duplicates and keep all source_post_ids that support each final topic.",
            f"Write in {'Russian' if language == 'ru' else 'English'}.",
            "<topics>",
            json.dumps(
                [
                    {"title": topic.title, "summary": topic.summary, "source_post_ids": topic.source_post_ids}
                    for topic in topics
                ],
                ensure_ascii=False,
            ),
            "</topics>",
        ]
    )


def _built_in_batch_prompt(summary_mode: SummaryMode, language: str) -> str:
    if language == "ru":
        detail = "подробные, но компактные" if summary_mode == SummaryMode.DETAILED else "краткие"
        return (
            f"Сделай {detail} тематические пункты дайджеста на русском языке. "
            "Сохрани факты, числа, имена, причинно-следственные связи и выводы. "
            "Не добавляй новую информацию."
        )
    detail = "detailed but compact" if summary_mode == SummaryMode.DETAILED else "brief"
    return (
        f"Write {detail} topical digest bullets in English. "
        "Keep facts, numbers, names, causal links, and conclusions. Do not add new information."
    )


def _serialize_posts_for_prompt(items: list[PendingDigestPost]) -> str:
    return json.dumps(
        [
            {
                "post_id": item.post_db_id,
                "channel": item.channel_username,
                "published_at": item.published_at.isoformat(),
                "text": _normalize_text(item.content),
            }
            for item in items
        ],
        ensure_ascii=False,
    )


def _without_empty_posts(
    items: list[PendingDigestPost],
    subscription: Subscription,
) -> tuple[list[PendingDigestPost], list[DeliveredSummary]]:
    included: list[PendingDigestPost] = []
    skipped: list[DeliveredSummary] = []
    for item in items:
        if _normalize_text(item.content):
            included.append(item)
            continue
        skipped.append(
            DeliveredSummary(
                post_id=item.post_db_id,
                summary_text=None,
                summary_mode=(
                    subscription.summary_mode.value
                    if subscription.digest_format == DigestFormat.SUMMARY
                    else DigestFormat.SHORT.value
                ),
                summary_model=None,
                prompt_snapshot=None,
                status=DELIVERY_STATUS_SKIPPED,
                skip_reason="empty post content",
            )
        )
    return included, skipped


def _chunk_posts_for_prompt(items: list[PendingDigestPost]) -> list[list[PendingDigestPost]]:
    chunks: list[list[PendingDigestPost]] = []
    current: list[PendingDigestPost] = []
    current_size = 0
    for item in items:
        item_size = len(_normalize_text(item.content)) + 200
        if current and current_size + item_size > BATCH_PROMPT_LIMIT:
            chunks.append(current)
            current = [item]
            current_size = item_size
            continue
        current.append(item)
        current_size += item_size
    if current:
        chunks.append(current)
    return chunks


def _parse_json_object(text: str) -> dict[str, Any]:
    stripped = text.strip()
    fenced = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", stripped, flags=re.DOTALL)
    if fenced:
        stripped = fenced.group(1)
    elif not stripped.startswith("{"):
        start = stripped.find("{")
        end = stripped.rfind("}")
        if start >= 0 and end > start:
            stripped = stripped[start : end + 1]
    payload = json.loads(stripped)
    if not isinstance(payload, dict):
        raise ValueError("LLM JSON response is not an object")
    return payload


def _validate_filter_payload(payload: dict[str, Any]) -> None:
    included_post_ids = payload.get("included_post_ids")
    if not isinstance(included_post_ids, list):
        raise ValueError("filter JSON included_post_ids must be a list")
    if any(not _is_int_like(post_id) for post_id in included_post_ids):
        raise ValueError("filter JSON included_post_ids must contain integers")

    skipped_posts = payload.get("skipped_posts")
    if not isinstance(skipped_posts, list):
        raise ValueError("filter JSON skipped_posts must be a list")
    for item in skipped_posts:
        if not isinstance(item, dict):
            raise ValueError("filter JSON skipped_posts entries must be objects")
        if not _is_int_like(item.get("post_id")):
            raise ValueError("filter JSON skipped_posts post_id must be an integer")
        if not isinstance(item.get("reason"), str):
            raise ValueError("filter JSON skipped_posts reason must be a string")


def _validate_topics_payload(payload: dict[str, Any]) -> None:
    topics = payload.get("topics")
    if not isinstance(topics, list):
        raise ValueError("topics JSON topics must be a list")
    for item in topics:
        if not isinstance(item, dict):
            raise ValueError("topics JSON entries must be objects")
        if not isinstance(item.get("title"), str):
            raise ValueError("topics JSON title must be a string")
        if not isinstance(item.get("summary"), str):
            raise ValueError("topics JSON summary must be a string")
        source_post_ids = item.get("source_post_ids")
        if not isinstance(source_post_ids, list) or any(not _is_int_like(post_id) for post_id in source_post_ids):
            raise ValueError("topics JSON source_post_ids must be an integer list")


def _log_preview(text: str, limit: int = 1000) -> str:
    return " ".join((text or "").split())[:limit]


def _topics_from_payload(payload: dict[str, Any], allowed_post_ids: set[int]) -> list[BatchTopic]:
    topics: list[BatchTopic] = []
    for item in payload.get("topics", []):
        if not isinstance(item, dict):
            continue
        source_ids = [int(post_id) for post_id in item.get("source_post_ids", []) if _is_int_like(post_id) and int(post_id) in allowed_post_ids]
        if not source_ids:
            continue
        title = _normalize_text(str(item.get("title") or "Digest item"))
        summary = _normalize_text(str(item.get("summary") or ""))
        if not summary:
            continue
        topics.append(BatchTopic(title=title, summary=summary, source_post_ids=source_ids))
    return topics


def _is_int_like(value: object) -> bool:
    try:
        int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return False
    return True


def _render_batch_digest(topics: list[BatchTopic], items_by_id: dict[int, PendingDigestPost], user_tz: ZoneInfo, language: str) -> str:
    parts: list[str] = []
    sources_label = "Источники" if language == "ru" else "Sources"
    for topic in topics:
        source_links = [_build_header(items_by_id[post_id], user_tz) for post_id in topic.source_post_ids if post_id in items_by_id]
        if not source_links:
            continue
        parts.append(
            "\n".join(
                [
                    f"<b>{escape(topic.title)}</b>",
                    f"• {telegram_safe_html(topic.summary)}",
                    f"{sources_label}: " + ", ".join(source_links),
                ]
            )
        )
    return "\n\n".join(parts)


def _summary_for_post_from_topics(post_id: int, topics: list[BatchTopic]) -> str | None:
    summaries = [topic.summary for topic in topics if post_id in topic.source_post_ids]
    return "\n".join(summaries) if summaries else None


async def _build_short_fallback_messages(
    included_items: list[PendingDigestPost],
    skipped_summaries: list[DeliveredSummary],
    subscription: Subscription,
    user: User,
    user_tz: ZoneInfo,
    prompt_snapshot: str | None,
) -> list[PreparedDigestMessage]:
    del subscription
    messages: list[PreparedDigestMessage] = []
    current_parts: list[str] = []
    current_summaries: list[DeliveredSummary] = []
    current_length = 0

    for item in included_items:
        summary = SummaryResult(
            text=_truncate_text(_normalize_text(item.content), SHORT_ITEM_LIMIT),
            mode=DigestFormat.SHORT.value,
            model_name=None,
            prompt_snapshot=prompt_snapshot,
        )
        rendered = RenderedDigestPost(
            text=f"{_build_header(item, user_tz)}\n{_markdown_to_html(summary.text)}",
            delivered_summary=DeliveredSummary(
                post_id=item.post_db_id,
                summary_text=summary.text,
                summary_mode=summary.mode,
                summary_model=None,
                prompt_snapshot=prompt_snapshot,
                status=DELIVERY_STATUS_DELIVERED,
            ),
        )
        for segment in _split_rendered_post(rendered, user_tz):
            separator = 2 if current_parts else 0
            next_length = current_length + separator + len(segment.text)

            if current_parts and next_length > TELEGRAM_TEXT_LIMIT:
                messages.append(PreparedDigestMessage(text="\n\n".join(current_parts), delivered_summaries=current_summaries))
                current_parts = [segment.text]
                current_summaries = list(segment.delivered_summaries)
                current_length = len(segment.text)
                continue

            current_parts.append(segment.text)
            current_summaries.extend(segment.delivered_summaries)
            current_length = next_length

    if current_parts:
        if skipped_summaries:
            current_summaries.extend(skipped_summaries)
        messages.append(PreparedDigestMessage(text="\n\n".join(current_parts), delivered_summaries=current_summaries))
    return messages or _build_all_skipped_message(user, skipped_summaries)


def _build_all_skipped_message(user: User, skipped_summaries: list[DeliveredSummary]) -> list[PreparedDigestMessage]:
    if not skipped_summaries:
        return []
    text = "Значимых постов для дайджеста нет: пустые, рекламные или низкоценные публикации пропущены."
    if user.language != "ru":
        text = "No meaningful posts for this digest: empty, ad, or low-signal posts were skipped."
    return [PreparedDigestMessage(text=escape(text), delivered_summaries=skipped_summaries)]


def _split_batch_text(text: str, processed_summaries: list[DeliveredSummary]) -> list[PreparedDigestMessage]:
    if len(text) <= TELEGRAM_TEXT_LIMIT:
        return [PreparedDigestMessage(text=text, delivered_summaries=processed_summaries)]
    chunks = _split_sections(text.split("\n\n"), TELEGRAM_TEXT_LIMIT)
    return [
        PreparedDigestMessage(text=chunk, delivered_summaries=processed_summaries if index == len(chunks) - 1 else [])
        for index, chunk in enumerate(chunks)
    ]


def _split_sections(sections: list[str], limit: int) -> list[str]:
    chunks: list[str] = []
    current: list[str] = []
    current_length = 0
    for section in sections:
        separator = 2 if current else 0
        if current and current_length + separator + len(section) > limit:
            chunks.append("\n\n".join(current))
            if len(section) > limit:
                chunks.extend(_split_text(section, limit))
                current = []
                current_length = 0
                continue
            current = [section]
            current_length = len(section)
            continue
        if not current and len(section) > limit:
            chunks.extend(_split_text(section, limit))
            continue
        current.append(section)
        current_length += separator + len(section)
    if current:
        chunks.append("\n\n".join(current))
    return chunks


def _build_header(item: PendingDigestPost, user_tz: ZoneInfo) -> str:
    channel_name = f"@{item.channel_username}" if item.channel_username else "@unknown"
    timestamp = item.published_at.astimezone(user_tz).strftime("%Y-%m-%d %H:%M")
    if item.channel_username:
        link = f"https://t.me/{item.channel_username}/{item.telegram_post_id}"
        return f'<a href="{escape(link, quote=True)}">{escape(channel_name)} | {escape(timestamp)}</a>'
    return f"{escape(channel_name)} | {escape(timestamp)}"


def _normalize_text(text: str) -> str:
    raw_lines = (text or "").splitlines()
    normalized_lines = [" ".join(line.split()) for line in raw_lines]
    collapsed_lines: list[str] = []
    previous_blank = False

    for line in normalized_lines:
        is_blank = line == ""
        if is_blank and previous_blank:
            continue
        collapsed_lines.append(line)
        previous_blank = is_blank

    normalized = "\n".join(collapsed_lines).strip()
    return normalized


def _truncate_text(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[: max(limit - 1, 1)].rstrip() + "…"


def _split_text(text: str, limit: int) -> list[str]:
    chunks: list[str] = []
    remaining = text

    while remaining:
        if len(remaining) <= limit:
            chunks.append(remaining)
            break

        split_at = remaining.rfind(" ", 0, limit)
        if split_at <= 0:
            split_at = limit
        chunks.append(remaining[:split_at].rstrip())
        remaining = remaining[split_at:].lstrip()

    return chunks


def _markdown_to_html(text: str) -> str:
    return telegram_safe_html(text)


class DigestService:
    """Collect pending posts and deliver scheduled digests."""

    def __init__(
        self,
        session_factory: async_sessionmaker,
        bot_token: str,
        llm_settings: LlmSettings | None = None,
        sender: DigestSender | None = None,
        model_pool: OpenRouterModelPool | None = None,
        memory_service: AssistantMemoryService | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._bot_token = bot_token
        self._llm_settings = llm_settings or LlmSettings()
        self._sender = sender
        self._model_pool = model_pool
        self._memory_service = memory_service

    async def run_once(self, now: datetime | None = None) -> int:
        """Deliver all due digests and return the number of users served."""
        if not self._bot_token and self._sender is None:
            logger.warning("Digest delivery skipped: BOT_TOKEN is empty")
            return 0

        delivered_subscriptions = 0
        sent_at = now or datetime.now(timezone.utc)

        async with self._session_factory() as session:
            subscriptions = await SubscriptionRepository(session).list_enabled()

        owned_sender = self._sender is None
        sender = self._sender or BotApiDigestSender(self._bot_token)

        try:
            for subscription in subscriptions:
                if subscription.user is None or not is_digest_due(subscription, subscription.user, sent_at):
                    continue

                async with self._session_factory() as session:
                    delivery_repo = DigestDeliveryRepository(session)
                    current_subscription = await SubscriptionRepository(session).get_by_id(subscription.id)
                    if current_subscription is None or current_subscription.user is None:
                        continue

                    current_user = current_subscription.user
                    items = await delivery_repo.get_pending_posts_for_subscription(current_subscription.id)
                    if not items:
                        continue

                    messages = await build_digest_messages(
                        current_subscription,
                        current_user,
                        items,
                        self._llm_settings,
                        self._model_pool,
                        self._memory_service,
                    )
                    delivered_summaries: list[DeliveredSummary] = []

                    try:
                        for message in messages:
                            await sender.send_message(current_user.chat_id, message.text, parse_mode=message.parse_mode)
                            delivered_summaries.extend(message.delivered_summaries)
                    except Exception:
                        await session.rollback()
                        logger.exception(
                            "Digest delivery failed for subscription_id=%s user_id=%s chat_id=%s",
                            current_subscription.id,
                            current_user.id,
                            current_user.chat_id,
                        )
                        continue

                    await delivery_repo.mark_posts_delivered(
                        current_user.id,
                        current_subscription.id,
                        delivered_summaries,
                        sent_at,
                    )
                    chat_repo = ChatMessageRepository(session)
                    for message in messages:
                        await chat_repo.add_message(
                            user_id=current_user.id,
                            chat_id=current_user.chat_id,
                            role="digest",
                            text=message.text,
                            metadata={"subscription_id": current_subscription.id},
                        )
                    await SubscriptionRepository(session).mark_digest_sent(current_subscription, sent_at)
                    await session.commit()
                    delivered_subscriptions += 1
                    logger.info(
                        "Delivered digest to subscription_id=%s user_id=%s posts=%d messages=%d",
                        current_subscription.id,
                        current_user.id,
                        len({item.post_id for item in delivered_summaries}),
                        len(messages),
                    )
        finally:
            if owned_sender:
                await sender.close()

        return delivered_subscriptions
