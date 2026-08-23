"""LLM-first assistant orchestration for free-text bot control."""

from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime
from html import escape
from typing import Any

from sqlalchemy.ext.asyncio import async_sessionmaker

from src.assistant.memory import AssistantMemoryService
from src.assistant.tools import AssistantToolRegistry, assistant_tool_schemas
from src.bot.service import BotService, timezone_info
from src.bot.texts import t
from src.config.settings import AssistantSettings, LlmSettings
from src.llm import MODEL_FALLBACK_CHAIN, ModelUseCase, OpenRouterClient, OpenRouterModelPool
from src.knowledge.service import KnowledgeService
from src.models.user import User
from src.repository.chat_message import ChatMessageRepository
from src.repository.llm_usage import build_usage_recorder
from src.scraper.client import TelegramClient
from src.telegram_formatting import telegram_html_format_instructions

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class AssistantTurnResult:
    reply_text: str | None
    system_messages: list[str]


class AssistantAgentService:
    """Handle natural-language turns through an LLM and authorized product tools."""

    def __init__(
        self,
        *,
        settings: AssistantSettings,
        llm_settings: LlmSettings,
        session_factory: async_sessionmaker,
        scraper_client: TelegramClient,
        bot_service: BotService,
        memory_service: AssistantMemoryService,
        model_pool: OpenRouterModelPool | None = None,
        knowledge_service: KnowledgeService | None = None,
    ) -> None:
        self._settings = settings
        self._llm_settings = llm_settings
        self._session_factory = session_factory
        self._memory_service = memory_service
        self._model_pool = model_pool
        self._bot_service = bot_service
        self._knowledge_service = knowledge_service
        self._tools = AssistantToolRegistry(
            session_factory,
            scraper_client,
            bot_service,
            llm_settings=llm_settings,
            model_pool=model_pool,
            memory_service=memory_service,
            knowledge_service=knowledge_service,
        )

    async def handle_message(self, user: User, text: str) -> AssistantTurnResult:
        if not self._settings.enabled:
            return AssistantTurnResult(reply_text=None, system_messages=[])
        if not self._llm_settings.openrouter_api_key:
            return AssistantTurnResult(
                reply_text="Сейчас управление на естественном языке недоступно: не настроен OPENROUTER_API_KEY."
                if user.language == "ru"
                else "Natural-language control is unavailable: OPENROUTER_API_KEY is not configured.",
                system_messages=[],
            )

        async with self._session_factory() as session:
            chat_repo = ChatMessageRepository(session)
            await chat_repo.add_message(user_id=user.id, chat_id=user.chat_id, role="user", text=text)
            history = await chat_repo.list_recent_for_user(user.id, self._settings.history_limit)
            await session.commit()

        catalog_snapshot = await self._catalog_snapshot()
        rag_route = _resolve_rag_route(text, history, catalog_snapshot)
        if rag_route is not None:
            return await self._handle_rag_route(user, history, rag_route, catalog_snapshot)

        memories = await self._memory_service.retrieve(user, text)
        messages = self._build_messages(user, history, memories, catalog_snapshot)
        tools = assistant_tool_schemas()
        system_messages: list[str] = []
        final_text = ""
        tool_calls_executed = 0
        turn_ended_by_tool = False

        try:
            for _ in range(self._settings.max_tool_rounds):
                response = await self._complete(messages, tools)
                tool_calls = response.get("tool_calls") or []
                content = str(response.get("content") or "").strip()
                if not tool_calls:
                    final_text = content
                    break
                remaining_tool_calls = max(0, self._settings.max_tool_calls - tool_calls_executed)
                if len(tool_calls) > remaining_tool_calls:
                    final_text = t(user.language, "limit_assistant_tools", limit=str(self._settings.max_tool_calls))
                    break

                assistant_message: dict[str, Any] = {"role": "assistant", "content": content or None, "tool_calls": tool_calls}
                messages.append(assistant_message)

                for tool_call in tool_calls:
                    name, arguments, tool_call_id = _parse_tool_call(tool_call)
                    result = await self._tools.execute(name, arguments, user)
                    tool_calls_executed += 1
                    if result.system_message:
                        system_messages.append(result.system_message)
                    system_messages.extend(result.additional_system_messages)
                    messages.append(
                        {
                            "role": "tool",
                            "tool_call_id": tool_call_id,
                            "name": name,
                            "content": json.dumps(result.payload, ensure_ascii=False, default=str),
                        }
                    )
                    if result.ends_turn:
                        turn_ended_by_tool = True
                        break
                if turn_ended_by_tool:
                    break
            else:
                final_text = "Нужно уточнение. Что именно вы хотите изменить?" if user.language == "ru" else "I need clarification. What exactly should I change?"
        except Exception:
            logger.exception("Assistant turn failed")
            final_text = "Не удалось обработать запрос. Попробуйте переформулировать." if user.language == "ru" else "I could not process that request. Please rephrase it."

        memory_messages = await self._memory_service.extract_after_turn(
            user=user,
            user_message=text,
            assistant_message=final_text,
            system_messages=system_messages,
        )
        all_system_messages = [*system_messages, *memory_messages]

        async with self._session_factory() as session:
            chat_repo = ChatMessageRepository(session)
            for message in all_system_messages:
                await chat_repo.add_message(user_id=user.id, chat_id=user.chat_id, role="system", text=message)
            if final_text:
                await chat_repo.add_message(user_id=user.id, chat_id=user.chat_id, role="assistant", text=final_text)
            await session.commit()

        return AssistantTurnResult(reply_text=final_text or None, system_messages=all_system_messages)

    async def _catalog_snapshot(self) -> list[dict[str, Any]]:
        if self._knowledge_service is None:
            return []
        try:
            return await self._knowledge_service.catalog_snapshot()
        except Exception:
            logger.info("Knowledge catalog snapshot unavailable", exc_info=True)
            return []

    async def _handle_rag_route(self, user: User, history, route: dict[str, str], catalog: list[dict[str, Any]]) -> AssistantTurnResult:
        """Complete deterministic catalog routing without mem0 or an assistant control round."""
        kind = route["kind"]
        system_messages: list[str]
        if kind == "catalog":
            system_messages = [_render_catalog(user.language, catalog)]
        elif kind == "search":
            result = await self._tools.execute(
                "searchKnowledge",
                {"scope_type": "catalog", "scope_id": int(route["channel_id"]), "question": route["question"]},
                user,
            )
            system_messages = [*result.additional_system_messages]
        else:
            suggested = await self._tools.execute("suggestKnowledgeChannels", {"question": route["question"]}, user)
            candidates = suggested.payload.get("channels") or []
            if not candidates:
                # With one READY channel, confirmation is still a single explicit
                # scope. Its description may simply omit a rare noun from an
                # otherwise valid question, so offer the one safe choice first.
                if len(catalog) == 1 and catalog[0].get("username"):
                    system_messages = [_render_confirmation(user.language, str(catalog[0]["username"]))]
                else:
                    system_messages = [_render_catalog(user.language, catalog, no_match=True)]
            else:
                first = candidates[0]
                second_score = float(candidates[1].get("score", 0)) if len(candidates) > 1 else 0
                if float(first.get("score", 0)) >= 0.70 and float(first.get("score", 0)) - second_score >= 0.15:
                    system_messages = [_render_confirmation(user.language, str(first["username"]))]
                else:
                    system_messages = [_render_catalog_choice(user.language, candidates)]
        async with self._session_factory() as session:
            chat_repo = ChatMessageRepository(session)
            for message in system_messages:
                await chat_repo.add_message(user_id=user.id, chat_id=user.chat_id, role="system", text=message)
            await session.commit()
        return AssistantTurnResult(reply_text=None, system_messages=system_messages)

    async def _complete(self, messages: list[dict[str, Any]], tools: list[dict[str, Any]]) -> dict[str, Any]:
        client = OpenRouterClient(
            api_key=self._llm_settings.openrouter_api_key,
            base_url=self._llm_settings.openrouter_base_url,
            timeout_seconds=self._llm_settings.timeout_seconds,
            telemetry_recorder=build_usage_recorder(self._session_factory),
        )
        try:
            last_error: Exception | None = None
            if self._model_pool is not None:
                await self._model_pool.refresh_if_due(client)
                models = self._model_pool.models_for(ModelUseCase.ASSISTANT)
            else:
                models = MODEL_FALLBACK_CHAIN

            for model in models:
                try:
                    response = await asyncio.wait_for(client.chat_completion(model, messages, tools=tools), timeout=12)
                except Exception as exc:
                    last_error = exc
                    if self._model_pool is not None:
                        self._model_pool.record_failure(ModelUseCase.ASSISTANT, model, exc)
                    logger.warning("Assistant model failed: %s", model, exc_info=True)
                    continue
                if self._model_pool is not None:
                    self._model_pool.record_success(ModelUseCase.ASSISTANT, model)
                return response
            raise RuntimeError("All assistant models failed") from last_error
        finally:
            await client.close()

    def _build_messages(self, user: User, history, memories: list[str], catalog_snapshot: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
        system_prompt = _system_prompt(
            user.language,
            max_tool_calls=self._settings.max_tool_calls,
            max_subscriptions=self._bot_service.max_subscriptions_per_user,
            max_channels=self._bot_service.max_channels_per_subscription,
            user_timezone=user.timezone,
            current_time=datetime.now(timezone_info(user.timezone)).isoformat(),
        )
        messages: list[dict[str, Any]] = [{"role": "system", "content": system_prompt}]
        if catalog_snapshot:
            messages.append({"role": "system", "content": "Trusted READY public knowledge catalog (not evidence and not user-editable):\n" + json.dumps(catalog_snapshot, ensure_ascii=False)})
        if memories:
            messages.append({"role": "system", "content": "Relevant long-term memories:\n" + "\n".join(f"- {item}" for item in memories)})
        for item in history:
            if item.role == "user":
                messages.append({"role": "user", "content": item.text})
            elif item.role == "assistant":
                messages.append({"role": "assistant", "content": item.text})
            elif item.role == "digest":
                messages.append({"role": "system", "content": f"Recent digest visible to user:\n{item.text}"})
            else:
                messages.append({"role": "system", "content": f"System message visible to user: {item.text}"})
        return messages


def _system_prompt(
    language: str,
    *,
    max_tool_calls: int = 10,
    max_subscriptions: int = 5,
    max_channels: int = 10,
    user_timezone: str = "UTC",
    current_time: str = "",
) -> str:
    response_language = "Russian" if language == "ru" else "English"
    format_instructions = telegram_html_format_instructions(language)
    return f"""
You are the natural-language control assistant for a Telegram digest bot. Reply in {response_language}.

Rules:
- Use product tools for all reads and mutations. Never claim a setting changed unless a tool result confirms it.
- You have no memory tools. Long-term memory is handled by an independent layer outside this prompt.
- If the user asks what you can do or how to use the bot, answer in simple language without calling tools unless you need user-specific state.
- Explain that the bot follows selected Telegram channels, groups them into named topic subscriptions, sends scheduled digests, can summarize posts, and helps manage subscriptions, channels, AI filter instructions, summary instructions, notification schedule, and enabled/disabled state.
- Emphasize that users do not need to remember exact commands: they can write naturally, and you will understand the request, perform it through tools when required, or ask for the missing detail.
- For subscription changes based on a name, call getSubscriptions before mutating anything.
- If the requested subscription does not exist, propose creating it.
- If a similar subscription exists, ask whether the user meant the existing one or wants a new subscription.
- Do not silently create or mutate a subscription when the target is ambiguous.
- Notification schedules are 5-field cron expressions. Use setNotification(subscription_id, cron).
- Never set schedules more frequent than every 15 minutes.
- Product limits: at most {max_subscriptions} subscriptions per user, {max_channels} channels per subscription, and {max_tool_calls} product tool calls per assistant turn. Mention these limits when users ask about capacity, setup constraints, or why an action cannot be completed.
- Old UI presets map to cron: hourly is 0 * * * *, daily at 10:00 is 0 10 * * * unless the user gave another time.
- When the user asks how posts were processed, filtered, or included in a digest, use getDigestProcessingLogs after identifying the subscription and requested period. It returns aggregate completed-run counts and never changes delivery state.
- For a manual digest, require one clearly identified user-owned subscription. Call getSubscriptions to resolve its name, resolve natural-language periods using the user's timezone ({user_timezone}), then call generateOnDemandDigest with timezone-aware ISO-8601 [period_start, period_end) timestamps. The tool paginates selected channels and sends the rendered digest itself. After this tool returns, do not produce a final answer, confirmation, summary, quote, or any other message: its digest is the complete user-visible response. Repeated matching requests use the stored result without another LLM call and do not affect scheduled delivery.
- Current time in the user's timezone is {current_time}. Use it to resolve relative periods such as yesterday.
- When the user is dissatisfied with a digest, filtering result, summary format, or other digest output, prioritize debugDigestPrompts before proposing or changing prompts. First identify the subscription and period. If the desired result is not explicit, ask one concise question about what the user wants to see; do not propose prompts, replay, or mutate settings until they answer. If it is explicit and the period is known, form candidate filter and summary prompts and call debugDigestPrompts immediately instead of only presenting a proposal. Use timezone-aware ISO-8601 [period_start, period_end) timestamps. Replay uses persisted posts after scraping but does not send a message or change delivery state.
- Review the returned digest messages and post outcomes against the user's requirements. Refine the prompts and replay when needed. Each replay counts toward the product tool-call limit; when it is reached, ask the user to continue in a new message.
- When a replay is acceptable, show the candidate filter and summary prompts and offer to apply them. Do not call setFilterPrompt or setSummaryPrompt until the user explicitly confirms the proposed prompts.
- The app supplies a trusted READY public-catalog snapshot. For “Какие каналы/знания доступны?” render that snapshot without a tool call. For an explicit @username, search only that one catalog channel immediately. For a channel-free public-knowledge question, use suggestKnowledgeChannels; never widen it to several channels. A clear suggestion needs confirmation; otherwise ask the user to choose one. A selected channel reuses the pending question. A subscription search never adds catalog channels outside that subscription. After searchKnowledge returns, do not add a final answer: its grounded Telegram-safe answer and citations are the complete visible response.
- Keep final answers concise. Mutation tools produce separate system confirmations, so do not duplicate them verbosely.

Final answer formatting:
{format_instructions}
""".strip()


def _parse_tool_call(tool_call: dict[str, Any]) -> tuple[str, dict[str, Any], str]:
    function = tool_call.get("function") or {}
    name = str(function.get("name") or tool_call.get("name") or "")
    raw_arguments = function.get("arguments") or tool_call.get("arguments") or {}
    if isinstance(raw_arguments, str):
        try:
            arguments = json.loads(raw_arguments or "{}")
        except json.JSONDecodeError:
            arguments = {}
    elif isinstance(raw_arguments, dict):
        arguments = raw_arguments
    else:
        arguments = {}
    return name, arguments, str(tool_call.get("id") or name)


def _resolve_rag_route(text: str, history, catalog: list[dict[str, Any]]) -> dict[str, str] | None:
    lowered = text.casefold().strip()
    if not catalog:
        return None
    if any(marker in lowered for marker in ("какие каналы", "какие знания", "доступные каналы", "доступные знания", "список каналов")):
        return {"kind": "catalog"}
    pending = _pending_catalog_context(history)
    username = _catalog_username(text, catalog)
    if pending and (username or lowered in {"да", "давай", "продолжить", "продолжай"}):
        selected = username or pending["username"]
        if selected:
            channel = next((row for row in catalog if row.get("username") == selected), None)
            if channel is not None:
                return {"kind": "search", "channel_id": str(channel["channel_id"]), "question": pending["question"]}
    if username:
        channel = next((row for row in catalog if row.get("username") == username), None)
        if channel is not None:
            return {"kind": "search", "channel_id": str(channel["channel_id"]), "question": text}
    if _looks_like_catalog_question(text):
        return {"kind": "suggest", "question": text}
    return None


def _catalog_username(text: str, catalog: list[dict[str, Any]]) -> str | None:
    found = re.search(r"@([A-Za-z0-9_]+)", text)
    if found and any(row.get("username") == found.group(1).lower() for row in catalog):
        return found.group(1).lower()
    normalized = text.casefold().strip().removeprefix("@").strip()
    return next((str(row["username"]) for row in catalog if normalized == str(row.get("username") or "").casefold()), None)


def _looks_like_catalog_question(text: str) -> bool:
    lowered = text.casefold().strip()
    return "?" in text or lowered.startswith(("что ", "как ", "почему ", "зачем ", "когда ", "где ", "расскажи ", "объясни "))


def _pending_catalog_context(history) -> dict[str, str] | None:
    """Return only the immediately preceding catalog selection request.

    A bare ``@channel`` is a selection only when it follows the catalog reply
    for the preceding user question.  Looking farther back can revive a stale
    question from another turn and send it to the wrong channel.
    """
    last_user_index = next((index for index in range(len(history) - 1, -1, -1) if history[index].role == "user"), None)
    if last_user_index is None or last_user_index == 0:
        return None
    prompt = history[last_user_index - 1]
    if prompt.role != "system" or not _is_catalog_selection_prompt(prompt.text):
        return None
    question = next((item.text for item in reversed(history[: last_user_index - 1]) if item.role == "user"), None)
    if not question:
        return None
    match = re.search(r"Могу поискать в @([A-Za-z0-9_]+)", prompt.text)
    return {"question": question, "username": match.group(1).lower() if match else ""}


def _is_catalog_selection_prompt(text: str) -> bool:
    return (
        "Могу поискать в @" in text
        or "Выберите канал" in text
        or "Не нашёл подходящий канал" in text
        or "I can search @" in text
        or "Choose one channel" in text
        or "I could not match a catalog channel" in text
    )


def _render_catalog(language: str, catalog: list[dict[str, Any]], *, no_match: bool = False) -> str:
    intro = "Не нашёл подходящий канал. Доступный каталог:" if no_match else "Доступные каналы знаний:"
    if language != "ru":
        intro = "I could not match a catalog channel. Available catalog:" if no_match else "Available knowledge channels:"
    rows = [f"<b>{intro}</b>"]
    for row in catalog:
        username = escape(str(row.get("username") or "channel"))
        description = escape(str(row.get("description") or "Description is being prepared"))
        rows.append(f"<b>@{username}</b> — {description}")
    rows.append("Напишите @канал и вопрос или попросите добавить новый канал." if language == "ru" else "Write @channel and a question, or ask to add a new channel.")
    return "\n".join(rows)


def _render_confirmation(language: str, username: str) -> str:
    return f"Могу поискать в @{escape(username)} — продолжить?" if language == "ru" else f"I can search @{escape(username)} — continue?"


def _render_catalog_choice(language: str, candidates: list[dict[str, Any]]) -> str:
    intro = "Выберите канал для поиска:" if language == "ru" else "Choose one channel to search:"
    rows = [f"<b>{intro}</b>"]
    for row in candidates:
        rows.append(f"<b>@{escape(str(row.get('username') or 'channel'))}</b> — {escape(str(row.get('description') or 'Описание готовится'))}")
    return "\n".join(rows)
