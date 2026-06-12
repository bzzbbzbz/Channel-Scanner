"""LLM-first assistant orchestration for free-text bot control."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any

from sqlalchemy.ext.asyncio import async_sessionmaker

from src.assistant.memory import AssistantMemoryService
from src.assistant.tools import AssistantToolRegistry, assistant_tool_schemas
from src.bot.service import BotService
from src.config.settings import AssistantSettings, LlmSettings
from src.llm import MODEL_FALLBACK_CHAIN, ModelUseCase, OpenRouterClient, OpenRouterModelPool
from src.models.user import User
from src.repository.chat_message import ChatMessageRepository
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
    ) -> None:
        self._settings = settings
        self._llm_settings = llm_settings
        self._session_factory = session_factory
        self._memory_service = memory_service
        self._model_pool = model_pool
        self._tools = AssistantToolRegistry(session_factory, scraper_client, bot_service)

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

        memories = await self._memory_service.retrieve(user, text)
        messages = self._build_messages(user, history, memories)
        tools = assistant_tool_schemas()
        system_messages: list[str] = []
        final_text = ""

        try:
            for _ in range(self._settings.max_tool_rounds):
                response = await self._complete(messages, tools)
                tool_calls = response.get("tool_calls") or []
                content = str(response.get("content") or "").strip()
                if not tool_calls:
                    final_text = content
                    break

                assistant_message: dict[str, Any] = {"role": "assistant", "content": content or None, "tool_calls": tool_calls}
                messages.append(assistant_message)

                for tool_call in tool_calls:
                    name, arguments, tool_call_id = _parse_tool_call(tool_call)
                    result = await self._tools.execute(name, arguments, user)
                    if result.system_message:
                        system_messages.append(result.system_message)
                    messages.append(
                        {
                            "role": "tool",
                            "tool_call_id": tool_call_id,
                            "name": name,
                            "content": json.dumps(result.payload, ensure_ascii=False, default=str),
                        }
                    )
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

    async def _complete(self, messages: list[dict[str, Any]], tools: list[dict[str, Any]]) -> dict[str, Any]:
        client = OpenRouterClient(
            api_key=self._llm_settings.openrouter_api_key,
            base_url=self._llm_settings.openrouter_base_url,
            timeout_seconds=self._llm_settings.timeout_seconds,
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
                    response = await client.chat_completion(model, messages, tools=tools)
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

    def _build_messages(self, user: User, history, memories: list[str]) -> list[dict[str, Any]]:
        system_prompt = _system_prompt(user.language)
        messages: list[dict[str, Any]] = [{"role": "system", "content": system_prompt}]
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


def _system_prompt(language: str) -> str:
    response_language = "Russian" if language == "ru" else "English"
    format_instructions = telegram_html_format_instructions(language)
    return f"""
You are the natural-language control assistant for a Telegram digest bot. Reply in {response_language}.

Rules:
- Use product tools for all reads and mutations. Never claim a setting changed unless a tool result confirms it.
- You have no memory tools. Long-term memory is handled by an independent layer outside this prompt.
- For subscription changes based on a name, call getSubscriptions before mutating anything.
- If the requested subscription does not exist, propose creating it.
- If a similar subscription exists, ask whether the user meant the existing one or wants a new subscription.
- Do not silently create or mutate a subscription when the target is ambiguous.
- Notification schedules are 5-field cron expressions. Use setNotification(subscription_id, cron).
- Never set schedules more frequent than every 15 minutes.
- Old UI presets map to cron: hourly is 0 * * * *, daily at 10:00 is 0 10 * * * unless the user gave another time.
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
