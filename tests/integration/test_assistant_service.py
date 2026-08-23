"""Integration tests for assistant orchestration without real LLM calls."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from src.assistant.service import AssistantAgentService, _resolve_rag_route, _system_prompt
from src.assistant.tools import ToolExecutionResult
from src.bot.service import BotService, TelegramIdentity
from src.config.settings import AssistantSettings, BotSettings, LlmSettings
from src.repository.chat_message import ChatMessageRepository
from src.repository.subscription import SubscriptionRepository


class FakeScraperClient:
    async def fetch_page(self, channel_username: str, before: int | None = None) -> tuple[str, int]:
        del channel_username, before
        return "<html></html>", 200


class FakeMemoryService:
    async def retrieve(self, user, query: str, limit: int = 5) -> list[str]:
        del user, query, limit
        return []

    async def extract_after_turn(self, *, user, user_message: str, assistant_message: str, system_messages: list[str]) -> list[str]:
        del user, user_message, assistant_message, system_messages
        return []


class FakeKnowledgeCatalog:
    async def catalog_snapshot(self) -> list[dict[str, object]]:
        return [{"channel_id": 8, "username": "turboproject", "description": "Широкий каталог ИИ"}]

    async def extract_after_turn(self, *, user, user_message: str, assistant_message: str, system_messages: list[str]) -> list[str]:
        del user, user_message, assistant_message, system_messages
        return []


class FakeAssistantService(AssistantAgentService):
    def __init__(self, *args, subscription_id: int, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._responses = [
            {
                "content": "",
                "tool_calls": [
                    {
                        "id": "tool-1",
                        "type": "function",
                        "function": {
                            "name": "setNotification",
                            "arguments": json.dumps({"subscription_id": subscription_id, "cron": "0 10 * * *"}),
                        },
                    }
                ],
            },
            {"content": "Готово.", "tool_calls": []},
        ]

    async def _complete(self, messages, tools):
        del messages, tools
        return self._responses.pop(0)


class TooManyToolsAssistantService(AssistantAgentService):
    def __init__(self, *args, subscription_id: int, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._subscription_id = subscription_id

    async def _complete(self, messages, tools):
        del messages, tools
        return {
            "content": "",
            "tool_calls": [
                {
                    "id": "tool-1",
                    "type": "function",
                    "function": {
                        "name": "setNotification",
                        "arguments": json.dumps({"subscription_id": self._subscription_id, "cron": "0 10 * * *"}),
                    },
                },
                {
                    "id": "tool-2",
                    "type": "function",
                    "function": {
                        "name": "setDigestFormat",
                        "arguments": json.dumps({"subscription_id": self._subscription_id, "format": "summary"}),
                    },
                },
            ],
        }


class ManualDigestAssistantService(AssistantAgentService):
    async def _complete(self, messages, tools):
        del messages, tools
        return {
            "content": "",
            "tool_calls": [
                {
                    "id": "tool-1",
                    "type": "function",
                    "function": {"name": "generateOnDemandDigest", "arguments": "{}"},
                },
            ],
        }


@pytest.mark.asyncio
async def test_assistant_executes_tool_and_records_visible_messages(engine: AsyncEngine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    bot_service = BotService(session_factory, FakeScraperClient(), BotSettings(default_timezone="UTC"))
    user = await bot_service.ensure_user(
        TelegramIdentity(
            telegram_user_id=7001,
            chat_id=8001,
            chat_type="private",
            username="agent",
            first_name="Agent",
            last_name=None,
            language_code="ru",
        )
    )
    subscription = await bot_service.create_subscription(user.telegram_user_id, "AI")
    assistant = FakeAssistantService(
        settings=AssistantSettings(enabled=True, history_limit=30, max_tool_rounds=5),
        llm_settings=LlmSettings(OPENROUTER_API_KEY="test-key"),
        session_factory=session_factory,
        scraper_client=FakeScraperClient(),
        bot_service=bot_service,
        memory_service=FakeMemoryService(),
        subscription_id=subscription.id,
    )

    result = await assistant.handle_message(user, "сделай уведомления по AI каждый день в 10")

    assert result.system_messages == ["Время уведомлений в подписке <b>AI</b> установлено: каждый день в 10:00."]
    assert result.reply_text == "Готово."
    async with session_factory() as session:
        stored = await SubscriptionRepository(session).get_for_user(user.id, subscription.id)
        assert stored is not None
        assert stored.notification_cron == "0 10 * * *"
        history = await ChatMessageRepository(session).list_recent_for_user(user.id, limit=10)
        assert [item.role for item in history] == ["user", "system", "assistant"]


@pytest.mark.asyncio
async def test_assistant_stops_before_exceeding_tool_call_limit(engine: AsyncEngine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    bot_service = BotService(session_factory, FakeScraperClient(), BotSettings(default_timezone="UTC"))
    user = await bot_service.ensure_user(
        TelegramIdentity(
            telegram_user_id=7002,
            chat_id=8002,
            chat_type="private",
            username="agent",
            first_name="Agent",
            last_name=None,
            language_code="ru",
        )
    )
    subscription = await bot_service.create_subscription(user.telegram_user_id, "AI")
    assistant = TooManyToolsAssistantService(
        settings=AssistantSettings(enabled=True, history_limit=30, max_tool_rounds=10, max_tool_calls=1),
        llm_settings=LlmSettings(OPENROUTER_API_KEY="test-key"),
        session_factory=session_factory,
        scraper_client=FakeScraperClient(),
        bot_service=bot_service,
        memory_service=FakeMemoryService(),
        subscription_id=subscription.id,
    )

    result = await assistant.handle_message(user, "сделай сразу много действий")

    assert result.system_messages == []
    assert result.reply_text == "Достигнут лимит действий ассистента за один запрос: 1. Уточните задачу или разбейте ее на несколько сообщений."
    async with session_factory() as session:
        stored = await SubscriptionRepository(session).get_for_user(user.id, subscription.id)
        assert stored is not None
        assert stored.notification_cron is None


@pytest.mark.asyncio
async def test_assistant_does_not_reply_after_manual_digest_tool(engine: AsyncEngine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    bot_service = BotService(session_factory, FakeScraperClient(), BotSettings(default_timezone="UTC"))
    user = await bot_service.ensure_user(
        TelegramIdentity(
            telegram_user_id=7003,
            chat_id=8003,
            chat_type="private",
            username="agent",
            first_name="Agent",
            last_name=None,
            language_code="ru",
        )
    )
    assistant = ManualDigestAssistantService(
        settings=AssistantSettings(enabled=True, history_limit=30, max_tool_rounds=5),
        llm_settings=LlmSettings(OPENROUTER_API_KEY="test-key"),
        session_factory=session_factory,
        scraper_client=FakeScraperClient(),
        bot_service=bot_service,
        memory_service=FakeMemoryService(),
    )
    assistant._tools.execute = AsyncMock(
        return_value=ToolExecutionResult(
            "generateOnDemandDigest",
            {"digest_messages": ["<b>Digest</b>"]},
            additional_system_messages=["<b>Digest</b>"],
            ends_turn=True,
        )
    )

    result = await assistant.handle_message(user, "собери дайджест AI за вчера")

    assert result.system_messages == ["<b>Digest</b>"]
    assert result.reply_text is None
    assistant._tools.execute.assert_awaited_once()
    async with session_factory() as session:
        history = await ChatMessageRepository(session).list_recent_for_user(user.id, limit=10)
        assert [item.role for item in history] == ["user", "system"]


@pytest.mark.asyncio
async def test_assistant_offers_the_only_ready_catalog_channel_when_description_has_no_matching_term(engine: AsyncEngine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    bot_service = BotService(session_factory, FakeScraperClient(), BotSettings(default_timezone="UTC"))
    user = await bot_service.ensure_user(
        TelegramIdentity(
            telegram_user_id=7004,
            chat_id=8004,
            chat_type="private",
            username="agent",
            first_name="Agent",
            last_name=None,
            language_code="ru",
        )
    )
    assistant = AssistantAgentService(
        settings=AssistantSettings(enabled=True, history_limit=30, max_tool_rounds=5),
        llm_settings=LlmSettings(OPENROUTER_API_KEY="test-key"),
        session_factory=session_factory,
        scraper_client=FakeScraperClient(),
        bot_service=bot_service,
        memory_service=FakeMemoryService(),
        knowledge_service=FakeKnowledgeCatalog(),  # type: ignore[arg-type]
    )
    assistant._tools.execute = AsyncMock(
        return_value=ToolExecutionResult("suggestKnowledgeChannels", {"channels": []})
    )

    result = await assistant.handle_message(user, "Почему гибридный RAG полезнее простого векторного поиска?")

    assert result.reply_text is None
    assert result.system_messages == ["Могу поискать в @turboproject — продолжить?"]
    assistant._tools.execute.assert_awaited_once_with(
        "suggestKnowledgeChannels",
        {"question": "Почему гибридный RAG полезнее простого векторного поиска?"},
        user,
    )


def test_assistant_system_prompt_describes_capabilities_and_natural_language_use() -> None:
    prompt = _system_prompt("ru", max_tool_calls=7, max_subscriptions=3, max_channels=8)

    assert "what you can do or how to use the bot" in prompt
    assert "groups them into named topic subscriptions" in prompt
    assert "write naturally" in prompt
    assert "at most 3 subscriptions per user, 8 channels per subscription, and 7 product tool calls per assistant turn" in prompt
    assert "debugDigestPrompts" in prompt
    assert "generateOnDemandDigest" in prompt
    assert "Current time in the user's timezone" in prompt
    assert "do not produce a final answer, confirmation, summary, quote" in prompt
    assert "prioritize debugDigestPrompts before proposing or changing prompts" in prompt
    assert "do not propose prompts, replay, or mutate settings until they answer" in prompt
    assert "Do not call setFilterPrompt or setSummaryPrompt until the user explicitly confirms" in prompt


def test_catalog_router_selects_only_an_explicit_ready_username() -> None:
    catalog = [{"channel_id": 8, "username": "turboproject", "description": "RAG и ИИ-агенты"}]

    route = _resolve_rag_route("@turboproject почему одного векторного поиска недостаточно?", [], catalog)

    assert route == {
        "kind": "search",
        "channel_id": "8",
        "question": "@turboproject почему одного векторного поиска недостаточно?",
    }


def test_catalog_router_marks_channel_free_question_for_suggestion_without_searching() -> None:
    catalog = [{"channel_id": 8, "username": "turboproject", "description": "RAG и ИИ-агенты"}]

    route = _resolve_rag_route("Почему одного векторного поиска недостаточно?", [], catalog)

    assert route == {"kind": "suggest", "question": "Почему одного векторного поиска недостаточно?"}


def test_catalog_router_reuses_question_after_catalog_selection() -> None:
    catalog = [{"channel_id": 8, "username": "turboproject", "description": "RAG и ИИ-агенты"}]
    history = [
        SimpleNamespace(role="user", text="Почему одного векторного поиска недостаточно?"),
        SimpleNamespace(role="system", text="<b>Не нашёл подходящий канал. Доступный каталог:</b>\n<b>@turboproject</b>"),
        SimpleNamespace(role="user", text="@turboproject"),
    ]

    route = _resolve_rag_route("@turboproject", history, catalog)

    assert route == {
        "kind": "search",
        "channel_id": "8",
        "question": "Почему одного векторного поиска недостаточно?",
    }


def test_catalog_router_does_not_reuse_stale_question_after_another_reply() -> None:
    catalog = [{"channel_id": 8, "username": "turboproject", "description": "RAG и ИИ-агенты"}]
    history = [
        SimpleNamespace(role="user", text="Старый вопрос"),
        SimpleNamespace(role="system", text="Могу поискать в @turboproject — продолжить?"),
        SimpleNamespace(role="user", text="Другая тема"),
        SimpleNamespace(role="system", text="Обычный ответ"),
        SimpleNamespace(role="user", text="@turboproject новый вопрос"),
    ]

    route = _resolve_rag_route("@turboproject новый вопрос", history, catalog)

    assert route == {
        "kind": "search",
        "channel_id": "8",
        "question": "@turboproject новый вопрос",
    }
