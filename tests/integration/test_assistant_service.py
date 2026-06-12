"""Integration tests for assistant orchestration without real LLM calls."""

from __future__ import annotations

import json

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from src.assistant.service import AssistantAgentService
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
