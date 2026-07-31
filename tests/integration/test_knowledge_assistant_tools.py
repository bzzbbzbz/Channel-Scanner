"""Assistant integration coverage for terminal grounded knowledge-search output."""

from unittest.mock import AsyncMock

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker

from src.assistant.tools import AssistantToolRegistry
from src.bot.service import BotService, TelegramIdentity
from src.config.settings import BotSettings
from src.knowledge.service import KnowledgeSearchResult


class _Scraper:
    async def fetch_page(self, channel_username, before=None):
        return "<html></html>", 200


@pytest.mark.asyncio
async def test_search_knowledge_is_user_scoped_terminal_output(engine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    bot_service = BotService(session_factory, _Scraper(), BotSettings())
    user = await bot_service.ensure_user(TelegramIdentity(telegram_user_id=88, chat_id=88, chat_type="private", username=None, first_name="U", last_name=None, language_code="ru"))
    knowledge = AsyncMock()
    knowledge.search.return_value = KnowledgeSearchResult(
        query_id=3,
        mode="normal",
        rendered_html='<b>обычный поиск</b>\n<a href="https://t.me/catalog/1">@catalog, 2026-01-01</a>',
        source_post_ids=[1],
        evidence_sufficient=True,
    )
    registry = AssistantToolRegistry(session_factory, _Scraper(), bot_service, knowledge_service=knowledge)

    result = await registry.execute("searchKnowledge", {"scope_type": "subscription", "scope_id": 4, "question": "Что писали о RAG?"}, user)

    knowledge.search.assert_awaited_once_with(user, scope_type="subscription", scope_id=4, question="Что писали о RAG?")
    assert result.ends_turn is True
    assert result.system_message is None
    assert result.additional_system_messages == ['<b>обычный поиск</b>\n<a href="https://t.me/catalog/1">@catalog, 2026-01-01</a>']
