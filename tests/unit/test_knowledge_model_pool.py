"""Knowledge enrichment and grounded-answer model selection."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.config.settings import KnowledgeSettings, LlmSettings
from src.knowledge.service import KnowledgeService
from src.llm import ModelUseCase


@pytest.mark.asyncio
async def test_knowledge_answer_generation_uses_summary_pool_and_records_failover() -> None:
    pool = MagicMock()
    pool.refresh_if_due = AsyncMock()
    pool.models_for.return_value = ["free/first:free", "free/second:free", "deepseek/deepseek-v4-flash"]
    client = AsyncMock()
    client.generate_summary.side_effect = [RuntimeError("first failed"), "valid result"]
    service = KnowledgeService(None, KnowledgeSettings(), LlmSettings(openrouter_api_key="key"), pool)

    with patch("src.knowledge.service.OpenRouterClient", return_value=client):
        text, model = await service._generate("system", "content", use_case="knowledge_answer")

    assert (text, model) == ("valid result", "free/second:free")
    pool.refresh_if_due.assert_awaited_once_with(client)
    pool.models_for.assert_called_once_with(ModelUseCase.SUMMARY)
    pool.record_failure.assert_called_once()
    assert pool.record_failure.call_args.args[:2] == (ModelUseCase.SUMMARY, "free/first:free")
    pool.record_success.assert_called_once_with(ModelUseCase.SUMMARY, "free/second:free")
    client.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_knowledge_enrichment_uses_configured_direct_model() -> None:
    settings = KnowledgeSettings(enrichment_model="deepseek/deepseek-v4-flash")
    service = KnowledgeService(None, settings, LlmSettings(openrouter_api_key="key"), MagicMock())
    client = AsyncMock()
    client.generate_summary.return_value = """{
        "title": "Title",
        "summary": "Summary",
        "topics": [],
        "entities": [],
        "content_type": "news",
        "epistemic_status": "factual",
        "questions_answered": [],
        "claims": []
    }"""

    with patch("src.knowledge.service.OpenRouterClient", return_value=client):
        enrichment, model = await service._enrich("post content")

    assert enrichment.title == "Title"
    assert model == settings.enrichment_model
    client.generate_summary.assert_awaited_once()
    assert client.generate_summary.call_args.args[0] == settings.enrichment_model
    assert client.generate_summary.call_args.kwargs["use_case"] == "knowledge_enrichment"
    client.close.assert_awaited_once()
