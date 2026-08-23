"""Knowledge enrichment and grounded-answer model selection."""

from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timezone

import pytest

from src.config.settings import KnowledgeSettings, LlmSettings
from src.knowledge.search import SourceContext
from src.knowledge.service import KnowledgeService
from src.llm import ModelUseCase
from src.models.channel import Channel
from src.models.post import Post


@pytest.mark.asyncio
async def test_knowledge_answer_generation_uses_fixed_reliable_model() -> None:
    pool = MagicMock()
    pool.refresh_if_due = AsyncMock()
    pool.models_for.return_value = ["free/first:free"]
    client = AsyncMock()
    client.generate_summary.return_value = "valid result"
    service = KnowledgeService(None, KnowledgeSettings(), LlmSettings(openrouter_api_key="key"), pool)

    with patch("src.knowledge.service.OpenRouterClient", return_value=client):
        text, model = await service._generate("system", "content", use_case="knowledge_answer")

    assert (text, model) == ("valid result", "deepseek/deepseek-v4-flash")
    pool.refresh_if_due.assert_not_awaited()
    pool.models_for.assert_not_called()
    pool.record_failure.assert_not_called()
    pool.record_success.assert_not_called()
    client.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_knowledge_answer_retries_its_fixed_model_after_an_invalid_contract() -> None:
    pool = MagicMock()
    client = AsyncMock()
    client.generate_summary.side_effect = ["not json", "valid result"]
    service = KnowledgeService(None, KnowledgeSettings(), LlmSettings(openrouter_api_key="key"), pool)

    with patch("src.knowledge.service.OpenRouterClient", return_value=client):
        text, model = await service._generate(
            "system",
            "content",
            use_case="knowledge_answer",
            validator=lambda candidate: candidate == "valid result",
        )

    assert (text, model) == ("valid result", "deepseek/deepseek-v4-flash")
    assert client.generate_summary.await_count == 2
    assert client.generate_summary.call_args.kwargs["response_format"] == {"type": "json_object"}
    pool.record_failure.assert_not_called()
    pool.record_success.assert_not_called()


@pytest.mark.asyncio
async def test_other_knowledge_generation_retries_http_success_that_breaks_contract() -> None:
    pool = MagicMock()
    pool.refresh_if_due = AsyncMock()
    pool.models_for.return_value = ["free/first:free", "free/second:free"]
    client = AsyncMock()
    client.generate_summary.side_effect = ["not json", '{"claims":[{"text":"supported","cited_post_ids":[1]}],"evidence_sufficient":true,"conflict_detected":false}']
    service = KnowledgeService(None, KnowledgeSettings(), LlmSettings(openrouter_api_key="key"), pool)

    with patch("src.knowledge.service.OpenRouterClient", return_value=client):
        text, model = await service._generate(
            "system",
            "content",
            use_case="knowledge_other",
            validator=lambda candidate: candidate.startswith('{"claims"'),
        )

    assert model == "free/second:free"
    assert '"supported"' in text
    assert pool.record_failure.call_args.args[:2] == (ModelUseCase.SUMMARY, "free/first:free")
    assert pool.record_success.call_args.args == (ModelUseCase.SUMMARY, "free/second:free")


@pytest.mark.asyncio
async def test_knowledge_answer_returns_honest_abstention_when_model_finds_no_support() -> None:
    pool = MagicMock()
    client = AsyncMock()
    client.generate_summary.return_value = '{"claims":[],"evidence_sufficient":false,"conflict_detected":false}'
    service = KnowledgeService(None, KnowledgeSettings(), LlmSettings(openrouter_api_key="key"), pool)
    channel = Channel(id=1, username="catalog")
    source = SourceContext(Post(id=1, channel_id=1, post_id=12, content="нерелевантный пост", datetime=datetime.now(timezone.utc)), channel, "нерелевантный пост", None)

    with patch("src.knowledge.service.OpenRouterClient", return_value=client):
        claims, sufficient, conflict = await service._answer("ru", "вопрос без ответа", [source], timeout=None)

    assert claims == []
    assert sufficient is False
    assert conflict is False
    client.generate_summary.assert_awaited_once()


@pytest.mark.asyncio
async def test_knowledge_answer_keeps_technical_fallback_after_provider_failure() -> None:
    pool = MagicMock()
    client = AsyncMock()
    client.generate_summary.side_effect = RuntimeError("provider stopped")
    service = KnowledgeService(None, KnowledgeSettings(), LlmSettings(openrouter_api_key="key"), pool)
    channel = Channel(id=1, username="catalog")
    source = SourceContext(Post(id=1, channel_id=1, post_id=12, content="пост", datetime=datetime.now(timezone.utc)), channel, "пост", None)

    with patch("src.knowledge.service.OpenRouterClient", return_value=client):
        claims, sufficient, conflict = await service._answer("ru", "вопрос", [source], timeout=None)

    assert len(claims) == 1
    assert claims[0].cited_post_ids == [1]
    assert sufficient is True


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
