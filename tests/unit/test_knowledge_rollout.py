"""Safety boundaries for the BL-21 controlled RAG candidate."""

import asyncio
from unittest.mock import patch
import hashlib

import pytest
from pydantic import ValidationError

from src.config.settings import KnowledgeSettings, LlmSettings
from src.knowledge.search import RankedPost, render_grounded_answer
from src.knowledge.evaluation import load_answer_audit
from src.knowledge.service import KnowledgeService
from src.llm.openrouter import OpenRouterClient
from src.models.post import Post
from src.models.user import User


def test_candidate_deep_mode_keeps_its_user_visible_search_label() -> None:
    assert "глубокий поиск" in render_grounded_answer("ru", "Ответ", [], mode="deep_rerank")
    assert "deep search" in render_grounded_answer("en", "Answer", [], mode="deep_rerank")


@pytest.mark.asyncio
async def test_slow_telemetry_does_not_block_a_model_call() -> None:
    blocked = asyncio.Event()

    async def recorder(**_kwargs) -> None:
        await blocked.wait()

    client = OpenRouterClient("test", telemetry_recorder=recorder)
    try:
        with patch("src.llm.openrouter._TELEMETRY_RECORD_TIMEOUT_SECONDS", 0.01):
            await client._record_usage(model="test", use_case="test", status="success")
            blocked.set()
            await asyncio.sleep(0)
    finally:
        await client.close()


def test_candidate_is_off_by_default_and_never_global() -> None:
    user = User(telegram_user_id=77, chat_id=77)
    disabled = KnowledgeService(None, KnowledgeSettings(enabled=False), LlmSettings())
    enabled = KnowledgeService(None, KnowledgeSettings(enabled=False, rag_rollout_enabled=True, rag_canary_telegram_ids=[88]), LlmSettings())

    assert disabled.candidate_enabled_for(user) is False
    assert enabled.candidate_enabled_for(user) is False


def test_rerank_candidate_limit_cannot_exceed_twenty() -> None:
    with pytest.raises(ValidationError):
        KnowledgeSettings(rag_rerank_candidate_limit=21)


def test_rerank_candidate_limit_defaults_to_selected_twenty() -> None:
    assert KnowledgeSettings().rag_rerank_candidate_limit == 20


def test_interactive_rag_timeout_settings_allow_measurement_without_an_upper_cap() -> None:
    settings = KnowledgeSettings(
        catalog_selection_timeout_seconds=3600,
        vector_retrieval_timeout_seconds=3600,
        rerank_timeout_seconds=3600,
        answer_timeout_seconds=3600,
        rag_total_timeout_seconds=3600,
        rag_provider_timeout_seconds=3600,
    )

    assert settings.rag_provider_timeout_seconds == 3600


def test_candidate_query_adds_hybrid_facet_only_when_both_signals_are_named() -> None:
    service = KnowledgeService(None, KnowledgeSettings(), LlmSettings())

    expanded = service._candidate_vector_queries("Как соединить графы и векторный поиск для агента?")

    assert len(expanded) == 2
    assert "гибридного RAG" in expanded[1]
    assert len(service._candidate_vector_queries("Как агенту искать код по графу?")) == 1


def test_candidate_query_adds_code_navigation_facet_for_an_architect_question() -> None:
    service = KnowledgeService(None, KnowledgeSettings(), LlmSettings())

    expanded = service._candidate_vector_queries(
        "Как устроить навигацию агента по большой кодовой базе с архитектором?"
    )

    assert len(expanded) == 2
    assert "Claude Code" in expanded[1]


def test_candidate_query_adds_documentation_facets_for_markdown_or_mcp_question() -> None:
    service = KnowledgeService(None, KnowledgeSettings(), LlmSettings())

    expanded = service._candidate_vector_queries("Почему документация в Markdown или MCP ненадёжна для ИИ-агентов?")

    assert len(expanded) == 4
    assert "GRACE-разметка" in expanded[1]
    assert "отравленный контекст" in expanded[2]
    assert "agents.md" in expanded[3]


def test_answer_audit_accepts_only_content_free_scores_for_exact_dataset(tmp_path) -> None:
    dataset_hash = hashlib.sha256(b"labelled dataset").hexdigest()
    path = tmp_path / "audit.json"
    path.write_text(
        '{"dataset_hash":"' + dataset_hash + '","sample_size":12,"judge_version":"manual-v1","faithfulness":0.9,"citation_validity":1,"citation_completeness":0.8,"answer_relevance":0.7}',
        encoding="utf-8",
    )

    audit = load_answer_audit(path, dataset_hash)

    assert audit.sample_size == 12
    assert audit.citation_validity == 1

    path.write_text(path.read_text(encoding="utf-8")[:-1] + ',"raw_answer":"must not persist"}', encoding="utf-8")
    with pytest.raises(ValueError, match="content-free"):
        load_answer_audit(path, dataset_hash)


@pytest.mark.asyncio
async def test_reranker_receives_at_most_twenty_authorized_parent_posts() -> None:
    captured: dict[str, object] = {}

    class FakeClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        async def rerank(self, model, question, documents):
            captured["model"] = model
            captured["question"] = question
            captured["documents"] = documents
            return list(reversed([(index, float(index)) for index in range(len(documents))])), 0.0025

        async def close(self) -> None:
            pass

    settings = KnowledgeSettings(
        enabled=False,
        rag_rollout_enabled=True,
        rag_canary_telegram_ids=[77],
        rag_rerank_candidate_limit=20,
        rag_rerank_max_cost_usd=0.01,
    )
    service = KnowledgeService(None, settings, LlmSettings(openrouter_api_key="test"))
    posts = {index: Post(id=index, channel_id=1, post_id=index, content=f"public post {index}") for index in range(1, 22)}
    ranked = [RankedPost(index, score=float(100 - index)) for index in range(1, 22)]

    with patch("src.knowledge.service.OpenRouterClient", FakeClient):
        outcome = await service.rerank_authorized_posts("question", ranked, posts)

    assert len(captured["documents"]) == 20
    assert captured["documents"] == [f"public post {index}" for index in range(1, 21)]
    assert outcome.fallback_reason is None
    assert outcome.ranked[0].post_id == 20


@pytest.mark.asyncio
async def test_cost_cap_falls_back_before_calling_provider() -> None:
    service = KnowledgeService(
        None,
        KnowledgeSettings(enabled=False, rag_rerank_estimated_cost_usd=0.02, rag_rerank_max_cost_usd=0.01),
        LlmSettings(openrouter_api_key="test"),
    )
    posts = {index: Post(id=index, channel_id=1, post_id=index, content=str(index)) for index in range(1, 3)}
    ranked = [RankedPost(index, score=float(index)) for index in range(1, 3)]

    outcome = await service.rerank_authorized_posts("question", ranked, posts)

    assert outcome.ranked == ranked
    assert outcome.fallback_reason == "cost_cap_preflight"
