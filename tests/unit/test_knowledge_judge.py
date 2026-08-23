"""Coverage for the content-free semantic claim judge."""

from types import SimpleNamespace

import pytest

from src.knowledge.judge import SemanticJudge, _parse_verdict, build_judge


def test_parse_verdict_handles_valid_and_invalid_outputs() -> None:
    assert _parse_verdict('{"equivalent": true}') is True
    assert _parse_verdict('{"equivalent": false}') is False
    assert _parse_verdict("not json") is None
    assert _parse_verdict('{"equivalent": "maybe"}') is None


@pytest.mark.asyncio
async def test_semantic_judge_forwards_only_claim_content() -> None:
    captured: dict = {}

    class _Client:
        async def chat_completion(self, model, system, user_content, *, response_format=None, use_case=None):
            captured["user_content"] = user_content
            captured["use_case"] = use_case
            captured["model"] = model
            return '{"equivalent": true}'

    judge = SemanticJudge(SimpleNamespace(judge_model="deepseek-v4-flash", judge_version="1"), _Client())

    verdict = await judge.equivalence("заявление модели", (101,), "эталонное заявление", (101,))

    assert verdict is True
    assert captured["use_case"] == "knowledge_answer_judge"
    assert "raw source" not in captured["user_content"]
    assert "заявление модели" in captured["user_content"]


@pytest.mark.asyncio
async def test_semantic_judge_returns_none_on_transport_error() -> None:
    class _Client:
        async def chat_completion(self, model, system, user_content, *, response_format=None, use_case=None):
            raise OSError("timeout")

    judge = SemanticJudge(SimpleNamespace(judge_model="deepseek-v4-flash"), _Client())

    assert await judge.equivalence("a", (101,), "b", (101,)) is None


def test_build_judge_requires_deepseek_key() -> None:
    settings = SimpleNamespace(deepseek_api_key="")
    assert build_judge(settings) is None
