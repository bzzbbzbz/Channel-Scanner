"""Unit tests for dynamic OpenRouter model selection."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.config.settings import LlmSettings
from src.llm.model_pool import (
    EXCLUDED_FREE_MODELS,
    ModelUseCase,
    OpenRouterModelPool,
    ToolSupportProbe,
    build_assistant_model_order,
    build_summary_model_order,
)
from src.llm.openrouter import EMERGENCY_FALLBACK_MODEL


class FakeOpenRouterClient:
    def __init__(self) -> None:
        self.model_lists: list[list[str]] = []
        self.tool_support: dict[str, bool | Exception] = {}
        self.list_calls = 0
        self.probe_calls: list[str] = []

    async def list_models(self, *, order: str = "most-popular", query: str = "free") -> list[str]:
        assert order == "most-popular"
        assert query == "free"
        self.list_calls += 1
        value = self.model_lists.pop(0)
        if isinstance(value, Exception):
            raise value
        return value

    async def probe_tool_support(self, model: str) -> bool:
        self.probe_calls.append(model)
        value = self.tool_support.get(model, False)
        if isinstance(value, Exception):
            raise value
        return value


def test_build_summary_model_order_keeps_free_popular_order_then_emergency_fallback() -> None:
    models = build_summary_model_order(
        [
            "paid/model",
            "provider/first:free",
            "provider/first:free",
            "provider/second:free",
        ]
    )

    assert models == [
        "provider/first:free",
        "provider/second:free",
        EMERGENCY_FALLBACK_MODEL,
    ]


def test_build_summary_model_order_excludes_permanently_blocked_free_models() -> None:
    blocked_model = "nvidia/nemotron-nano-9b-v2:free"

    models = build_summary_model_order([blocked_model, "provider/usable:free"])

    assert blocked_model in EXCLUDED_FREE_MODELS
    assert models == ["provider/usable:free", EMERGENCY_FALLBACK_MODEL]


def test_build_assistant_model_order_requires_positive_tool_probe() -> None:
    probes = {
        "provider/first:free": ToolSupportProbe(supported=True),
        "provider/second:free": ToolSupportProbe(supported=False),
        "provider/unknown:free": ToolSupportProbe(supported=None),
    }

    models = build_assistant_model_order(
        ["provider/first:free", "provider/second:free", "provider/unknown:free"],
        probes,
    )

    assert models == ["provider/first:free", EMERGENCY_FALLBACK_MODEL]


def test_build_assistant_model_order_excludes_permanently_blocked_free_models() -> None:
    blocked_model = "nvidia/nemotron-nano-9b-v2:free"

    models = build_assistant_model_order([blocked_model], {blocked_model: ToolSupportProbe(supported=True)})

    assert models == [EMERGENCY_FALLBACK_MODEL]


@pytest.mark.asyncio
async def test_refresh_builds_summary_and_assistant_chains_with_tool_probes() -> None:
    client = FakeOpenRouterClient()
    client.model_lists.append(["provider/first:free", "provider/second:free", "paid/model"])
    client.tool_support = {"provider/first:free": False, "provider/second:free": True}
    pool = OpenRouterModelPool(LlmSettings(OPENROUTER_API_KEY="key"))

    await pool.refresh_if_due(client, force=True, now=datetime(2026, 6, 11, tzinfo=timezone.utc))

    snapshot = pool.snapshot()
    assert snapshot.summary_models == [
        "provider/first:free",
        "provider/second:free",
        EMERGENCY_FALLBACK_MODEL,
    ]
    assert snapshot.assistant_models == ["provider/second:free", EMERGENCY_FALLBACK_MODEL]
    assert client.probe_calls == ["provider/first:free", "provider/first:free", "provider/first:free", "provider/second:free"]


@pytest.mark.asyncio
async def test_refresh_stops_inline_tool_probing_after_confirmed_assistant_model() -> None:
    client = FakeOpenRouterClient()
    client.model_lists.append(["provider/first:free", "provider/second:free", "provider/third:free"])
    client.tool_support = {"provider/first:free": True, "provider/second:free": True, "provider/third:free": True}
    pool = OpenRouterModelPool(LlmSettings(OPENROUTER_API_KEY="key"))

    await pool.refresh_if_due(client, force=True, now=datetime(2026, 6, 11, tzinfo=timezone.utc))

    assert pool.snapshot().assistant_models == ["provider/first:free", EMERGENCY_FALLBACK_MODEL]
    assert client.probe_calls == ["provider/first:free"]


@pytest.mark.asyncio
async def test_refresh_failure_reuses_last_successful_cache() -> None:
    client = FakeOpenRouterClient()
    client.model_lists.append(["provider/first:free"])
    client.model_lists.append(RuntimeError("metadata down"))
    client.tool_support = {"provider/first:free": True}
    pool = OpenRouterModelPool(LlmSettings(OPENROUTER_API_KEY="key"))

    await pool.refresh_if_due(client, force=True, now=datetime(2026, 6, 11, 10, tzinfo=timezone.utc))
    await pool.refresh_if_due(client, force=True, now=datetime(2026, 6, 11, 11, tzinfo=timezone.utc))

    assert pool.snapshot().summary_models == [
        "provider/first:free",
        EMERGENCY_FALLBACK_MODEL,
    ]
    assert pool.snapshot().assistant_models == ["provider/first:free", EMERGENCY_FALLBACK_MODEL]


def test_model_health_disables_after_three_failures_and_recovers_after_cooldown() -> None:
    pool = OpenRouterModelPool(LlmSettings())
    model = "openai/gpt-oss-120b:free"
    now = datetime(2026, 6, 11, 10, tzinfo=timezone.utc)

    for _ in range(3):
        pool.record_failure(ModelUseCase.SUMMARY, model, RuntimeError("boom"), now=now)

    assert model not in pool.models_for(ModelUseCase.SUMMARY, now=now)
    assert model in pool.models_for(ModelUseCase.SUMMARY, now=now + timedelta(minutes=4))


def test_model_health_isolated_between_summary_and_assistant() -> None:
    pool = OpenRouterModelPool(LlmSettings())
    model = "openai/gpt-oss-120b:free"
    now = datetime(2026, 6, 11, 10, tzinfo=timezone.utc)

    for _ in range(3):
        pool.record_failure(ModelUseCase.ASSISTANT, model, RuntimeError("tools failed"), now=now)

    assert model not in pool.models_for(ModelUseCase.ASSISTANT, now=now)
    assert model in pool.models_for(ModelUseCase.SUMMARY, now=now)
