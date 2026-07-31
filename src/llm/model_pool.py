"""Dynamic OpenRouter model selection with health backoff."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from typing import Any

from src.config.settings import LlmSettings
from src.llm.openrouter import EMERGENCY_FALLBACK_MODEL

logger = logging.getLogger(__name__)

# Models with confirmed output-quality failures stay excluded across refreshes and restarts.
EXCLUDED_FREE_MODELS = frozenset({"nvidia/nemotron-nano-9b-v2:free"})
STATIC_SUMMARY_FALLBACK = [
    "openai/gpt-oss-120b:free",
    "nvidia/nemotron-3-super-120b-a12b:free",
    "google/gemma-4-31b-it:free",
    "openai/gpt-oss-20b:free",
    EMERGENCY_FALLBACK_MODEL,
]
STATIC_ASSISTANT_FALLBACK = [
    "openai/gpt-oss-120b:free",
    "nvidia/nemotron-3-super-120b-a12b:free",
    "google/gemma-4-31b-it:free",
    "openai/gpt-oss-20b:free",
    EMERGENCY_FALLBACK_MODEL,
]
MODEL_REFRESH_INTERVAL = timedelta(hours=1)
TOOL_PROBE_ATTEMPTS = 3
MIN_CONFIRMED_ASSISTANT_MODELS = 1
FAILURES_BEFORE_COOLDOWN = 3
COOLDOWN_STEPS = [timedelta(minutes=3), timedelta(minutes=10), timedelta(minutes=30), timedelta(hours=2)]


class ModelUseCase(StrEnum):
    """Independent model health scopes."""

    SUMMARY = "summary"
    ASSISTANT = "assistant"


@dataclass(slots=True)
class ModelHealth:
    """Failure state for one model/use-case pair."""

    consecutive_failures: int = 0
    cooldown_count: int = 0
    disabled_until: datetime | None = None
    last_error: str | None = None


@dataclass(slots=True)
class ToolSupportProbe:
    """Cached assistant tool-support probe result."""

    supported: bool | None = None
    attempts: int = 0
    transient_errors: int = 0
    checked_at: datetime | None = None
    last_error: str | None = None


@dataclass(slots=True)
class ModelPoolSnapshot:
    """Current model ordering for each LLM use case."""

    summary_models: list[str]
    assistant_models: list[str]
    source: str
    refreshed_at: datetime | None = None
    tool_probe_results: dict[str, ToolSupportProbe] = field(default_factory=dict)


def is_free_model_id(model_id: str) -> bool:
    """Return whether an OpenRouter model id is explicitly free."""
    return model_id.endswith(":free")


def build_summary_model_order(model_ids: list[str]) -> list[str]:
    """Build popular-order summary chain with a paid emergency fallback."""
    models = _dedupe(
        [model_id for model_id in model_ids if is_free_model_id(model_id) and model_id not in EXCLUDED_FREE_MODELS]
    )
    return [*models, EMERGENCY_FALLBACK_MODEL]


def build_assistant_model_order(model_ids: list[str], tool_probe_results: dict[str, ToolSupportProbe]) -> list[str]:
    """Build assistant chain from probed free models plus the paid emergency fallback."""
    result: list[str] = []
    for model_id in _dedupe(model_ids):
        if not is_free_model_id(model_id) or model_id in EXCLUDED_FREE_MODELS:
            continue
        probe = tool_probe_results.get(model_id)
        if probe is not None and probe.supported is True:
            result.append(model_id)
    return [*result, EMERGENCY_FALLBACK_MODEL]


class OpenRouterModelPool:
    """In-memory OpenRouter model cache, tool probing, and health policy."""

    def __init__(self, settings: LlmSettings, *, now: datetime | None = None) -> None:
        self._settings = settings
        self._last_refresh_at: datetime | None = None
        self._summary_models: list[str] = []
        self._assistant_models: list[str] = []
        self._tool_probe_results: dict[str, ToolSupportProbe] = {}
        self._health: dict[tuple[ModelUseCase, str], ModelHealth] = {}
        self._created_at = now or _utcnow()

    async def refresh_if_due(self, client, *, force: bool = False, now: datetime | None = None) -> None:
        """Refresh model metadata when stale, retaining last good cache on failure."""
        current = now or _utcnow()
        if not force and self._last_refresh_at is not None and current - self._last_refresh_at < MODEL_REFRESH_INTERVAL:
            return
        if not self._settings.openrouter_api_key:
            logger.info("OpenRouter model refresh skipped: OPENROUTER_API_KEY is empty")
            return

        try:
            model_ids = await client.list_models(order="most-popular", query="free")
        except Exception:
            logger.warning("OpenRouter model refresh failed; keeping cached model order", exc_info=True)
            return

        summary_models = build_summary_model_order(model_ids)
        free_models = [model for model in summary_models if is_free_model_id(model)]
        for model_id in free_models:
            confirmed_free_models = sum(
                self._tool_probe_results.get(candidate, ToolSupportProbe()).supported is True for candidate in free_models
            )
            if confirmed_free_models >= MIN_CONFIRMED_ASSISTANT_MODELS:
                break
            await self._ensure_tool_probe(client, model_id, current)

        assistant_models = build_assistant_model_order(free_models, self._tool_probe_results)
        self._summary_models = summary_models or list(STATIC_SUMMARY_FALLBACK)
        self._assistant_models = assistant_models or list(STATIC_ASSISTANT_FALLBACK)
        self._last_refresh_at = current
        logger.info(
            "OpenRouter model pool refreshed: summary_models=%s assistant_models=%s excluded_from_assistant=%s",
            self._summary_models,
            self._assistant_models,
            [model for model in free_models if model not in assistant_models],
        )

    def models_for(self, use_case: ModelUseCase, *, now: datetime | None = None) -> list[str]:
        """Return healthy models for a use case, with emergency fallback if all are disabled."""
        current = now or _utcnow()
        base_models = self._base_models_for(use_case)
        healthy = [model for model in base_models if not self._is_disabled(use_case, model, current)]
        if healthy:
            return healthy

        fallback = STATIC_SUMMARY_FALLBACK if use_case == ModelUseCase.SUMMARY else STATIC_ASSISTANT_FALLBACK
        logger.warning("All %s models are disabled; using emergency fallback order", use_case.value)
        return list(fallback)

    def record_success(self, use_case: ModelUseCase, model: str) -> None:
        """Reset failure state after a successful model response."""
        state = self._health.get((use_case, model))
        if state is None:
            return
        state.consecutive_failures = 0
        state.disabled_until = None
        state.last_error = None

    def record_failure(self, use_case: ModelUseCase, model: str, error: Exception, *, now: datetime | None = None) -> None:
        """Record a model failure and apply cooldown after repeated failures."""
        current = now or _utcnow()
        state = self._health.setdefault((use_case, model), ModelHealth())
        state.consecutive_failures += 1
        state.last_error = f"{type(error).__name__}: {error}"
        if state.consecutive_failures < FAILURES_BEFORE_COOLDOWN:
            return

        step = COOLDOWN_STEPS[min(state.cooldown_count, len(COOLDOWN_STEPS) - 1)]
        state.cooldown_count += 1
        state.disabled_until = current + step
        state.consecutive_failures = 0
        logger.warning(
            "OpenRouter model temporarily disabled: use_case=%s model=%s disabled_until=%s cooldown=%s last_error=%s",
            use_case.value,
            model,
            state.disabled_until.isoformat(),
            step,
            state.last_error,
        )

    def snapshot(self) -> ModelPoolSnapshot:
        """Return a diagnostic snapshot for tests/logging."""
        source = "openrouter" if self._last_refresh_at is not None else "static-fallback"
        return ModelPoolSnapshot(
            summary_models=self._summary_models or list(STATIC_SUMMARY_FALLBACK),
            assistant_models=self._assistant_models or list(STATIC_ASSISTANT_FALLBACK),
            source=source,
            refreshed_at=self._last_refresh_at,
            tool_probe_results=dict(self._tool_probe_results),
        )

    async def _ensure_tool_probe(self, client, model_id: str, now: datetime) -> None:
        probe = self._tool_probe_results.setdefault(model_id, ToolSupportProbe())
        if probe.supported is not None:
            return

        completed_without_tool = 0
        for _ in range(max(0, TOOL_PROBE_ATTEMPTS - probe.attempts)):
            probe.attempts += 1
            try:
                supported = await client.probe_tool_support(model_id)
            except Exception as exc:
                probe.transient_errors += 1
                probe.last_error = f"{type(exc).__name__}: {exc}"
                logger.warning("Assistant tool-support probe failed: model=%s attempt=%d", model_id, probe.attempts, exc_info=True)
                continue

            if supported:
                probe.supported = True
                probe.checked_at = now
                probe.last_error = None
                logger.info("Assistant tool-support probe result: model=%s supported=True attempts=%d", model_id, probe.attempts)
                return
            completed_without_tool += 1

        if completed_without_tool > 0 and probe.attempts >= TOOL_PROBE_ATTEMPTS:
            probe.supported = False
            probe.checked_at = now
            probe.last_error = "tool call not returned"
            logger.info("Assistant tool-support probe result: model=%s supported=False attempts=%d", model_id, probe.attempts)

    def _base_models_for(self, use_case: ModelUseCase) -> list[str]:
        if use_case == ModelUseCase.SUMMARY:
            return list(self._summary_models or STATIC_SUMMARY_FALLBACK)
        return list(self._assistant_models or STATIC_ASSISTANT_FALLBACK)

    def _is_disabled(self, use_case: ModelUseCase, model: str, now: datetime) -> bool:
        state = self._health.get((use_case, model))
        return state is not None and state.disabled_until is not None and state.disabled_until > now


_default_pool: OpenRouterModelPool | None = None


def get_default_model_pool(settings: LlmSettings) -> OpenRouterModelPool:
    """Return the process-local default model pool."""
    global _default_pool
    if _default_pool is None:
        _default_pool = OpenRouterModelPool(settings)
    return _default_pool


def _dedupe(items: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)
