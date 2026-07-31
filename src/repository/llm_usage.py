"""Persistence for non-sensitive LLM request telemetry."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any

import logging

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.models.llm_usage import LlmUsage

logger = logging.getLogger(__name__)


class LlmUsageRepository:
    """Record OpenRouter completion metadata without prompt or response content."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def record(
        self,
        *,
        model: str,
        use_case: str,
        status: str,
        usage: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> None:
        usage = usage or {}
        self._session.add(
            LlmUsage(
                model=model,
                use_case=use_case,
                status=status,
                prompt_tokens=_integer_or_none(usage.get("prompt_tokens")),
                completion_tokens=_integer_or_none(usage.get("completion_tokens")),
                total_tokens=_integer_or_none(usage.get("total_tokens")),
                cost=_decimal_or_none(usage.get("cost") or usage.get("total_cost")),
                error=(error or None)[:1000] if error else None,
            )
        )
        await self._session.flush()


def build_usage_recorder(session_factory: async_sessionmaker):
    """Return a callback that stores telemetry independently of a product transaction."""

    async def record(**kwargs: Any) -> None:
        try:
            async with session_factory() as session:
                await LlmUsageRepository(session).record(**kwargs)
                await session.commit()
        except Exception:
            logger.warning("Could not persist LLM usage telemetry", exc_info=True)

    return record


def _integer_or_none(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
