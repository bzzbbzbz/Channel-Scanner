"""Minimal async client for the OpenAI-compatible DeepSeek API."""

from __future__ import annotations

import logging
from typing import Any

import httpx

from src.llm.openrouter import UsageRecorder

logger = logging.getLogger(__name__)


class DeepSeekClient:
    """Thin async client for direct DeepSeek chat completions.

    OpenAI-compatible endpoint; used only for bounded knowledge work
    (grounded answers and the offline semantic judge).  Telemetry records
    usage counts and cost without prompt or response content.
    """

    def __init__(
        self,
        api_key: str,
        base_url: str = "https://api.deepseek.com",
        timeout_seconds: float | None = 30.0,
        telemetry_recorder: UsageRecorder | None = None,
    ) -> None:
        self._api_key = api_key
        self._client = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            timeout=timeout_seconds,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
        )
        self._telemetry_recorder = telemetry_recorder

    async def chat_completion(
        self,
        model: str,
        system_prompt: str,
        user_content: str,
        *,
        response_format: dict[str, Any] | None = None,
        use_case: str = "knowledge_answer_direct",
    ) -> str:
        payload: dict[str, Any] = {
            "model": model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_content},
            ],
        }
        if response_format is not None:
            payload["response_format"] = response_format

        try:
            response = await self._client.post("/chat/completions", json=payload)
            response.raise_for_status()
        except Exception as exc:
            await self._record_usage(model=model, use_case=use_case, status="error", error=exc)
            raise
        payload = response.json()
        await self._record_usage(model=model, use_case=use_case, status="success", usage=payload.get("usage"))
        choices = payload.get("choices") or []
        if not choices:
            raise ValueError("DeepSeek returned no choices")
        message = choices[0].get("message") or {}
        content = message.get("content")
        text = content.strip() if isinstance(content, str) else ""
        if not text:
            raise ValueError("DeepSeek returned empty content")
        return text

    async def _record_usage(
        self,
        *,
        model: str,
        use_case: str,
        status: str,
        usage: dict[str, Any] | None = None,
        error: Exception | None = None,
    ) -> None:
        if self._telemetry_recorder is None:
            return
        try:
            await self._telemetry_recorder(
                model=model,
                use_case=use_case,
                status=status,
                usage=usage,
                error=type(error).__name__ if error else None,
            )
        except Exception:
            logger.warning("Could not record DeepSeek usage telemetry", exc_info=True)

    async def close(self) -> None:
        await self._client.aclose()
