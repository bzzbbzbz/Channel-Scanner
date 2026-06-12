"""Small OpenRouter-compatible client for summaries and assistant calls."""

from __future__ import annotations

import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)

MODEL_FALLBACK_CHAIN = [
    "openai/gpt-oss-120b:free",
    "nvidia/nemotron-3-super-120b-a12b:free",
    "google/gemma-4-31b-it:free",
    "openai/gpt-oss-20b:free",
    "google/gemini-2.5-flash-lite",
]

_TOOL_PROBE_SCHEMA = [
    {
        "type": "function",
        "function": {
            "name": "check_tools",
            "description": "Return whether tool calling works.",
            "parameters": {
                "type": "object",
                "properties": {"ok": {"type": "boolean"}},
                "required": ["ok"],
            },
        },
    }
]

_ERROR_BODY_PREVIEW_LIMIT = 1200


class OpenRouterClient:
    """Minimal async client for OpenRouter chat completions."""

    def __init__(self, api_key: str, base_url: str = "https://openrouter.ai/api/v1", timeout_seconds: float = 30.0) -> None:
        self._api_key = api_key
        self._client = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            timeout=timeout_seconds,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
        )

    async def list_models(self, *, order: str = "most-popular", query: str = "free") -> list[str]:
        """List OpenRouter model ids in the requested order when supported."""
        response = await self._client.get("/models", params={"order": order, "q": query})
        response.raise_for_status()
        payload: dict[str, Any] = response.json()
        models = payload.get("data") or []
        result: list[str] = []
        for item in models:
            if not isinstance(item, dict):
                continue
            model_id = item.get("id")
            if isinstance(model_id, str) and model_id:
                result.append(model_id)
        return result

    async def generate_summary(
        self,
        model: str,
        system_prompt: str,
        post_text: str,
        *,
        response_format: dict[str, Any] | None = None,
        require_parameters: bool = False,
    ) -> str:
        payload: dict[str, Any] = {
            "model": model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": post_text},
            ],
        }
        if response_format is not None:
            payload["response_format"] = response_format
        if require_parameters:
            payload["provider"] = {"require_parameters": True}

        response = await self._client.post("/chat/completions", json=payload)
        self._raise_for_status(response, operation="summary generation", model=model)
        payload: dict[str, Any] = response.json()
        choices = payload.get("choices") or []
        if not choices:
            raise ValueError("OpenRouter returned no choices")

        message = choices[0].get("message") or {}
        content = message.get("content")
        if isinstance(content, str):
            text = content.strip()
        else:
            text = "".join(
                part.get("text", "")
                for part in content or []
                if isinstance(part, dict)
            ).strip()
        if not text:
            raise ValueError("OpenRouter returned empty content")
        return text

    async def chat_completion(
        self,
        model: str,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
        tool_choice: Any = "auto",
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "model": model,
            "messages": messages,
        }
        if tools:
            payload["tools"] = tools
            payload["tool_choice"] = tool_choice

        response = await self._client.post("/chat/completions", json=payload)
        self._raise_for_status(response, operation="chat completion", model=model)
        data: dict[str, Any] = response.json()
        choices = data.get("choices") or []
        if not choices:
            raise ValueError("OpenRouter returned no choices")
        message = choices[0].get("message") or {}
        content = message.get("content")
        if isinstance(content, list):
            content = "".join(part.get("text", "") for part in content if isinstance(part, dict))
        return {
            "role": message.get("role", "assistant"),
            "content": (content or "").strip(),
            "tool_calls": message.get("tool_calls") or [],
        }

    async def probe_tool_support(self, model: str) -> bool:
        """Return whether a model responds with a tool call for a harmless probe."""
        response = await self.chat_completion(
            model,
            [
                {"role": "system", "content": "You are checking whether tool calling is available."},
                {"role": "user", "content": "Call the check_tools tool with ok=true."},
            ],
            tools=_TOOL_PROBE_SCHEMA,
            tool_choice={"type": "function", "function": {"name": "check_tools"}},
        )
        return bool(response.get("tool_calls"))

    async def close(self) -> None:
        await self._client.aclose()

    def _raise_for_status(self, response: httpx.Response, *, operation: str, model: str | None = None) -> None:
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError:
            logger.warning(
                "OpenRouter %s failed: model=%s status=%s body_preview=%r",
                operation,
                model,
                response.status_code,
                self._response_preview(response),
                exc_info=True,
            )
            raise

    def _response_preview(self, response: httpx.Response) -> str:
        text = response.text.replace(self._api_key, "<redacted>")
        return text[:_ERROR_BODY_PREVIEW_LIMIT]
