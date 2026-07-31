"""LLM telemetry must not alter OpenRouter client behavior."""

import httpx
import pytest

from src.llm.openrouter import OpenRouterClient


@pytest.mark.asyncio
async def test_openrouter_records_available_usage_metadata() -> None:
    records = []

    async def recorder(**kwargs) -> None:
        records.append(kwargs)

    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/chat/completions"
        return httpx.Response(
            200,
            json={
                "choices": [{"message": {"content": "Summary"}}],
                "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15, "cost": 0.001},
            },
        )

    client = OpenRouterClient("key", telemetry_recorder=recorder)
    await client.close()
    client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="https://example.test")
    try:
        assert await client.generate_summary("model", "system", "post") == "Summary"
    finally:
        await client.close()

    assert records == [
        {
            "model": "model",
            "use_case": "summary",
            "status": "success",
            "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15, "cost": 0.001},
            "error": None,
        }
    ]
