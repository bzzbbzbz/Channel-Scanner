"""Coverage for the direct DeepSeek chat client."""

from types import SimpleNamespace

import pytest

from src.llm.deepseek import DeepSeekClient


class _FakeResponse:
    def __init__(self, json_value=None, status_error: str | None = None) -> None:
        self._json_value = json_value
        self._status_error = status_error

    def raise_for_status(self) -> None:
        if self._status_error:
            raise RuntimeError(self._status_error)

    def json(self) -> dict:
        return self._json_value


class _FakeHttpClient:
    def __init__(self) -> None:
        self.calls: list[dict] = []
        self._responses: list[_FakeResponse] = []

    def enqueue(self, response: _FakeResponse) -> None:
        self._responses.append(response)

    async def post(self, url, json):
        self.calls.append({"url": url, "json": json})
        return self._responses.pop(0)

    async def aclose(self) -> None:
        pass


@pytest.mark.asyncio
async def test_deepseek_client_returns_content_and_records_usage() -> None:
    fake = _FakeHttpClient()
    fake.enqueue(_FakeResponse({"choices": [{"message": {"content": '{"equivalent": true}'}}], "usage": {"prompt_tokens": 10}}))
    recorded: list[dict] = []

    async def recorder(**kwargs) -> None:
        recorded.append(kwargs)

    client = DeepSeekClient("test-key", telemetry_recorder=recorder)
    client._client = fake  # type: ignore[assignment]
    try:
        text = await client.chat_completion(
            "deepseek-v4-flash",
            "system",
            "user",
            response_format={"type": "json_object"},
            use_case="knowledge_answer_judge",
        )
    finally:
        await client.close()

    assert text == '{"equivalent": true}'
    assert fake.calls[0]["url"].endswith("/chat/completions")
    assert fake.calls[0]["json"]["model"] == "deepseek-v4-flash"
    assert recorded and recorded[0]["status"] == "success"
    assert recorded[0]["use_case"] == "knowledge_answer_judge"
    assert recorded[0]["usage"]["prompt_tokens"] == 10


@pytest.mark.asyncio
async def test_deepseek_client_records_errors_and_raises() -> None:
    fake = _FakeHttpClient()
    fake.enqueue(_FakeResponse(status_error="boom"))
    recorded: list[dict] = []

    async def recorder(**kwargs) -> None:
        recorded.append(kwargs)

    client = DeepSeekClient("test-key", telemetry_recorder=recorder)
    client._client = fake  # type: ignore[assignment]
    try:
        with pytest.raises(RuntimeError, match="boom"):
            await client.chat_completion("deepseek-v4-flash", "system", "user")
    finally:
        await client.close()

    assert recorded and recorded[0]["status"] == "error"
    assert recorded[0]["error"] == "RuntimeError"


@pytest.mark.asyncio
async def test_deepseek_client_rejects_empty_content() -> None:
    fake = _FakeHttpClient()
    fake.enqueue(_FakeResponse({"choices": [{"message": {"content": "   "}}]}))
    client = DeepSeekClient("test-key")
    client._client = fake  # type: ignore[assignment]
    try:
        with pytest.raises(ValueError, match="empty"):
            await client.chat_completion("deepseek-v4-flash", "system", "user")
    finally:
        await client.close()
