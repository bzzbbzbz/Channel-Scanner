"""The isolated reranking client validates untrusted provider output."""

import json

import httpx
import pytest

from src.llm.openrouter import OpenRouterClient, RerankResult


@pytest.mark.asyncio
async def test_rerank_returns_provider_indexes_without_retaining_documents() -> None:
    seen = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.update(json.loads(request.content))
        return httpx.Response(200, json={"results": [
            {"index": 1, "relevance_score": 0.9},
            {"index": 0, "relevance_score": 0.4},
        ], "usage": {"search_units": 1}})

    client = OpenRouterClient("key")
    await client.close()
    client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="https://example.test")
    try:
        result = await client.rerank("cohere/rerank-4-pro", "Which post?", ["first", "second"], top_n=2)
    finally:
        await client.close()

    assert result == [RerankResult(1, 0.9), RerankResult(0, 0.4)]
    assert seen == {"model": "cohere/rerank-4-pro", "query": "Which post?", "documents": ["first", "second"], "top_n": 2}


@pytest.mark.asyncio
async def test_rerank_rejects_duplicate_or_out_of_range_indexes() -> None:
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"results": [
            {"index": 3, "relevance_score": 0.9},
        ]})

    client = OpenRouterClient("key")
    await client.close()
    client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="https://example.test")
    try:
        with pytest.raises(ValueError, match="invalid rerank result"):
            await client.rerank("cohere/rerank-4-pro", "Question", ["only"], top_n=1)
    finally:
        await client.close()
