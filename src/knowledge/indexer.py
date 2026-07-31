"""One process-local Qdrant index for knowledge representations only."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from src.config.settings import KnowledgeSettings


@dataclass(frozen=True, slots=True)
class VectorHit:
    post_id: int
    representation_type: str
    ordinal: int | None
    score: float


class KnowledgeVectorIndex:
    """Direct local Qdrant access, deliberately isolated from mem0's collection/path."""

    def __init__(self, settings: KnowledgeSettings) -> None:
        self._settings = settings
        self._client = None
        self._ready = False
        self._write_lock = asyncio.Lock()

    async def upsert(self, records, *, channel_id: int, published_at: datetime, language: str | None, content_type: str | None, topics: list[str] | None, vectors: list[list[float]]) -> None:
        async with self._write_lock:
            await asyncio.to_thread(self._upsert_sync, records, channel_id, published_at, language, content_type, topics, vectors)

    async def search(self, vector: list[float], *, channel_ids: set[int], index_version: int, limit: int = 50) -> list[VectorHit]:
        if not channel_ids:
            return []
        return await asyncio.to_thread(self._search_sync, vector, channel_ids, index_version, limit)

    async def delete(self, point_ids: list[str]) -> None:
        if point_ids:
            async with self._write_lock:
                await asyncio.to_thread(self._delete_sync, point_ids)

    def _client_or_create(self):
        if self._client is None:
            from qdrant_client import QdrantClient

            self._client = QdrantClient(path=self._settings.qdrant_path)
        if not self._ready:
            from qdrant_client.models import Distance, VectorParams

            if not self._client.collection_exists(self._settings.collection_name):
                self._client.create_collection(self._settings.collection_name, vectors_config=VectorParams(size=self._settings.embedding_dimensions, distance=Distance.COSINE))
            self._ready = True
        return self._client

    def _upsert_sync(self, records, channel_id: int, published_at: datetime, language: str | None, content_type: str | None, topics: list[str] | None, vectors: list[list[float]]) -> None:
        from qdrant_client.models import PointStruct

        if len(records) != len(vectors):
            raise ValueError("every representation must have one embedding")
        client = self._client_or_create()
        payload_base: dict[str, Any] = {
            "channel_id": channel_id,
            "published_at": published_at.isoformat(),
            "language": language or "",
            "content_type": content_type or "",
            "topics": topics or [],
            "index_version": self._settings.index_version,
        }
        points = [
            PointStruct(
                id=record.qdrant_point_id,
                vector=vector,
                payload={
                    **payload_base,
                    "post_id": record.post_id,
                    "knowledge_document_id": record.knowledge_document_id,
                    "representation_type": record.representation_type.value,
                    "ordinal": record.ordinal,
                },
            )
            for record, vector in zip(records, vectors, strict=True)
        ]
        client.upsert(self._settings.collection_name, points=points, wait=True)

    def _search_sync(self, vector: list[float], channel_ids: set[int], index_version: int, limit: int) -> list[VectorHit]:
        from qdrant_client.models import FieldCondition, Filter, MatchAny

        client = self._client_or_create()
        query_filter = Filter(must=[
            FieldCondition(key="channel_id", match=MatchAny(any=list(channel_ids))),
            FieldCondition(key="index_version", match=MatchAny(any=[index_version])),
        ])
        if hasattr(client, "query_points"):
            points = client.query_points(self._settings.collection_name, query=vector, query_filter=query_filter, limit=limit).points
        else:
            points = client.search(self._settings.collection_name, query_vector=vector, query_filter=query_filter, limit=limit)
        return [
            VectorHit(int(item.payload["post_id"]), str(item.payload["representation_type"]), item.payload.get("ordinal"), float(item.score))
            for item in points
        ]

    def _delete_sync(self, point_ids: list[str]) -> None:
        from qdrant_client.models import PointIdsList

        self._client_or_create().delete(self._settings.collection_name, points_selector=PointIdsList(points=point_ids), wait=True)
