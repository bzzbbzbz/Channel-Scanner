"""Private BM25-plus-vector retrieval used only by BL-21 experiments."""

from __future__ import annotations

import hashlib
import math
import re
from collections import Counter
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Iterable, Sequence

from src.knowledge.experiments import ExperimentError, config_sha256

_TOKEN = re.compile(r"[\w.-]+", re.UNICODE)
_COLLECTION_PREFIX = "bl21_parent_hybrid_"


class HybridMethod(str, Enum):
    DENSE = "dense"
    BM25 = "bm25"
    RRF_EQUAL = "rrf_equal"
    RRF_DENSE_2 = "rrf_dense_2"
    RRF_BM25_2 = "rrf_bm25_2"
    DBSF = "dbsf"


@dataclass(frozen=True, slots=True)
class HybridPost:
    post_id: int
    vector: tuple[float, ...]
    text: str


@dataclass(frozen=True, slots=True)
class HybridQuery:
    post_ids: tuple[int, ...]
    method: HybridMethod
    confidence: float = 0.0


def bm25_tokens(text: str) -> list[str]:
    """Keep identifiers, dates and abbreviations as searchable word units."""
    return [token.lower() for token in _TOKEN.findall(text) if token]


def sparse_bm25_vector(text: str, *, average_document_length: float, k1: float = 1.2, b: float = 0.75) -> tuple[list[int], list[float]]:
    """Build local BM25 term-frequency weights; Qdrant applies collection IDF."""
    if average_document_length <= 0 or k1 <= 0 or not 0 <= b <= 1:
        raise ExperimentError("BM25 parameters are invalid")
    tokens = bm25_tokens(text)
    if not tokens:
        return [], []
    frequencies = Counter(tokens)
    length = len(tokens)
    values: dict[int, float] = {}
    for token, frequency in frequencies.items():
        denominator = frequency + k1 * (1 - b + b * length / average_document_length)
        index = int.from_bytes(hashlib.sha256(token.encode("utf-8")).digest()[:4], "big")
        values[index] = max(values.get(index, 0.0), frequency * (k1 + 1) / denominator)
    pairs = sorted(values.items())
    return [index for index, _value in pairs], [value for _index, value in pairs]


class PrivateHybridIndex:
    """A private parent-post Qdrant collection; it never opens production data."""

    def __init__(self, root: Path, *, dimensions: int, identity: object) -> None:
        if dimensions < 1:
            raise ExperimentError("hybrid vector dimensions must be positive")
        self._root = root.resolve()
        self._dimensions = dimensions
        self._collection = _COLLECTION_PREFIX + config_sha256(identity)[:32]
        if ".data" in self._root.parts or self._root.is_symlink():
            raise ExperimentError("hybrid index root is unsafe")
        self._client = None
        self._average_document_length: float | None = None

    @property
    def collection_name(self) -> str:
        return self._collection

    def build(self, posts: Sequence[HybridPost]) -> None:
        if not posts or len({post.post_id for post in posts}) != len(posts):
            raise ExperimentError("hybrid index requires unique canonical posts")
        if any(len(post.vector) != self._dimensions or not post.text.strip() for post in posts):
            raise ExperimentError("hybrid post inputs are invalid")
        lengths = [len(bm25_tokens(post.text)) for post in posts]
        if not all(lengths):
            raise ExperimentError("hybrid BM25 input must contain tokens")
        self._average_document_length = sum(lengths) / len(lengths)
        client = self._client_or_create()
        from qdrant_client import models

        # A previous successful local build is reusable.  ``posts`` are still
        # validated and their average length is recalculated above, so a later
        # query has exactly the same BM25 parameters without keeping a second
        # metadata file or rebuilding vectors.
        if client.collection_exists(self._collection):
            return
        client.create_collection(
            self._collection,
            vectors_config={"dense": models.VectorParams(size=self._dimensions, distance=models.Distance.COSINE)},
            sparse_vectors_config={"bm25": models.SparseVectorParams(modifier=models.Modifier.IDF)},
        )
        points = []
        for post in posts:
            indexes, values = sparse_bm25_vector(post.text, average_document_length=self._average_document_length)
            points.append(models.PointStruct(
                id=post.post_id,
                vector={"dense": list(post.vector), "bm25": models.SparseVector(indices=indexes, values=values)},
                payload={"post_id": post.post_id},
            ))
        client.upsert(self._collection, points=points, wait=True)

    def query(self, dense_vector: Sequence[float], question: str, *, method: HybridMethod, pool_limit: int = 30, result_limit: int = 5) -> HybridQuery:
        if self._average_document_length is None or len(dense_vector) != self._dimensions:
            raise ExperimentError("private hybrid index is not ready")
        if pool_limit < result_limit or result_limit < 1 or pool_limit > 100:
            raise ExperimentError("hybrid query limits are invalid")
        indexes, values = sparse_bm25_vector(question, average_document_length=self._average_document_length)
        if not indexes:
            return HybridQuery((), method, 0.0)
        client = self._client_or_create()
        from qdrant_client import models

        dense = models.Prefetch(query=list(dense_vector), using="dense", limit=pool_limit)
        sparse = models.Prefetch(query=models.SparseVector(indices=indexes, values=values), using="bm25", limit=pool_limit)
        if method == HybridMethod.DENSE:
            response = client.query_points(self._collection, query=list(dense_vector), using="dense", limit=result_limit)
        elif method == HybridMethod.BM25:
            response = client.query_points(self._collection, query=models.SparseVector(indices=indexes, values=values), using="bm25", limit=result_limit)
        else:
            query = _fusion_query(method, models)
            response = client.query_points(self._collection, prefetch=[dense, sparse], query=query, limit=result_limit)
        points = [
            point for point in response.points
            if point.payload and isinstance(point.payload.get("post_id"), int)
        ]
        return HybridQuery(
            tuple(int(point.payload["post_id"]) for point in points),
            method,
            float(points[0].score) if points else 0.0,
        )

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    def _client_or_create(self):
        if self._client is None:
            from qdrant_client import QdrantClient

            self._root.mkdir(parents=True, exist_ok=True)
            self._client = QdrantClient(path=str(self._root))
        return self._client


def _fusion_query(method: HybridMethod, models):
    if method == HybridMethod.DBSF:
        return models.FusionQuery(fusion=models.Fusion.DBSF)
    if method == HybridMethod.RRF_EQUAL:
        return models.RrfQuery(rrf=models.Rrf(k=60))
    if method == HybridMethod.RRF_DENSE_2:
        return models.RrfQuery(rrf=models.Rrf(k=60, weights=[2.0, 1.0]))
    if method == HybridMethod.RRF_BM25_2:
        return models.RrfQuery(rrf=models.Rrf(k=60, weights=[1.0, 2.0]))
    raise ExperimentError("hybrid method does not fuse results")


def parent_dense_vectors_from_snapshot(source_root: Path, *, collection_name: str, dimensions: int) -> dict[int, tuple[float, ...]]:
    """Copy one existing dense vector per parent from a private snapshot.

    Preference is full original text, then summary, then a chunk.  This only
    reads the already copied snapshot; it never calls an embedding provider.
    """
    if source_root.is_symlink() or not source_root.is_dir() or not collection_name:
        raise ExperimentError("hybrid snapshot source is unsafe")
    from qdrant_client import QdrantClient

    client = QdrantClient(path=str(source_root))
    try:
        if not client.collection_exists(collection_name):
            raise ExperimentError("hybrid snapshot collection is absent")
        offset = None
        selected: dict[int, tuple[int, tuple[float, ...]]] = {}
        priority = {"full": 0, "summary": 1, "chunk": 2}
        while True:
            points, offset = client.scroll(collection_name, offset=offset, limit=256, with_payload=True, with_vectors=True)
            for point in points:
                payload = point.payload or {}
                post_id = payload.get("post_id")
                representation_type = payload.get("representation_type")
                vector = point.vector
                if not isinstance(post_id, int) or representation_type not in priority:
                    continue
                if isinstance(vector, dict):
                    continue
                values = tuple(float(value) for value in vector)
                if len(values) != dimensions or not all(math.isfinite(value) for value in values):
                    continue
                previous = selected.get(post_id)
                rank = priority[representation_type]
                if previous is None or rank < previous[0]:
                    selected[post_id] = (rank, values)
            if offset is None:
                break
        if not selected:
            raise ExperimentError("hybrid snapshot contains no valid parent vectors")
        return {post_id: vector for post_id, (_priority, vector) in selected.items()}
    finally:
        client.close()
