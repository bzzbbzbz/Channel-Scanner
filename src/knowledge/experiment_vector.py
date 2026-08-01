"""Isolated, content-free vector candidate primitives for BL-21."""

from __future__ import annotations

import hashlib
import math
import re
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Iterable, Mapping, Protocol, Sequence

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.knowledge.experiment_retriever import CanonicalLexicalCandidateRetriever, LexicalCandidateMode
from src.knowledge.experiments import BudgetExceeded, ExperimentError, config_sha256, normalize_money
from src.models.channel import Channel
from src.models.knowledge import IndexStatus, KnowledgeChannel, KnowledgeChannelState, KnowledgeRepresentation, RepresentationType
from src.models.post import Post


OPERATOR_EMBEDDING_PRICING_VERSION = "operator_embedding_input_usd_0_01_per_million_v1"
OPERATOR_EMBEDDING_PRICING_SOURCE = "operator_override"
OPERATOR_EMBEDDING_PRICE_PER_MILLION = Decimal("0.01")
DEFAULT_EMBEDDING_MODEL = "qwen/qwen3-embedding-8b"
REPRESENTATION_TOKEN_TOTAL = 750_444
VECTOR_POOL_LIMIT = 30
VECTOR_RESULT_LIMIT = 5
VECTOR_RRF_K = 60
PARENT_DIVERSITY_LIMIT = 1
_SAFE_MODEL_ID = re.compile(r"[a-z0-9][a-z0-9._/-]{0,254}\Z")
_SAFE_CANDIDATE_NAME = re.compile(r"vector_(?:summary|full|chunk|all)|hybrid_(?:summary|full|chunk|all)\Z")


class RepresentationAblation(str, Enum):
    SUMMARY = "summary"
    FULL = "full"
    CHUNK = "chunk"
    ALL = "all"


class VectorRetrievalMode(str, Enum):
    VECTOR = "vector"
    HYBRID = "hybrid"


@dataclass(frozen=True, slots=True)
class OperatorEmbeddingPricing:
    """The explicit operator assumption, never inferred from provider metadata."""

    model_id: str = DEFAULT_EMBEDDING_MODEL
    input_usd_per_million_tokens: Decimal = OPERATOR_EMBEDDING_PRICE_PER_MILLION
    version: str = OPERATOR_EMBEDDING_PRICING_VERSION
    source: str = OPERATOR_EMBEDDING_PRICING_SOURCE

    def __post_init__(self) -> None:
        if not _SAFE_MODEL_ID.fullmatch(self.model_id):
            raise ExperimentError("embedding model ID is not a safe operator identifier")
        if self.input_usd_per_million_tokens != OPERATOR_EMBEDDING_PRICE_PER_MILLION:
            raise ExperimentError("BL-21 embedding pricing must use the explicit operator assumption")
        if self.version != OPERATOR_EMBEDDING_PRICING_VERSION or self.source != OPERATOR_EMBEDDING_PRICING_SOURCE:
            raise ExperimentError("BL-21 embedding pricing provenance is invalid")

    def project(self, token_total: int) -> Decimal:
        if not isinstance(token_total, int) or isinstance(token_total, bool) or token_total < 1:
            raise ExperimentError("embedding token total must be a positive integer")
        return normalize_money(Decimal(token_total) * self.input_usd_per_million_tokens / Decimal(1_000_000))


@dataclass(frozen=True, slots=True)
class VectorCandidateConfig:
    hypothesis_id: str
    retrieval_mode: VectorRetrievalMode
    representations: RepresentationAblation
    pool_limit: int = VECTOR_POOL_LIMIT
    result_limit: int = VECTOR_RESULT_LIMIT
    rrf_k: int = VECTOR_RRF_K
    parent_diversity_limit: int = PARENT_DIVERSITY_LIMIT

    def __post_init__(self) -> None:
        if not _SAFE_CANDIDATE_NAME.fullmatch(self.hypothesis_id):
            raise ExperimentError("vector candidate is not allowlisted")
        expected_mode = VectorRetrievalMode.HYBRID if self.hypothesis_id.startswith("hybrid_") else VectorRetrievalMode.VECTOR
        if self.retrieval_mode != expected_mode or self.hypothesis_id.rsplit("_", 1)[1] != self.representations.value:
            raise ExperimentError("vector candidate configuration is inconsistent")
        if self.result_limit < 1 or self.pool_limit < self.result_limit or self.rrf_k < 1 or self.parent_diversity_limit != 1:
            raise ExperimentError("vector candidate bounds are invalid")

    def configuration(self, pricing: OperatorEmbeddingPricing, *, token_total: int = REPRESENTATION_TOKEN_TOTAL) -> dict[str, object]:
        return {
            "hypothesis_id": self.hypothesis_id,
            "retrieval_mode": self.retrieval_mode.value,
            "representation_ablation": self.representations.value,
            "source": "knowledge_representations",
            "result_limit": self.result_limit,
            "pool_limit": self.pool_limit,
            "rrf_k": self.rrf_k,
            "parent_diversity_limit": self.parent_diversity_limit,
            "embedding_model_id": pricing.model_id,
            "embedding_pricing_version": pricing.version,
            "embedding_pricing_source": pricing.source,
            "embedding_input_tokens": token_total,
            "embedding_projected_cost_usd": format(pricing.project(token_total), "f"),
            "optional_model_policy": "free_only",
            "non_embedding_paid_cost_usd": "0.000000",
        }


VECTOR_CANDIDATES = tuple(
    VectorCandidateConfig(
        hypothesis_id=f"{mode.value}_{representation.value}",
        retrieval_mode=mode,
        representations=representation,
    )
    for mode in (VectorRetrievalMode.VECTOR, VectorRetrievalMode.HYBRID)
    for representation in RepresentationAblation
)
_VECTOR_CANDIDATES_BY_ID = {candidate.hypothesis_id: candidate for candidate in VECTOR_CANDIDATES}


@dataclass(frozen=True, slots=True)
class ExperimentVectorIdentity:
    root: Path
    collection_name: str


def vector_candidate_config(candidate_id: str) -> VectorCandidateConfig:
    try:
        return _VECTOR_CANDIDATES_BY_ID[candidate_id]
    except KeyError as exc:
        raise ExperimentError("vector candidate is not allowlisted") from exc


def vector_identity(experiment_root: Path, candidate: VectorCandidateConfig) -> ExperimentVectorIdentity:
    """Allocate a candidate-private root and collection below ``.data-experiment`` only."""
    root = experiment_root.resolve()
    data_root = root / ".data-experiment"
    vector_root = data_root / "vector"
    if not data_root.is_dir() or data_root.is_symlink() or (vector_root.exists() and vector_root.is_symlink()):
        raise ExperimentError("isolated vector root is unsafe")
    candidate_hash = config_sha256(candidate.configuration(OperatorEmbeddingPricing()))
    candidate_root = vector_root / candidate_hash
    if candidate_root.exists() and (candidate_root.is_symlink() or not candidate_root.is_dir()):
        raise ExperimentError("candidate vector root is unsafe")
    candidate_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    resolved_root = candidate_root.resolve()
    if resolved_root.parent != vector_root.resolve() or ".data" in resolved_root.parts:
        raise ExperimentError("candidate vector root escapes the experiment data root")
    return ExperimentVectorIdentity(resolved_root, f"bl21_{candidate_hash[:32]}")


def validate_vector_execution(candidate_id: str, experiment_root: Path, *, token_total: int = REPRESENTATION_TOKEN_TOTAL) -> dict[str, object]:
    """Validate one non-executing, priced vector candidate request for the controlled runner."""
    candidate = vector_candidate_config(candidate_id)
    pricing = OperatorEmbeddingPricing()
    if token_total != REPRESENTATION_TOKEN_TOTAL:
        raise ExperimentError("vector execution must use the manifest-bound representation token total")
    identity = vector_identity(experiment_root, candidate)
    projected = pricing.project(token_total)
    if projected > Decimal("1.00"):
        raise BudgetExceeded("embedding projection exceeds the BL-21 campaign budget")
    configuration = candidate.configuration(pricing, token_total=token_total)
    return {
        "candidate_sha256": config_sha256(configuration),
        "collection_sha256": hashlib.sha256(identity.collection_name.encode("ascii")).hexdigest(),
        "embedding_input_tokens": token_total,
        "embedding_projected_cost_usd": format(projected, "f"),
        "embedding_pricing_source": pricing.source,
        "embedding_pricing_version": pricing.version,
        "optional_model_policy": "free_only",
    }


def validate_non_embedding_cost(cost_usd: Decimal | None, *, remaining_budget_usd: Decimal) -> Decimal:
    """Unknown metadata is never treated as free or as permission for paid work."""
    if cost_usd is None:
        raise BudgetExceeded("non-embedding paid cost metadata is required")
    cost = normalize_money(cost_usd)
    if cost != 0:
        raise BudgetExceeded("BL-21 vector candidates permit only free optional model work")
    if cost > normalize_money(remaining_budget_usd):
        raise BudgetExceeded("non-embedding paid cost exceeds campaign budget")
    return cost


@dataclass(frozen=True, slots=True)
class ExperimentVectorHit:
    post_id: int
    representation_type: str
    ordinal: int | None
    score: float
    channel_id: int
    index_version: int


@dataclass(frozen=True, slots=True)
class VectorCandidateResult:
    telegram_post_ids: tuple[int, ...]
    vector_ms: float
    lexical_ms: float = 0.0


class ExperimentVectorClient(Protocol):
    def collection_exists(self, name: str) -> bool: ...
    def create_collection(self, name: str, *, dimensions: int) -> None: ...
    def upsert(self, name: str, points: Sequence[Mapping[str, object]]) -> None: ...
    def search(self, name: str, vector: Sequence[float], *, limit: int) -> Sequence[Mapping[str, object]]: ...


class LocalExperimentQdrantClient:
    """Lazy local Qdrant wrapper for a future explicitly authorized candidate run."""

    def __init__(self, root: Path) -> None:
        from qdrant_client import QdrantClient

        self._client = QdrantClient(path=str(root))

    def collection_exists(self, name: str) -> bool:
        return bool(self._client.collection_exists(name))

    def create_collection(self, name: str, *, dimensions: int) -> None:
        from qdrant_client.models import Distance, VectorParams

        self._client.create_collection(name, vectors_config=VectorParams(size=dimensions, distance=Distance.COSINE))

    def upsert(self, name: str, points: Sequence[Mapping[str, object]]) -> None:
        from qdrant_client.models import PointStruct

        self._client.upsert(
            name,
            points=[PointStruct(id=point["id"], vector=point["vector"], payload=point["payload"]) for point in points],
            wait=True,
        )

    def search(self, name: str, vector: Sequence[float], *, limit: int) -> Sequence[Mapping[str, object]]:
        if hasattr(self._client, "query_points"):
            points = self._client.query_points(name, query=list(vector), limit=limit).points
        else:
            points = self._client.search(name, query_vector=list(vector), limit=limit)
        return [{"score": float(point.score), "payload": dict(point.payload or {})} for point in points]


def local_experiment_vector_index(identity: ExperimentVectorIdentity, *, dimensions: int) -> "IsolatedExperimentVectorIndex":
    """Build the local client only when an authorized caller elects to use Qdrant."""
    return IsolatedExperimentVectorIndex(identity, LocalExperimentQdrantClient(identity.root), dimensions=dimensions)


class IsolatedExperimentVectorIndex:
    """Qdrant adapter whose only mutable state is one candidate-private collection."""

    def __init__(self, identity: ExperimentVectorIdentity, client: ExperimentVectorClient, *, dimensions: int) -> None:
        if dimensions < 1:
            raise ExperimentError("vector dimensions must be positive")
        self._identity = identity
        self._client = client
        self._dimensions = dimensions

    @property
    def identity(self) -> ExperimentVectorIdentity:
        return self._identity

    def ensure_collection(self) -> None:
        if not self._client.collection_exists(self._identity.collection_name):
            self._client.create_collection(self._identity.collection_name, dimensions=self._dimensions)

    def index(self, records: Sequence[KnowledgeRepresentation], vectors: Sequence[Sequence[float]], *, channel_id: int, index_version: int) -> int:
        if len(records) != len(vectors) or channel_id < 1 or index_version < 1:
            raise ExperimentError("vector index inputs are invalid")
        self.ensure_collection()
        points: list[dict[str, object]] = []
        for record, vector in zip(records, vectors, strict=True):
            if len(vector) != self._dimensions or record.index_version != index_version:
                raise ExperimentError("vector dimensions or index version are invalid")
            points.append({
                "id": record.qdrant_point_id,
                "vector": list(vector),
                "payload": {
                    "post_id": record.post_id,
                    "representation_type": record.representation_type.value,
                    "ordinal": record.ordinal,
                    "channel_id": channel_id,
                    "index_version": index_version,
                },
            })
        self._client.upsert(self._identity.collection_name, points)
        return sum(record.token_count for record in records)

    def search(self, vector: Sequence[float], *, channel_id: int, index_version: int, limit: int) -> list[ExperimentVectorHit]:
        if len(vector) != self._dimensions or channel_id < 1 or index_version < 1 or limit < 1:
            raise ExperimentError("vector search inputs are invalid")
        hits: list[ExperimentVectorHit] = []
        for item in self._client.search(self._identity.collection_name, vector, limit=limit):
            payload = item.get("payload")
            if not isinstance(payload, Mapping):
                continue
            try:
                hit = ExperimentVectorHit(
                    post_id=int(payload["post_id"]),
                    representation_type=str(payload["representation_type"]),
                    ordinal=int(payload["ordinal"]) if payload.get("ordinal") is not None else None,
                    score=float(item["score"]),
                    channel_id=int(payload["channel_id"]),
                    index_version=int(payload["index_version"]),
                )
            except (KeyError, TypeError, ValueError):
                continue
            if hit.channel_id == channel_id and hit.index_version == index_version and math.isfinite(hit.score):
                hits.append(hit)
        return hits


class ExperimentRepresentationRetriever:
    """Clone DB representation retrieval with mandatory parent scope and citation reconstruction."""

    def __init__(self, session: AsyncSession, index: IsolatedExperimentVectorIndex, *, channel_username: str, candidate: VectorCandidateConfig) -> None:
        self._session = session
        self._index = index
        self._candidate = candidate
        self._lexical = CanonicalLexicalCandidateRetriever(session, channel_username=channel_username, result_limit=candidate.pool_limit, pool_limit=candidate.pool_limit)
        self._channel_id: int | None = None
        self._index_version: int | None = None

    async def resolve_channel(self) -> tuple[int, int]:
        channel_id = await self._lexical.resolve_channel()
        row = (await self._session.execute(
            select(KnowledgeChannel.active_index_version)
            .where(KnowledgeChannel.channel_id == channel_id, KnowledgeChannel.state == KnowledgeChannelState.READY)
        )).scalar_one_or_none()
        if not isinstance(row, int) or row < 1:
            raise ExperimentError("approved catalog channel has no active vector index")
        self._channel_id, self._index_version = channel_id, row
        return channel_id, row

    async def representations(self) -> list[KnowledgeRepresentation]:
        if self._channel_id is None or self._index_version is None:
            await self.resolve_channel()
        assert self._channel_id is not None and self._index_version is not None
        statement = (
            select(KnowledgeRepresentation)
            .join(Post, Post.id == KnowledgeRepresentation.post_id)
            .join(KnowledgeChannel, KnowledgeChannel.channel_id == Post.channel_id)
            .where(
                Post.channel_id == self._channel_id,
                KnowledgeChannel.state == KnowledgeChannelState.READY,
                KnowledgeRepresentation.index_version == self._index_version,
                KnowledgeRepresentation.index_status == IndexStatus.INDEXED,
            )
            .order_by(KnowledgeRepresentation.post_id, KnowledgeRepresentation.representation_type, KnowledgeRepresentation.ordinal)
        )
        if self._candidate.representations != RepresentationAblation.ALL:
            statement = statement.where(KnowledgeRepresentation.representation_type == RepresentationType(self._candidate.representations.value))
        return list((await self._session.execute(statement)).scalars())

    async def retrieve(self, query_vector: Sequence[float], *, vector_ms: float = 0.0, query: str | None = None) -> VectorCandidateResult:
        if self._channel_id is None or self._index_version is None:
            await self.resolve_channel()
        assert self._channel_id is not None and self._index_version is not None
        vector_hits = self._index.search(query_vector, channel_id=self._channel_id, index_version=self._index_version, limit=self._candidate.pool_limit)
        vector_parent_ids = _collapse_parent_hits(vector_hits, self._candidate.representations)
        lexical_ms = 0.0
        if self._candidate.retrieval_mode == VectorRetrievalMode.HYBRID:
            if query is None:
                raise ExperimentError("hybrid vector retrieval requires an in-memory query")
            lexical = await self._lexical.retrieve(mode=LexicalCandidateMode.TOKEN_ILIKE, query=query)
            lexical_ms = lexical.lexical_ms
            parent_ids = _rrf_parent_ids(lexical.telegram_post_ids, vector_parent_ids, k=self._candidate.rrf_k)
        else:
            parent_ids = vector_parent_ids
        telegram_ids = await self._reconstruct_canonical_citations(parent_ids[: self._candidate.result_limit])
        return VectorCandidateResult(tuple(telegram_ids), vector_ms, lexical_ms)

    async def _reconstruct_canonical_citations(self, parent_ids: Sequence[int]) -> list[int]:
        if not parent_ids:
            return []
        assert self._channel_id is not None and self._index_version is not None
        rows = (await self._session.execute(
            select(Post.id, Post.post_id, Post.content)
            .join(KnowledgeChannel, KnowledgeChannel.channel_id == Post.channel_id)
            .where(
                Post.id.in_(parent_ids),
                Post.channel_id == self._channel_id,
                KnowledgeChannel.state == KnowledgeChannelState.READY,
                KnowledgeChannel.active_index_version == self._index_version,
            )
        )).all()
        # Reading Post.content here is intentional: it is the sole parent evidence source.
        allowed = {int(parent_id): int(telegram_id) for parent_id, telegram_id, content in rows if isinstance(content, str)}
        return [allowed[parent_id] for parent_id in parent_ids if parent_id in allowed]


def _collapse_parent_hits(hits: Iterable[ExperimentVectorHit], ablation: RepresentationAblation) -> list[int]:
    grouped: dict[int, list[ExperimentVectorHit]] = defaultdict(list)
    allowed = {ablation.value} if ablation != RepresentationAblation.ALL else {item.value for item in RepresentationAblation if item != RepresentationAblation.ALL}
    for hit in hits:
        if hit.representation_type in allowed:
            grouped[hit.post_id].append(hit)
    ranked = []
    for post_id, values in grouped.items():
        primary = max(values, key=lambda item: (item.score, -(item.ordinal or 0), item.representation_type))
        ranked.append((post_id, primary.score + min(0.05, 0.01 * (len(values) - 1))))
    return [post_id for post_id, _score in sorted(ranked, key=lambda item: (-item[1], item[0]))]


def _rrf_parent_ids(lexical_ids: Iterable[int], vector_ids: Iterable[int], *, k: int) -> list[int]:
    if k < 1:
        raise ExperimentError("RRF k must be positive")
    scores: dict[int, float] = defaultdict(float)
    for rank, parent_id in enumerate(_unique_ids(lexical_ids), start=1):
        scores[parent_id] += 1 / (k + rank)
    for rank, parent_id in enumerate(_unique_ids(vector_ids), start=1):
        scores[parent_id] += 1 / (k + rank)
    return [parent_id for parent_id, _score in sorted(scores.items(), key=lambda item: (-item[1], item[0]))]


def _unique_ids(values: Iterable[int]) -> list[int]:
    seen: set[int] = set()
    return [value for value in values if not (value in seen or seen.add(value))]
