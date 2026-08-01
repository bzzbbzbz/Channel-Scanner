"""Focused no-provider coverage for the isolated BL-21 vector foundation."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from src.knowledge.experiment_vector import (
    REPRESENTATION_TOKEN_TOTAL,
    ExperimentRepresentationRetriever,
    ExperimentVectorIdentity,
    IsolatedExperimentVectorIndex,
    OperatorEmbeddingPricing,
    RepresentationAblation,
    VectorCandidateConfig,
    VectorRetrievalMode,
    _rrf_parent_ids,
    validate_non_embedding_cost,
    vector_candidate_config,
    vector_identity,
)
from src.knowledge.experiments import BudgetExceeded, ExperimentError, config_sha256
from src.models.channel import Channel
from src.models.knowledge import IndexStatus, KnowledgeChannel, KnowledgeChannelState, KnowledgeDocument, KnowledgeRepresentation, RepresentationType
from src.models.post import Post


class FakeVectorClient:
    def __init__(self, hits: list[dict[str, object]] | None = None) -> None:
        self.collections: set[str] = set()
        self.points: dict[str, list[dict[str, object]]] = {}
        self.hits = hits or []

    def collection_exists(self, name: str) -> bool:
        return name in self.collections

    def create_collection(self, name: str, *, dimensions: int) -> None:
        assert dimensions == 2
        self.collections.add(name)

    def upsert(self, name: str, points) -> None:
        self.points[name] = list(points)

    def search(self, name: str, vector, *, limit: int):
        assert name in self.collections
        return self.hits[:limit]


def _candidate(name: str = "vector_all") -> VectorCandidateConfig:
    return vector_candidate_config(name)


def test_candidate_identity_is_unique_under_data_experiment_and_never_the_production_collection(tmp_path: Path) -> None:
    (tmp_path / ".data-experiment").mkdir()
    first = vector_identity(tmp_path, _candidate("vector_summary"))
    second = vector_identity(tmp_path, _candidate("vector_full"))

    assert first.root.parent == tmp_path / ".data-experiment" / "vector"
    assert first.root != second.root
    assert first.collection_name != second.collection_name
    assert "knowledge" not in first.root.parts
    assert first.collection_name != "telegram_channel_knowledge"


def test_representation_ablation_hash_and_operator_projection_are_fixed_and_content_free() -> None:
    pricing = OperatorEmbeddingPricing()
    configurations = [_candidate(name).configuration(pricing) for name in (
        "vector_summary", "vector_full", "vector_chunk", "vector_all",
        "hybrid_summary", "hybrid_full", "hybrid_chunk", "hybrid_all",
    )]

    assert len({config_sha256(config) for config in configurations}) == 8
    assert pricing.project(REPRESENTATION_TOKEN_TOTAL) == Decimal("0.007504")
    assert all(config["embedding_pricing_source"] == "operator_override" for config in configurations)
    assert all("text" not in str(config).lower() and "content" not in str(config).lower() for config in configurations)
    with pytest.raises(BudgetExceeded):
        validate_non_embedding_cost(None, remaining_budget_usd=Decimal("1.00"))
    with pytest.raises(BudgetExceeded):
        validate_non_embedding_cost(Decimal("0.000001"), remaining_budget_usd=Decimal("1.00"))


def test_isolated_index_keeps_candidate_collections_separate_without_cleanup() -> None:
    client = FakeVectorClient()
    first = IsolatedExperimentVectorIndex(ExperimentVectorIdentity(Path("/tmp/vector-a"), "bl21_a"), client, dimensions=2)
    second = IsolatedExperimentVectorIndex(ExperimentVectorIdentity(Path("/tmp/vector-b"), "bl21_b"), client, dimensions=2)

    first.ensure_collection()
    second.ensure_collection()

    assert client.collections == {"bl21_a", "bl21_b"}
    assert not hasattr(first, "delete")


def test_rrf_and_parent_diversity_are_stable_under_ties() -> None:
    assert _rrf_parent_ids([3, 1, 1], [1, 3, 3], k=60) == [1, 3]
    assert _rrf_parent_ids([9, 4], [4, 9], k=60) == [4, 9]


@pytest.mark.asyncio
async def test_vector_retriever_reconstructs_only_scoped_canonical_post_citations(engine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(timezone.utc)
    async with session_factory() as session:
        catalog = Channel(username="catalog")
        outside = Channel(username="outside")
        session.add_all([catalog, outside])
        await session.flush()
        session.add_all([
            KnowledgeChannel(channel_id=catalog.id, state=KnowledgeChannelState.READY, active_index_version=3),
            Post(channel_id=catalog.id, post_id=10, content="canonical evidence", datetime=now),
            Post(channel_id=outside.id, post_id=11, content="outside evidence", datetime=now),
        ])
        await session.flush()
        inside_post = (await session.execute(select(Post).where(Post.channel_id == catalog.id))).scalar_one()
        document = KnowledgeDocument(post_id=inside_post.id, source_content_hash="a" * 64)
        session.add(document)
        await session.flush()
        session.add(KnowledgeRepresentation(
            knowledge_document_id=document.id,
            post_id=inside_post.id,
            representation_type=RepresentationType.SUMMARY,
            ordinal=None,
            text="representation only",
            text_hash="b" * 64,
            token_count=2,
            qdrant_point_id="c" * 64,
            index_version=3,
            index_status=IndexStatus.INDEXED,
        ))
        await session.commit()

    client = FakeVectorClient([
        {"id": "c" * 64, "score": 0.9, "payload": {"post_id": inside_post.id, "representation_type": "summary", "ordinal": None, "channel_id": catalog.id, "index_version": 3}},
        {"id": "d" * 64, "score": 0.99, "payload": {"post_id": 999, "representation_type": "summary", "ordinal": None, "channel_id": outside.id, "index_version": 3}},
        {"id": "e" * 64, "score": 0.98, "payload": {"post_id": 999, "representation_type": "summary", "ordinal": None, "channel_id": catalog.id, "index_version": 2}},
    ])
    identity = ExperimentVectorIdentity(Path("/tmp/vector-test"), "bl21_scope")
    index = IsolatedExperimentVectorIndex(identity, client, dimensions=2)
    index.ensure_collection()
    async with session_factory() as session:
        retriever = ExperimentRepresentationRetriever(session, index, channel_username="catalog", candidate=_candidate("vector_summary"))
        result = await retriever.retrieve([0.0, 1.0])

    assert result.telegram_post_ids == (10,)
    assert "canonical evidence" not in repr(result)


@pytest.mark.asyncio
async def test_hybrid_rrf_uses_parent_db_ids_and_rejects_forged_or_stale_hits(engine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(timezone.utc)
    async with session_factory() as session:
        catalog = Channel(username="catalog")
        session.add(catalog)
        await session.flush()
        session.add(KnowledgeChannel(channel_id=catalog.id, state=KnowledgeChannelState.READY, active_index_version=3))
        # The first parent DB ID deliberately equals the second Telegram post ID.
        vector_post = Post(channel_id=catalog.id, post_id=100, content="alpha vector", datetime=now)
        lexical_post = Post(channel_id=catalog.id, post_id=1, content="alpha lexical", datetime=now)
        session.add_all([vector_post, lexical_post])
        await session.flush()
        valid_document = KnowledgeDocument(post_id=vector_post.id, source_content_hash="a" * 64)
        stale_document = KnowledgeDocument(post_id=lexical_post.id, source_content_hash="b" * 64)
        session.add_all([valid_document, stale_document])
        await session.flush()
        session.add_all([
            KnowledgeRepresentation(
                knowledge_document_id=valid_document.id, post_id=vector_post.id,
                representation_type=RepresentationType.SUMMARY, ordinal=None, text="valid", text_hash="c" * 64,
                token_count=1, qdrant_point_id="d" * 64, index_version=3, index_status=IndexStatus.INDEXED,
            ),
            KnowledgeRepresentation(
                knowledge_document_id=stale_document.id, post_id=lexical_post.id,
                representation_type=RepresentationType.SUMMARY, ordinal=None, text="stale", text_hash="e" * 64,
                token_count=1, qdrant_point_id="f" * 64, index_version=3, index_status=IndexStatus.FAILED,
            ),
        ])
        await session.commit()

    client = FakeVectorClient([
        # A forged point cannot claim the valid representation by copying its payload.
        {"id": "0" * 64, "score": 1.0, "payload": {"post_id": vector_post.id, "representation_type": "summary", "ordinal": None, "channel_id": catalog.id, "index_version": 3}},
        # A point from a failed representation and a stale representation type are ineligible.
        {"id": "f" * 64, "score": 0.99, "payload": {"post_id": lexical_post.id, "representation_type": "summary", "ordinal": None, "channel_id": catalog.id, "index_version": 3}},
        {"id": "d" * 64, "score": 0.98, "payload": {"post_id": vector_post.id, "representation_type": "full", "ordinal": None, "channel_id": catalog.id, "index_version": 3}},
        {"id": "d" * 64, "score": 0.9, "payload": {"post_id": vector_post.id, "representation_type": "summary", "ordinal": None, "channel_id": catalog.id, "index_version": 3}},
    ])
    index = IsolatedExperimentVectorIndex(ExperimentVectorIdentity(Path("/tmp/vector-test"), "bl21_adversarial"), client, dimensions=2)
    index.ensure_collection()
    async with session_factory() as session:
        retriever = ExperimentRepresentationRetriever(session, index, channel_username="catalog", candidate=_candidate("hybrid_summary"))
        result = await retriever.retrieve([0.0, 1.0], query="alpha")

    assert result.telegram_post_ids == (100, 1)


@pytest.mark.asyncio
async def test_vector_retriever_rejects_catalog_without_an_active_index(engine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as session:
        channel = Channel(username="catalog")
        session.add(channel)
        await session.flush()
        session.add(KnowledgeChannel(channel_id=channel.id, state=KnowledgeChannelState.READY, active_index_version=None))
        await session.commit()
        index = IsolatedExperimentVectorIndex(ExperimentVectorIdentity(Path("/tmp/vector-test"), "bl21_none"), FakeVectorClient(), dimensions=2)
        retriever = ExperimentRepresentationRetriever(session, index, channel_username="catalog", candidate=_candidate())
        with pytest.raises(ExperimentError, match="active vector index"):
            await retriever.resolve_channel()
