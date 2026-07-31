"""Coverage for manually labelled retrieval metrics."""

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from src.knowledge.evaluation import EvaluationCase, evaluate_catalog
from src.knowledge.indexer import VectorHit
from src.models.channel import Channel
from src.models.knowledge import KnowledgeChannel, KnowledgeChannelState, KnowledgeEvaluationRun
from src.models.post import Post


def test_evaluation_case_keeps_multiple_manual_relevance_labels() -> None:
    case = EvaluationCase("multi", "question", frozenset({10, 20}))

    assert case.expected_telegram_post_ids == {10, 20}


@pytest.mark.asyncio
async def test_evaluation_persists_parent_level_hybrid_metrics(engine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as session:
        channel = Channel(username="catalog")
        session.add(channel)
        await session.flush()
        catalog = KnowledgeChannel(channel_id=channel.id, state=KnowledgeChannelState.READY)
        first = Post(channel_id=channel.id, post_id=101, content="hybrid retrieval", datetime=datetime.now(timezone.utc))
        second = Post(channel_id=channel.id, post_id=102, content="graph retrieval", datetime=datetime.now(timezone.utc))
        session.add_all([catalog, first, second])
        await session.commit()

    service = SimpleNamespace(
        _settings=SimpleNamespace(index_version=1),
        _vector_search=AsyncMock(return_value=[
            VectorHit(post_id=second.id, representation_type="summary", ordinal=None, score=0.9),
            VectorHit(post_id=second.id, representation_type="full", ordinal=None, score=0.8),
        ]),
    )
    result = await evaluate_catalog(
        session_factory,
        service,
        channel_username="catalog",
        cases=[EvaluationCase("graph", "graph retrieval", frozenset({102}))],
        dataset_hash="a" * 64,
    )

    assert result["recall_at_k"] == 1
    assert result["mrr"] == 1
    assert result["duplicate_source_share"] == 0.5
    async with session_factory() as session:
        record = (await session.execute(select(KnowledgeEvaluationRun))).scalar_one()
    assert record.mode == "hybrid_parent_rrf@5"
    assert record.recall_at_k == 1
