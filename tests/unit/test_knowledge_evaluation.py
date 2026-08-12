"""Coverage for manually labelled retrieval metrics."""

from datetime import datetime, timezone
from collections import Counter
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from src.knowledge.evaluation import EvaluationCase, evaluate_catalog, evaluation_run, load_dataset, load_dataset_bytes, split_evaluation_cases
from src.knowledge.experiments import EvaluationMetrics, RetrievalMetrics
from src.knowledge.indexer import VectorHit
from src.models.channel import Channel
from src.models.knowledge import KnowledgeChannel, KnowledgeChannelState, KnowledgeEvaluationRun
from src.models.post import Post


def test_evaluation_case_keeps_multiple_manual_relevance_labels() -> None:
    case = EvaluationCase("multi", "question", frozenset({10, 20}))

    assert case.expected_telegram_post_ids == {10, 20}


def test_labelled_dataset_supports_fixed_phases_and_no_answer_cases() -> None:
    cases, _digest = load_dataset_bytes(
        b'{"id":"known","question":"What is BM25?","expected_telegram_post_ids":[7],"category":"technical","phase":"development"}\n'
        b'{"id":"unknown","question":"What is not in this channel?","expected_telegram_post_ids":[],"category":"no_answer","phase":"holdout"}\n'
    )

    development, holdout = split_evaluation_cases(cases)

    assert [case.id for case in development] == ["known"]
    assert [case.id for case in holdout] == ["unknown"]
    assert not holdout[0].expects_answer


def test_no_answer_label_must_have_no_expected_post() -> None:
    with pytest.raises(ValueError, match="invalid labels"):
        load_dataset_bytes(
            b'{"id":"bad","question":"Question","expected_telegram_post_ids":[7],"category":"no_answer","phase":"development"}\n'
        )


def test_expanded_experiment_dataset_is_stratified_and_complete() -> None:
    cases, _digest = load_dataset(Path(".data-experiment/inputs/turboproject-ai-expanded-100.jsonl"))

    assert Counter(case.category for case in cases) == {
        "technical": 50,
        "conversational": 20,
        "exact": 15,
        "no_answer": 15,
    }
    assert Counter(case.phase for case in cases) == {"development": 70, "holdout": 30}


def test_candidate_evaluation_record_is_content_free_and_stable() -> None:
    record = evaluation_run(
        phase="dev",
        metrics=EvaluationMetrics(2, RetrievalMetrics(1.0, 0.5, 0.75), 0.0, 1.0, 0),
        phase_timings_ms={"retrieval": [2.0, 4.0]},
    ).record()

    assert record == {
        "phase": "dev",
        "metrics": {
            "case_count": 2,
            "recall_at_k": 1.0,
            "mrr": 0.5,
            "ndcg": 0.75,
            "duplicate_source_share": 0.0,
            "source_diversity": 1.0,
            "insufficient_evidence_count": 0,
            "no_answer_case_count": 0,
            "correct_no_answer_count": 0,
            "false_no_answer_count": 0,
        },
        "percentiles": {"retrieval": {"count": 2, "p50_ms": 2.0, "p95_ms": 4.0, "p99_ms": 4.0}},
    }


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
