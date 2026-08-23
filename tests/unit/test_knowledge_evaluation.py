"""Coverage for manually labelled retrieval metrics."""

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from src.knowledge.evaluation import EvaluationCase, EvaluationCheckpoint, _answer_metrics, _claim_coverage_sufficiency, _judge_claim_metrics, evaluate_catalog, load_dataset
from src.knowledge.evaluate_runner import _selected_dataset_hash
from src.knowledge.indexer import VectorHit
from src.models.channel import Channel
from src.models.knowledge import KnowledgeChannel, KnowledgeChannelState, KnowledgeEvaluationRun
from src.models.post import Post


def test_evaluation_case_keeps_multiple_manual_relevance_labels() -> None:
    case = EvaluationCase("multi", "question", frozenset({10, 20}))

    assert case.expected_telegram_post_ids == {10, 20}


def test_reviewed_positive_dataset_defaults_to_complete_labels(tmp_path) -> None:
    path = tmp_path / "reviewed.jsonl"
    path.write_text(
        '{"id":"reviewed","question":"question","expected_telegram_post_ids":[101],'
        '"reference_answer_html":"Ответ <a href=\\"https://t.me/catalog/101\\">[1]</a>",'
        '"expected_claims":[{"id":"claim","text":"Ответ","telegram_post_ids":[101]}]}\n',
        encoding="utf-8",
    )

    cases, _ = load_dataset(path)

    assert cases[0].relevance_complete is True


def test_smoke_hash_identifies_the_selected_subset() -> None:
    first = EvaluationCase("first", "question one", frozenset({1}), relevance_complete=True)
    second = EvaluationCase("second", "question two", frozenset({2}), relevance_complete=True)

    assert _selected_dataset_hash([first], 1, "full") != _selected_dataset_hash([first, second], 2, "full")
    assert _selected_dataset_hash([first], None, "full") == "full"


def test_evaluation_checkpoint_is_content_free_and_resumable(tmp_path) -> None:
    path = tmp_path / "checkpoint.json"
    checkpoint = EvaluationCheckpoint.load_or_create(
        path,
        dataset_hash="a" * 64,
        configuration_id="candidate-v1",
        candidate=True,
        question_count=100,
    )
    checkpoint.metrics["recalls"].append(1.0)
    checkpoint.metrics["latencies"].append(123)
    checkpoint.next_case_index = 1
    checkpoint.save()

    resumed = EvaluationCheckpoint.load_or_create(
        path,
        dataset_hash="a" * 64,
        configuration_id="candidate-v1",
        candidate=True,
        question_count=100,
    )

    assert resumed.next_case_index == 1
    assert resumed.metrics["recalls"] == [1.0]
    assert "question one" not in path.read_text(encoding="utf-8")
    with pytest.raises(ValueError, match="different run"):
        EvaluationCheckpoint.load_or_create(
            path,
            dataset_hash="b" * 64,
            configuration_id="candidate-v1",
            candidate=True,
            question_count=100,
        )



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
    assert record.p50_latency_ms is not None
    assert record.p50_retrieval_latency_ms is not None
    assert record.retrieval_latency_ms is not None
    assert record.answer_generation_ms is None


@pytest.mark.asyncio
async def test_evaluation_refuses_to_publish_metrics_after_vector_failure(engine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as session:
        channel = Channel(username="catalog")
        session.add(channel)
        await session.flush()
        session.add(KnowledgeChannel(channel_id=channel.id, state=KnowledgeChannelState.READY))
        await session.commit()

    service = SimpleNamespace(
        _settings=SimpleNamespace(index_version=1),
        _vector_search=AsyncMock(side_effect=OSError("Qdrant lock unavailable")),
    )

    with pytest.raises(RuntimeError, match="vector retrieval"):
        await evaluate_catalog(
            session_factory,
            service,
            channel_username="catalog",
            cases=[EvaluationCase("graph", "graph retrieval", frozenset({102}))],
            dataset_hash="b" * 64,
        )


@pytest.mark.asyncio
async def test_evaluation_measures_correct_abstention_and_false_attribution(engine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as session:
        channel = Channel(username="catalog")
        session.add(channel)
        await session.flush()
        session.add(KnowledgeChannel(channel_id=channel.id, state=KnowledgeChannelState.READY))
        await session.commit()

    service = SimpleNamespace(
        _settings=SimpleNamespace(index_version=1),
        _vector_search=AsyncMock(return_value=[]),
    )
    negative = EvaluationCase("neg", "вопрос без ответа", frozenset(), answer_expected=False, relevance_complete=True)
    result = await evaluate_catalog(
        session_factory,
        service,
        channel_username="catalog",
        cases=[negative],
        dataset_hash="c" * 64,
    )

    assert result["correct_abstention_share"] == 1
    assert result["false_attribution_share"] == 0
    async with session_factory() as session:
        record = (await session.execute(select(KnowledgeEvaluationRun))).scalar_one()
    assert record.correct_abstention_share == 1
    assert record.false_attribution_share == 0


@pytest.mark.asyncio
async def test_evaluation_records_false_attribution_when_no_answer_is_expected(engine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as session:
        channel = Channel(username="catalog")
        session.add(channel)
        await session.flush()
        catalog = KnowledgeChannel(channel_id=channel.id, state=KnowledgeChannelState.READY)
        post = Post(channel_id=channel.id, post_id=999, content="нерелевантный пост", datetime=datetime.now(timezone.utc))
        session.add_all([catalog, post])
        await session.commit()

    service = SimpleNamespace(
        _settings=SimpleNamespace(index_version=1),
        _vector_search=AsyncMock(return_value=[VectorHit(post_id=post.id, representation_type="full", ordinal=None, score=0.5)]),
    )
    negative = EvaluationCase("neg2", "вопрос без ответа", frozenset(), answer_expected=False, relevance_complete=True)
    result = await evaluate_catalog(
        session_factory,
        service,
        channel_username="catalog",
        cases=[negative],
        dataset_hash="d" * 64,
    )

    assert result["correct_abstention_share"] == 0
    assert result["false_attribution_share"] == 1


@pytest.mark.asyncio
async def test_evaluation_measures_abstention_by_answer_not_retrieval(engine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as session:
        channel = Channel(username="catalog")
        session.add(channel)
        await session.flush()
        catalog = KnowledgeChannel(channel_id=channel.id, state=KnowledgeChannelState.READY)
        post = Post(channel_id=channel.id, post_id=999, content="нерелевантный пост", datetime=datetime.now(timezone.utc))
        session.add_all([catalog, post])
        await session.commit()

    service = SimpleNamespace(
        _settings=SimpleNamespace(index_version=1, parent_context_limit=2000, neighbor_expansion=0),
        _vector_search=AsyncMock(return_value=[VectorHit(post_id=post.id, representation_type="full", ordinal=None, score=0.5)]),
        _answer=AsyncMock(return_value=([], False, False)),
    )
    negative = EvaluationCase("neg3", "вопрос без ответа", frozenset(), answer_expected=False, relevance_complete=True)
    result = await evaluate_catalog(
        session_factory,
        service,
        channel_username="catalog",
        cases=[negative],
        dataset_hash="e" * 64,
    )

    assert result["correct_abstention_share"] == 1
    assert result["false_attribution_share"] == 0


@pytest.mark.asyncio
async def test_evaluation_records_false_attribution_when_answer_cites_similar_posts(engine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as session:
        channel = Channel(username="catalog")
        session.add(channel)
        await session.flush()
        catalog = KnowledgeChannel(channel_id=channel.id, state=KnowledgeChannelState.READY)
        post = Post(channel_id=channel.id, post_id=999, content="нерелевантный пост", datetime=datetime.now(timezone.utc))
        session.add_all([catalog, post])
        await session.commit()

    service = SimpleNamespace(
        _settings=SimpleNamespace(index_version=1, parent_context_limit=2000, neighbor_expansion=0),
        _vector_search=AsyncMock(return_value=[VectorHit(post_id=post.id, representation_type="full", ordinal=None, score=0.5)]),
        _answer=AsyncMock(return_value=([SimpleNamespace(text="claim", cited_post_ids=[post.id])], True, False)),
    )
    negative = EvaluationCase("neg4", "вопрос без ответа", frozenset(), answer_expected=False, relevance_complete=True)
    result = await evaluate_catalog(
        session_factory,
        service,
        channel_username="catalog",
        cases=[negative],
        dataset_hash="f" * 64,
    )

    assert result["correct_abstention_share"] == 0
    assert result["false_attribution_share"] == 1


def test_answer_metrics_require_matching_claim_text_and_canonical_citation() -> None:
    case = EvaluationCase(
        "claims",
        "question",
        frozenset({101, 102}),
        reference_answer_html="<b>Answer</b> <a href=\"https://t.me/catalog/101\">[1]</a> <a href=\"https://t.me/catalog/102\">[2]</a>",
        expected_claims=(
            {"id": "one", "text": "Гибридный поиск объединяет лексический и векторный сигналы.", "telegram_post_ids": (101,)},
            {"id": "two", "text": "Граф знаний связывает сущности и факты.", "telegram_post_ids": (102,)},
        ),
    )
    claims = [
        SimpleNamespace(text="Гибридный поиск объединяет лексический и векторный сигналы.", cited_post_ids=[1]),
        SimpleNamespace(text="Граф знаний связывает сущности и факты.", cited_post_ids=[2]),
    ]

    assert _answer_metrics(claims, case, telegram_post_ids_by_id={1: 101, 2: 102}) == {
        "citation_precision": 1,
        "citation_recall": 1,
        "citation_f1": 1,
        "citation_placement": 1,
        "claim_precision": 1,
        "claim_recall": 1,
        "claim_f1": 1,
    }


def test_citation_placement_penalizes_mismatched_claim_link_binding() -> None:
    case = EvaluationCase(
        "claims",
        "question",
        frozenset({101, 102}),
        expected_claims=(
            {"id": "one", "text": "Гибридный поиск объединяет лексический и векторный сигналы.", "telegram_post_ids": (101,)},
            {"id": "two", "text": "Граф знаний связывает сущности и факты.", "telegram_post_ids": (102,)},
        ),
    )
    # The claim text matches expected #1, but it also cites post 102, which
    # expected #1 does not support -> the linked post is not placed correctly.
    claims = [
        SimpleNamespace(text="Гибридный поиск объединяет лексический и векторный сигналы.", cited_post_ids=[1, 2]),
        SimpleNamespace(text="Граф знаний связывает сущности и факты.", cited_post_ids=[2]),
    ]

    metrics = _answer_metrics(claims, case, telegram_post_ids_by_id={1: 101, 2: 102})

    assert metrics["citation_placement"] == 0.5
    assert metrics["claim_precision"] == 1


def test_claim_coverage_sufficiency_requires_every_expected_claim() -> None:
    case = EvaluationCase(
        "coverage",
        "question",
        frozenset({101, 102}),
        expected_claims=(
            {"id": "one", "text": "Первое.", "telegram_post_ids": (101,)},
            {"id": "two", "text": "Второе.", "telegram_post_ids": (102,)},
        ),
    )

    assert _claim_coverage_sufficiency(case, [101]) == 0.5
    assert _claim_coverage_sufficiency(case, [101, 102]) == 1.0
    assert _claim_coverage_sufficiency(case, []) == 0.0


async def test_judge_claim_metrics_uses_semantic_verdicts() -> None:
    case = EvaluationCase(
        "judged",
        "question",
        frozenset({101, 102}),
        expected_claims=(
            {"id": "one", "text": "Гибридный поиск объединяет сигналы.", "telegram_post_ids": (101,)},
            {"id": "two", "text": "Граф знаний связывает факты.", "telegram_post_ids": (102,)},
        ),
    )
    claims = [
        SimpleNamespace(text="Гибридный поиск объединяет сигналы.", cited_post_ids=[1]),
        SimpleNamespace(text="Полностью другое утверждение.", cited_post_ids=[2]),
    ]

    async def judge(generated_text, generated_ids, expected_text, expected_ids):
        return "сигналы" in generated_text

    metrics = await _judge_claim_metrics(judge, claims, case, telegram_post_ids_by_id={1: 101, 2: 102})

    assert metrics["claim_precision"] == 0.5
    assert metrics["claim_recall"] == 0.5
    assert metrics["claim_f1"] == 0.5
