"""Focused in-memory coverage for BL-21's durable, content-free control plane."""

from __future__ import annotations

import hashlib
import json
import subprocess
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import String, cast, func, insert, select
from sqlalchemy.exc import StatementError
from sqlalchemy.ext.asyncio import async_sessionmaker

from src.knowledge.experiment_repository import ExperimentRepository
from src.knowledge import experiment_runner
from src.knowledge.experiment_runner import BaselineSnapshot, CandidateOutcome, CandidateSpec, PhaseResult, _select_on_development, experiment_database_url_for_engine, main, validate_database_url, validate_preflight
from src.knowledge.experiment_retriever import LexicalCandidateMode
from src.knowledge.experiment_vector import OperatorEmbeddingPricing, REPRESENTATION_TOKEN_TOTAL, vector_candidate_config
from src.knowledge.experiments import (
    CampaignState,
    CandidateState,
    EvaluationMetrics,
    ExperimentError,
    ExperimentPolicy,
    PromotionDecision,
    RUNNER_GIT_BRANCH_ENV,
    RUNNER_GIT_REVISION_ENV,
    RetrievalMetrics,
    StateTransitionError,
    config_sha256,
    hash_identifier,
    validate_report,
)
from src.models.channel import Channel
from src.models.knowledge import (
    ExperimentCampaign,
    ExperimentCandidate,
    KnowledgeChannel,
    KnowledgeChannelState,
    KnowledgeEvaluationRun,
)


DATABASE_URL = "postgresql+asyncpg://bot:experiment-only-password@db:5432/telegram_bot_bl21_experiment?experiment=bl21"
DATABASE_DRIVER_URL = DATABASE_URL.removesuffix("?experiment=bl21")


def _metrics() -> EvaluationMetrics:
    return EvaluationMetrics(2, RetrievalMetrics(1.0, 1.0, 1.0), 0.0, 1.0, 0)


def _metrics_record() -> dict[str, float | int]:
    return {
        "case_count": 2,
        "recall_at_k": 1.0,
        "mrr": 1.0,
        "ndcg": 1.0,
        "duplicate_source_share": 0.0,
        "source_diversity": 1.0,
        "insufficient_evidence_count": 0,
    }


def _timings_record() -> dict[str, dict[str, float | int]]:
    return {"retrieval": {"count": 2, "p50_ms": 3.0, "p95_ms": 5.0, "p99_ms": 5.0}}


def _baseline_snapshot() -> dict[str, object]:
    return {
        "run_id": 1,
        "index_version": 1,
        "metrics": {"recall_at_k": 0.8, "mrr": 0.8, "ndcg": 0.8, "duplicate_source_share": 0.0},
        "latency": {"historical_mean_ms": 10, "phase_percentiles_available": False},
    }


def _baseline_snapshot_sha256() -> str:
    return config_sha256(_baseline_snapshot())


@pytest.mark.asyncio
async def test_campaign_store_is_idempotent_locks_channels_and_refuses_unsafe_transitions(engine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as session:
        repo = ExperimentRepository(session)
        policy = ExperimentPolicy()
        campaign = await repo.create_or_get_campaign(
            campaign_key="batch_two",
            channel_sha256=hash_identifier("catalog"),
            dataset_sha256="a" * 64,
            source_snapshot_sha256="b" * 64,
            source_snapshot_table_count=2,
            baseline_run_id=1,
            baseline_snapshot_sha256=_baseline_snapshot_sha256(),
            baseline_snapshot=_baseline_snapshot(),
            configuration={"limit": 5},
            policy=policy,
        )
        same = await repo.create_or_get_campaign(
            campaign_key="batch_two",
            channel_sha256=hash_identifier("catalog"),
            dataset_sha256="a" * 64,
            source_snapshot_sha256="b" * 64,
            source_snapshot_table_count=2,
            baseline_run_id=1,
            baseline_snapshot_sha256=_baseline_snapshot_sha256(),
            baseline_snapshot=_baseline_snapshot(),
            configuration={"limit": 5},
            policy=policy,
        )

        assert same.id == campaign.id
        await repo.acquire_campaign_lock(campaign)
        await repo.transition_campaign(campaign, CampaignState.READY)
        await repo.transition_campaign(campaign, CampaignState.RUNNING)
        candidate = await repo.claim_candidate(
            campaign,
            hypothesis_id="lexical_bias",
            configuration={"ranking_limit": 5},
            index_label="index_v1",
            projected_cost_usd=Decimal("0.10"),
        )
        resumed = await repo.claim_candidate(
            campaign,
            hypothesis_id="lexical_bias",
            configuration={"ranking_limit": 5},
            index_label="index_v1",
            projected_cost_usd=Decimal("0.10"),
        )

        assert candidate is not None
        assert resumed is candidate
        assert candidate.status == CandidateState.RUNNING
        with pytest.raises(ExperimentError, match="different immutable inputs"):
            await repo.claim_candidate(
                campaign,
                hypothesis_id="lexical_bias",
                configuration={"ranking_limit": 5},
                index_label="index_v2",
                projected_cost_usd=Decimal("0.10"),
            )
        await repo.complete_candidate(
            candidate,
            dev_metrics=_metrics(),
            holdout_metrics=_metrics(),
            phase_timings_ms={"retrieval": [3.0, 5.0]},
            actual_cost_usd=Decimal("0.05"),
            decision=PromotionDecision.PASSING_FOR_REVIEW,
            decision_reason="development_selected_holdout_review",
        )
        assert candidate.status == CandidateState.EVALUATED
        assert candidate.dev_metrics == {
            "case_count": 2,
            "recall_at_k": 1.0,
            "mrr": 1.0,
            "ndcg": 1.0,
            "duplicate_source_share": 0.0,
            "source_diversity": 1.0,
            "insufficient_evidence_count": 0,
        }
        assert candidate.phase_percentiles == {"retrieval": {"count": 2, "p50_ms": 3.0, "p95_ms": 5.0, "p99_ms": 5.0}}
        assert candidate.decision_reason == "development_selected_holdout_review"
        assert await repo.claim_candidate(
            campaign,
            hypothesis_id="lexical_bias",
            configuration={"ranking_limit": 5},
            index_label="index_v1",
            projected_cost_usd=Decimal("0.10"),
        ) is None
        await repo.transition_campaign(campaign, CampaignState.COMPLETED)
        with pytest.raises(StateTransitionError):
            await repo.transition_campaign(campaign, CampaignState.RUNNING)
        retriable = await repo.create_or_get_campaign(
            campaign_key="retryable",
            channel_sha256=hash_identifier("other_catalog"),
            dataset_sha256="a" * 64,
            source_snapshot_sha256="b" * 64,
            source_snapshot_table_count=2,
            baseline_run_id=1,
            baseline_snapshot_sha256=_baseline_snapshot_sha256(),
            baseline_snapshot=_baseline_snapshot(),
            configuration={"limit": 7},
            policy=policy,
        )
        await repo.transition_campaign(retriable, CampaignState.READY)
        await repo.transition_campaign(retriable, CampaignState.RUNNING)
        await repo.transition_campaign(retriable, CampaignState.FAILED)
        assert (await repo.resume_campaign(retriable, {"limit": 7})).status == CampaignState.READY


@pytest.mark.asyncio
async def test_vector_candidate_pricing_metadata_is_persisted_and_reserved_within_campaign_cap(engine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as session:
        repo = ExperimentRepository(session)
        campaign = await repo.create_or_get_campaign(
            campaign_key="vector_pricing",
            channel_sha256=hash_identifier("catalog"),
            dataset_sha256="a" * 64,
            source_snapshot_sha256="b" * 64,
            source_snapshot_table_count=1,
            baseline_run_id=1,
            baseline_snapshot_sha256=_baseline_snapshot_sha256(),
            baseline_snapshot=_baseline_snapshot(),
            configuration={"candidate_set": 8},
            policy=ExperimentPolicy(),
        )
        await repo.transition_campaign(campaign, CampaignState.READY)
        await repo.transition_campaign(campaign, CampaignState.RUNNING)
        pricing = OperatorEmbeddingPricing()
        spec = vector_candidate_config("vector_all")
        with pytest.raises(ExperimentError, match="pricing metadata must be complete"):
            await repo.claim_candidate(
                campaign,
                hypothesis_id=spec.hypothesis_id,
                configuration=spec.configuration(pricing),
                index_label="bl21_vector_index",
                projected_cost_usd=pricing.project(REPRESENTATION_TOKEN_TOTAL),
            )
        candidate = await repo.claim_candidate(
            campaign,
            hypothesis_id=spec.hypothesis_id,
            configuration=spec.configuration(pricing),
            index_label="bl21_vector_index",
            projected_cost_usd=pricing.project(REPRESENTATION_TOKEN_TOTAL),
            embedding_model_id=pricing.model_id,
            embedding_pricing_version=pricing.version,
            embedding_pricing_source=pricing.source,
            embedding_input_tokens=REPRESENTATION_TOKEN_TOTAL,
        )

        assert candidate is not None
        assert candidate.embedding_model_id == "qwen/qwen3-embedding-8b"
        assert candidate.embedding_pricing_source == "operator_override"
        assert candidate.embedding_input_tokens == 750_444
        assert candidate.projected_cost_usd == Decimal("0.007504")


@pytest.mark.asyncio
async def test_experiment_enums_persist_lowercase_values_and_round_trip_in_sqlite(engine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as session:
        campaign = ExperimentCampaign(
            campaign_key="enum_storage",
            channel_sha256="a" * 64,
            dataset_sha256="b" * 64,
            source_snapshot_sha256="c" * 64,
            source_snapshot_table_count=1,
            config_sha256="d" * 64,
            policy_sha256="e" * 64,
            resume_key="f" * 64,
            budget_usd=Decimal("1.00"),
            status=CampaignState.DRAFT,
        )
        session.add(campaign)
        await session.flush()
        candidate = ExperimentCandidate(
            campaign_id=campaign.id,
            hypothesis_id="enum_storage",
            config_sha256="1" * 64,
            index_label="index_v1",
            status=CandidateState.PLANNED,
            promotion_decision=PromotionDecision.PASSING_FOR_REVIEW,
        )
        session.add(candidate)
        await session.commit()
        campaign_id = campaign.id
        candidate_id = candidate.id

        stored = (await session.execute(
            select(
                cast(ExperimentCampaign.status, String).label("campaign_status"),
                cast(ExperimentCandidate.status, String).label("candidate_status"),
                cast(ExperimentCandidate.promotion_decision, String).label("promotion_decision"),
            ).join(ExperimentCandidate)
        )).one()
        assert stored == ("draft", "planned", "passing_for_review")

        session.expire_all()
        assert (await session.get(ExperimentCampaign, campaign_id)).status == CampaignState.DRAFT
        assert (await session.get(ExperimentCandidate, candidate_id)).status == CandidateState.PLANNED


@pytest.mark.asyncio
async def test_execute_binds_the_preflighted_dataset_and_manifest_bytes(tmp_path, monkeypatch) -> None:
    root, dataset = _experiment_root(tmp_path)
    _write_cases(dataset, "original")
    _write_manifest(root, dataset, snapshot="c" * 64, post_count=1)
    original_validate_preflight = experiment_runner.validate_preflight
    captured: dict[str, object] = {}

    def replace_inputs_after_preflight(**kwargs):
        evidence = original_validate_preflight(**kwargs)
        _write_cases(dataset, "replacement")
        _write_manifest(root, dataset, snapshot="d" * 64, post_count=2)
        return evidence

    class FakeSessionContext:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def commit(self) -> None:
            return None

    class FakeEngine:
        async def dispose(self) -> None:
            return None

    async def capture_snapshot(_session, table_counts) -> None:
        captured["table_counts"] = table_counts

    async def capture_campaign(_session, *, evidence, development_cases, holdout_cases, **_kwargs):
        captured["dataset_sha256"] = evidence.dataset_sha256
        captured["questions"] = [case.question for case in (*development_cases, *holdout_cases)]
        return [], SimpleNamespace(
            status=CampaignState.COMPLETED,
            config_sha256="a" * 64,
            dataset_sha256=evidence.dataset_sha256,
            resume_key="b" * 64,
            baseline_run_id=1,
            baseline_snapshot_sha256=config_sha256(_baseline_snapshot()),
            baseline_snapshot=_baseline_snapshot(),
        )

    async def capture_baseline(_session, **_kwargs) -> BaselineSnapshot:
        return BaselineSnapshot(1, 1, 0.8, 0.8, 0.8, 0.0, 10)

    report_path = tmp_path / "report.json"
    report_path.write_bytes(b"report")
    monkeypatch.setattr(experiment_runner, "validate_preflight", replace_inputs_after_preflight)
    def create_engine(database_url, **_kwargs):
        captured["engine_url"] = database_url
        return FakeEngine()

    monkeypatch.setattr(experiment_runner, "create_async_engine", create_engine)
    monkeypatch.setattr(experiment_runner, "async_sessionmaker", lambda *_args, **_kwargs: lambda: FakeSessionContext())
    monkeypatch.setattr(experiment_runner, "_validate_database_snapshot", capture_snapshot)
    monkeypatch.setattr(experiment_runner, "_load_baseline_snapshot", capture_baseline)
    monkeypatch.setattr(experiment_runner, "_run_campaign", capture_campaign)
    monkeypatch.setattr(experiment_runner, "write_experiment_report", lambda _root, _name, report, **_kwargs: captured.setdefault("report", report) and report_path)

    await experiment_runner.execute_experiment(
        experiment_root=root,
        database_url=DATABASE_URL,
        dataset=dataset,
        channel="catalog",
        campaign_key="batch_two",
        baseline_run_id=1,
    )

    assert captured["table_counts"] == (("posts", 1),)
    assert set(captured["questions"]) == {f"original question {index}" for index in range(10)}
    assert captured["report"]["campaign"]["dataset_sha256"] == captured["dataset_sha256"]
    assert captured["report"]["campaign"]["baseline_snapshot_sha256"] == config_sha256(_baseline_snapshot())
    assert captured["engine_url"] == DATABASE_DRIVER_URL


@pytest.mark.asyncio
async def test_execute_rejects_dataset_mismatch_before_database_or_report_writes(tmp_path, monkeypatch) -> None:
    root, dataset = _experiment_root(tmp_path)
    dataset.write_text('{"id":"one","question":"changed","expected_telegram_post_ids":[1]}\n', encoding="utf-8")
    database_opened = False

    def fail_if_database_opened(*_args, **_kwargs):
        nonlocal database_opened
        database_opened = True
        raise AssertionError("database must not open after preflight failure")

    monkeypatch.setattr(experiment_runner, "create_async_engine", fail_if_database_opened)
    with pytest.raises(ExperimentError, match="immutable clone manifest"):
        await experiment_runner.execute_experiment(
            experiment_root=root,
            database_url=DATABASE_URL,
            dataset=dataset,
            channel="catalog",
            campaign_key="batch_two",
            baseline_run_id=1,
        )

    assert not database_opened
    assert not (root / ".data").exists()


@pytest.mark.asyncio
async def test_baseline_binding_is_exact_content_free_and_rejects_legacy_or_mismatched_rows(engine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    dataset_sha256 = "a" * 64
    async with session_factory() as session:
        channel = Channel(username="catalog")
        session.add(channel)
        await session.flush()
        catalog = KnowledgeChannel(channel_id=channel.id, state=KnowledgeChannelState.READY, active_index_version=1)
        session.add(catalog)
        await session.flush()
        baseline_row = KnowledgeEvaluationRun(
            knowledge_channel_id=catalog.id,
            index_version=1,
            dataset_hash=dataset_sha256,
            mode="hybrid_parent_rrf@5",
            recall_at_k=0.8,
            mrr=0.7,
            ndcg=0.75,
            duplicate_source_share=0.1,
            latency_ms=12,
        )
        legacy_row = KnowledgeEvaluationRun(
            knowledge_channel_id=catalog.id,
            index_version=1,
            dataset_hash=dataset_sha256,
            mode="legacy",
            recall_at_k=None,
            mrr=0.7,
            ndcg=0.75,
            duplicate_source_share=0.1,
        )
        stale_index_row = KnowledgeEvaluationRun(
            knowledge_channel_id=catalog.id,
            index_version=2,
            dataset_hash=dataset_sha256,
            mode="stale_index",
            recall_at_k=0.8,
            mrr=0.7,
            ndcg=0.75,
            duplicate_source_share=0.1,
        )
        session.add_all([baseline_row, legacy_row, stale_index_row])
        await session.commit()

    async with session_factory() as session:
        snapshot = await experiment_runner._load_baseline_snapshot(
            session,
            baseline_run_id=baseline_row.id,
            channel="@catalog",
            dataset_sha256=dataset_sha256,
        )
        record = snapshot.record()
        assert record["run_id"] == baseline_row.id
        assert record["latency"] == {"historical_mean_ms": 12, "phase_percentiles_available": False}
        assert "catalog" not in str(record)
        repo = ExperimentRepository(session)
        campaign = await repo.create_or_get_campaign(
            campaign_key="baseline_bound",
            channel_sha256=hash_identifier("catalog"),
            dataset_sha256=dataset_sha256,
            source_snapshot_sha256="b" * 64,
            source_snapshot_table_count=1,
            baseline_run_id=snapshot.run_id,
            baseline_snapshot_sha256=config_sha256(record),
            baseline_snapshot=record,
            configuration={"candidate_set": 2},
            policy=ExperimentPolicy(),
        )
        await session.commit()
        assert campaign.baseline_snapshot == record
        assert campaign.baseline_snapshot_sha256 == config_sha256(record)
        with pytest.raises(ExperimentError, match="is absent"):
            await experiment_runner._load_baseline_snapshot(
                session,
                baseline_run_id=999_999,
                channel="catalog",
                dataset_sha256=dataset_sha256,
            )
        with pytest.raises(ExperimentError, match="dataset SHA"):
            await experiment_runner._load_baseline_snapshot(
                session,
                baseline_run_id=baseline_row.id,
                channel="catalog",
                dataset_sha256="c" * 64,
            )
        with pytest.raises(ExperimentError, match="approved catalog channel"):
            await experiment_runner._load_baseline_snapshot(
                session,
                baseline_run_id=baseline_row.id,
                channel="other_catalog",
                dataset_sha256=dataset_sha256,
            )
        with pytest.raises(ExperimentError, match="active catalog index"):
            await experiment_runner._load_baseline_snapshot(
                session,
                baseline_run_id=stale_index_row.id,
                channel="catalog",
                dataset_sha256=dataset_sha256,
            )
        with pytest.raises(ExperimentError, match="lacks required quality metrics"):
            await experiment_runner._load_baseline_snapshot(
                session,
                baseline_run_id=legacy_row.id,
                channel="catalog",
                dataset_sha256=dataset_sha256,
            )


@pytest.mark.asyncio
async def test_execute_campaign_runs_only_challenger_ablations(engine, monkeypatch) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as session:
        channel = Channel(username="catalog")
        session.add(channel)
        await session.flush()
        session.add(KnowledgeChannel(channel_id=channel.id, state=KnowledgeChannelState.READY))
        await session.commit()

    evidence = experiment_runner.PreflightEvidence(
        campaign_key="challengers_only",
        channel_sha256=hash_identifier("catalog"),
        dataset_sha256="a" * 64,
        dataset_case_count=2,
        source_snapshot_sha256="b" * 64,
        source_snapshot_table_count=1,
        manifest_sha256="c" * 64,
        config_sha256="d" * 64,
        resume_key="e" * 64,
        policy_sha256="f" * 64,
        dataset_cases=(),
        snapshot_table_counts=(),
    )
    phases: list[LexicalCandidateMode] = []

    async def evaluate(_retriever, mode, _cases, _policy) -> PhaseResult:
        phases.append(mode)
        return PhaseResult(_metrics(), _timings_record(), {"retrieval": [1.0], "lexical": [1.0]})

    monkeypatch.setattr(experiment_runner, "_evaluate_phase", evaluate)
    baseline = BaselineSnapshot(7, 1, 0.8, 0.8, 0.8, 0.0, None)
    cases = [experiment_runner.EvaluationCase("dev", "question", frozenset({1}))]
    async with session_factory() as session:
        outcomes, campaign = await experiment_runner._run_campaign(
            session,
            evidence=evidence,
            channel="catalog",
            development_cases=cases,
            holdout_cases=cases,
            baseline=baseline,
        )
        await session.commit()

    assert {outcome.spec.hypothesis_id for outcome in outcomes} == {"russian_fts", "exact_short_circuit"}
    assert LexicalCandidateMode.TOKEN_ILIKE not in phases
    assert campaign.status == CampaignState.COMPLETED
    report = experiment_runner._report(campaign, {"train_count": 1, "holdout_count": 1, "train_id_hashes": ["1" * 64], "holdout_id_hashes": ["2" * 64]}, outcomes)
    validate_report(report)
    comparison = report["candidates"][0]["comparison"]
    assert comparison["quality_deltas"]["development"]["recall_at_k"] == pytest.approx(0.2)
    assert comparison["latency"]["status"] == "baseline_phase_percentiles_unavailable"
    assert all(outcome.decision != PromotionDecision.PROMOTED for outcome in outcomes)


@pytest.mark.asyncio
async def test_store_rejects_content_fields_and_a_second_channel_campaign_lock(engine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as session:
        repo = ExperimentRepository(session)
        with pytest.raises(ExperimentError, match="prohibited content key"):
            await repo.create_or_get_campaign(
                campaign_key="unsafe",
                channel_sha256=hash_identifier("catalog"),
                dataset_sha256="a" * 64,
                source_snapshot_sha256="b" * 64,
                source_snapshot_table_count=1,
                baseline_run_id=1,
                baseline_snapshot_sha256=_baseline_snapshot_sha256(),
                baseline_snapshot=_baseline_snapshot(),
                configuration={"prompt": "never persist"},
                policy=ExperimentPolicy(),
            )
        first = await repo.create_or_get_campaign(
            campaign_key="first",
            channel_sha256=hash_identifier("catalog"),
            dataset_sha256="a" * 64,
            source_snapshot_sha256="b" * 64,
            source_snapshot_table_count=1,
            baseline_run_id=1,
            baseline_snapshot_sha256=_baseline_snapshot_sha256(),
            baseline_snapshot=_baseline_snapshot(),
            configuration={"limit": 5},
            policy=ExperimentPolicy(),
        )
        second = await repo.create_or_get_campaign(
            campaign_key="second",
            channel_sha256=hash_identifier("catalog"),
            dataset_sha256="a" * 64,
            source_snapshot_sha256="b" * 64,
            source_snapshot_table_count=1,
            baseline_run_id=1,
            baseline_snapshot_sha256=_baseline_snapshot_sha256(),
            baseline_snapshot=_baseline_snapshot(),
            configuration={"limit": 10},
            policy=ExperimentPolicy(),
        )
        await repo.acquire_campaign_lock(first)
        with pytest.raises(ExperimentError, match="channel lock"):
            await repo.acquire_campaign_lock(second)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("transport", "field_name", "unsafe_value"),
    [
        ("orm", "dev_metrics", {"case_count": 1, "nested": {"post": {"body": "raw post"}}}),
        ("core", "dev_metrics", {"case_count": 1, "nested": {"post": {"body": "raw post"}}}),
        ("orm", "holdout_metrics", {"case_count": "postgresql://bot:credential@db/experiment"}),
        ("core", "holdout_metrics", {"case_count": "postgresql://bot:credential@db/experiment"}),
        ("orm", "phase_percentiles", {"retrieval": {"count": 1, "p50_ms": 1, "p95_ms": 1, "p99_ms": 1}, "audit": {"credentials": "secret"}}),
        ("core", "phase_percentiles", {"retrieval": {"count": 1, "p50_ms": 1, "p95_ms": 1, "p99_ms": 1}, "audit": {"credentials": "secret"}}),
    ],
)
async def test_typed_experiment_json_rejects_unsafe_orm_and_core_values_before_persistence(
    engine,
    transport: str,
    field_name: str,
    unsafe_value: dict[str, object],
) -> None:
    """Mapped ORM/Core inserts share the strict JSON type; raw SQL is out of scope."""
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as session:
        campaign = ExperimentCampaign(
            campaign_key="typed_boundary",
            channel_sha256="a" * 64,
            dataset_sha256="b" * 64,
            source_snapshot_sha256="c" * 64,
            source_snapshot_table_count=1,
            config_sha256="d" * 64,
            policy_sha256="e" * 64,
            resume_key="f" * 64,
            budget_usd=Decimal("1.00"),
            status=CampaignState.DRAFT,
        )
        session.add(campaign)
        await session.commit()
        values: dict[str, object] = {
            "campaign_id": campaign.id,
            "hypothesis_id": "typed_boundary",
            "config_sha256": "1" * 64,
            "index_label": "index_v1",
            "dev_metrics": _metrics_record(),
            "holdout_metrics": _metrics_record(),
            "phase_percentiles": _timings_record(),
        }
        values[field_name] = unsafe_value

        with pytest.raises(StatementError):
            if transport == "orm":
                session.add(ExperimentCandidate(**values))
                await session.flush()
            else:
                await session.execute(insert(ExperimentCandidate).values(**values))
        await session.rollback()

        assert await session.scalar(select(func.count(ExperimentCandidate.id))) == 0


def test_dry_run_preflight_validates_clone_inputs_without_creating_report_state(tmp_path, capsys) -> None:
    root, dataset = _experiment_root(tmp_path)

    evidence = validate_preflight(
        experiment_root=root,
        database_url=DATABASE_URL,
        dataset=dataset,
        channel="catalog",
        campaign_key="batch_two",
    )
    assert evidence.dataset_case_count == 1
    assert not (root / ".data").exists()
    assert main([
        "--experiment-root", str(root),
        "--database-url", DATABASE_URL,
        "--dataset", str(dataset),
        "--channel", "catalog",
        "--campaign-id", "batch_two",
        "--dry-run",
    ]) == 0
    output = capsys.readouterr().out
    assert "example question" not in output
    assert "catalog" not in output
    assert not (root / ".data").exists()


def test_preflight_rejects_non_experiment_database_and_changed_dataset(tmp_path) -> None:
    root, dataset = _experiment_root(tmp_path)
    with pytest.raises(ExperimentError, match="experiment=bl21"):
        validate_database_url(DATABASE_URL.removesuffix("?experiment=bl21"))
    with pytest.raises(ExperimentError, match="isolated experiment clone"):
        validate_database_url(DATABASE_URL.replace("@db:", "@production-db:"))
    for invalid_url in (
        DATABASE_URL.replace("experiment=bl21", "experiment=other"),
        f"{DATABASE_URL}&application_name=bl21",
        f"{DATABASE_URL}&experiment=bl21",
    ):
        with pytest.raises(ExperimentError, match="exactly experiment=bl21"):
            validate_database_url(invalid_url)
    assert experiment_database_url_for_engine(DATABASE_URL) == DATABASE_DRIVER_URL

    dataset.write_text('{"id":"one","question":"changed","expected_telegram_post_ids":[1]}\n', encoding="utf-8")
    with pytest.raises(ExperimentError, match="immutable clone manifest"):
        validate_preflight(
            experiment_root=root,
            database_url=DATABASE_URL,
            dataset=dataset,
            channel="catalog",
            campaign_key="batch_two",
        )


def test_runner_preflight_uses_validated_launcher_metadata_only_without_git(tmp_path, monkeypatch) -> None:
    root, dataset = _experiment_root(tmp_path)
    revision = subprocess.run(
        ["git", "-C", str(root), "rev-parse", "--verify", "HEAD^{commit}"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    monkeypatch.setattr("src.knowledge.experiments._git", lambda *_args: (_ for _ in ()).throw(FileNotFoundError()))
    monkeypatch.setenv(RUNNER_GIT_BRANCH_ENV, "feature/bl-21-rag-quality-experiments")
    monkeypatch.setenv(RUNNER_GIT_REVISION_ENV, revision)

    evidence = experiment_runner.validate_preflight(
        experiment_root=root,
        database_url=DATABASE_URL,
        dataset=dataset,
        channel="catalog",
        campaign_key="batch_two",
    )

    assert evidence.dataset_case_count == 1


@pytest.mark.parametrize(
    ("branch", "revision", "message"),
    [
        (None, None, "requires launcher Git metadata"),
        ("main", "a" * 40, "must name the BL-21 feature branch"),
        ("feature/bl-21-rag-quality-experiments", "A" * 40, "full lowercase commit revision"),
    ],
)
def test_runner_preflight_rejects_missing_or_invalid_launcher_metadata_without_git(
    tmp_path,
    monkeypatch,
    branch: str | None,
    revision: str | None,
    message: str,
) -> None:
    root, dataset = _experiment_root(tmp_path)
    monkeypatch.setattr("src.knowledge.experiments._git", lambda *_args: (_ for _ in ()).throw(FileNotFoundError()))
    if branch is not None:
        monkeypatch.setenv(RUNNER_GIT_BRANCH_ENV, branch)
    if revision is not None:
        monkeypatch.setenv(RUNNER_GIT_REVISION_ENV, revision)

    with pytest.raises(ExperimentError, match=message):
        experiment_runner.validate_preflight(
            experiment_root=root,
            database_url=DATABASE_URL,
            dataset=dataset,
            channel="catalog",
            campaign_key="batch_two",
        )


def test_runner_preflight_rejects_metadata_that_conflicts_with_direct_git(tmp_path, monkeypatch) -> None:
    root, dataset = _experiment_root(tmp_path)
    monkeypatch.setenv(RUNNER_GIT_BRANCH_ENV, "feature/bl-21-rag-quality-experiments")
    monkeypatch.setenv(RUNNER_GIT_REVISION_ENV, "a" * 40)

    with pytest.raises(ExperimentError, match="does not match the experiment clone"):
        experiment_runner.validate_preflight(
            experiment_root=root,
            database_url=DATABASE_URL,
            dataset=dataset,
            channel="catalog",
            campaign_key="batch_two",
        )


def test_cli_requires_an_explicit_dry_run(tmp_path) -> None:
    root, dataset = _experiment_root(tmp_path)
    with pytest.raises(SystemExit):
        main([
            "--experiment-root", str(root),
            "--database-url", DATABASE_URL,
            "--dataset", str(dataset),
            "--channel", "catalog",
            "--campaign-id", "batch_two",
        ])


def test_cli_dry_run_does_not_write_and_execute_requires_explicit_safe_mode(tmp_path, monkeypatch, capsys) -> None:
    root, dataset = _experiment_root(tmp_path)
    with pytest.raises(ExperimentError, match="not permitted"):
        main([
            "--experiment-root", str(root),
            "--database-url", DATABASE_URL,
            "--dataset", str(dataset),
            "--channel", "catalog",
            "--campaign-id", "batch_two",
            "--dry-run",
            "--vector",
        ])
    assert not (root / ".data").exists()

    with pytest.raises(ExperimentError, match="requires --baseline-run-id"):
        main([
            "--experiment-root", str(root),
            "--database-url", DATABASE_URL,
            "--dataset", str(dataset),
            "--channel", "catalog",
            "--campaign-id", "batch_two",
            "--execute",
        ])

    called = {}

    async def fake_execute(**kwargs):
        called.update(kwargs)
        return {"campaign_sha256": "a" * 64, "candidate_count": 2, "report_sha256": "b" * 64}

    monkeypatch.setattr(experiment_runner, "execute_experiment", fake_execute)
    assert main([
        "--experiment-root", str(root),
        "--database-url", DATABASE_URL,
        "--dataset", str(dataset),
        "--channel", "catalog",
        "--campaign-id", "batch_two",
        "--baseline-run-id", "1",
        "--execute",
    ]) == 0
    assert called["campaign_key"] == "batch_two"
    assert called["baseline_run_id"] == 1
    assert "example question" not in capsys.readouterr().out


def test_cli_vector_execution_requires_its_own_allowlisted_gate(tmp_path, capsys) -> None:
    root, dataset = _experiment_root(tmp_path)
    arguments = [
        "--experiment-root", str(root), "--database-url", DATABASE_URL, "--dataset", str(dataset),
        "--channel", "catalog", "--campaign-id", "batch_two", "--baseline-run-id", "1",
    ]
    with pytest.raises(ExperimentError, match="--execute-vector requires --baseline-run-id"):
        main([*arguments[:-2], "--vector-candidate", "vector_all", "--execute-vector"])
    with pytest.raises(ExperimentError, match="requires one allowlisted"):
        main([*arguments, "--execute-vector"])
    with pytest.raises(ExperimentError, match="not allowlisted"):
        main([*arguments, "--vector-candidate", "vector_unknown", "--execute-vector"])
    assert main([*arguments, "--vector-candidate", "vector_all", "--execute-vector"]) == 0
    output = capsys.readouterr().out
    assert "operator_override" in output
    assert "qwen/qwen3-embedding-8b" in output
    assert "example question" not in output
    assert "catalog" not in output
    assert len(list((root / ".data-experiment" / "vector").iterdir())) == 1


def test_candidate_selection_uses_development_only_and_short_circuit_requires_no_regression() -> None:
    fts_metrics = EvaluationMetrics(2, RetrievalMetrics(1.0, 1.0, 1.0), 0.0, 1.0, 0)
    short_metrics = EvaluationMetrics(2, RetrievalMetrics(0.7, 1.0, 1.0), 0.0, 1.0, 0)
    timings = {"retrieval": {"count": 2, "p50_ms": 1.0, "p95_ms": 1.0, "p99_ms": 1.0}, "lexical": {"count": 2, "p50_ms": 1.0, "p95_ms": 1.0, "p99_ms": 1.0}}
    raw_timings = {"retrieval": [1.0, 1.0], "lexical": [1.0, 1.0]}
    baseline = BaselineSnapshot(1, 1, 0.8, 0.8, 0.8, 0.0, 10)
    fts = CandidateOutcome(CandidateSpec("russian_fts", LexicalCandidateMode.RUSSIAN_FTS), CandidateState.RUNNING, PromotionDecision.INSUFFICIENT_EVIDENCE, "development_pending", PhaseResult(fts_metrics, timings, raw_timings))
    short = CandidateOutcome(CandidateSpec("exact_short_circuit", LexicalCandidateMode.EXACT_SHORT_CIRCUIT), CandidateState.RUNNING, PromotionDecision.INSUFFICIENT_EVIDENCE, "development_pending", PhaseResult(short_metrics, timings, raw_timings))

    assert _select_on_development([fts, short], ExperimentPolicy(), baseline) is fts


def _experiment_root(tmp_path: Path) -> tuple[Path, Path]:
    root = tmp_path / "experiment"
    root.mkdir()
    subprocess.run(["git", "init", "--quiet", str(root)], check=True)
    subprocess.run(["git", "-C", str(root), "checkout", "--quiet", "-b", "feature/bl-21-rag-quality-experiments"], check=True)
    subprocess.run(
        [
            "git", "-C", str(root), "-c", "user.name=BL21 Test", "-c", "user.email=bl21@example.invalid",
            "commit", "--quiet", "--allow-empty", "-m", "feature fixture",
        ],
        check=True,
    )
    inputs = root / ".data-experiment" / "inputs"
    inputs.mkdir(parents=True)
    dataset = inputs / "cases.jsonl"
    dataset.write_text('{"id":"one","question":"example question","expected_telegram_post_ids":[1]}\n', encoding="utf-8")
    _write_manifest(root, dataset, snapshot="c" * 64, post_count=1)
    return root, dataset


def _write_cases(dataset: Path, prefix: str) -> None:
    dataset.write_text(
        "".join(
            json.dumps({"id": f"case-{index}", "question": f"{prefix} question {index}", "expected_telegram_post_ids": [index]}) + "\n"
            for index in range(10)
        ),
        encoding="utf-8",
    )


def _write_manifest(root: Path, dataset: Path, *, snapshot: str, post_count: int) -> None:
    raw = dataset.read_bytes()
    manifest = {
        "schema_version": 1,
        "dataset": {"path": "inputs/cases.jsonl", "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()},
        "logical_snapshot": {"bytes": 1, "format": "pg_dump_custom", "sha256": snapshot},
        "table_counts": {
            "snapshot_equals_test": True,
            "source_post_restore_equals_test": True,
            "table_count": 1,
            "source_at_snapshot": {"posts": str(post_count)},
            "source_post_restore": {"posts": str(post_count)},
            "test_after_restore": {"posts": str(post_count)},
        },
    }
    (root / ".data-experiment" / "clone-manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
