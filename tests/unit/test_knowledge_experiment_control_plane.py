"""Focused in-memory coverage for BL-21's durable, content-free control plane."""

from __future__ import annotations

import hashlib
import json
import subprocess
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import func, insert, select
from sqlalchemy.exc import StatementError
from sqlalchemy.ext.asyncio import async_sessionmaker

from src.knowledge.experiment_repository import ExperimentRepository
from src.knowledge import experiment_runner
from src.knowledge.experiment_runner import CandidateOutcome, CandidateSpec, PhaseResult, _select_on_development, main, validate_database_url, validate_preflight
from src.knowledge.experiment_retriever import LexicalCandidateMode
from src.knowledge.experiments import (
    CampaignState,
    CandidateState,
    EvaluationMetrics,
    ExperimentError,
    ExperimentPolicy,
    PromotionDecision,
    RetrievalMetrics,
    StateTransitionError,
    hash_identifier,
)
from src.models.knowledge import ExperimentCampaign, ExperimentCandidate


DATABASE_URL = "postgresql+asyncpg://bot:experiment-only-password@db:5432/telegram_bot_bl21_experiment?experiment=bl21"


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
            configuration={"limit": 5},
            policy=policy,
        )
        same = await repo.create_or_get_campaign(
            campaign_key="batch_two",
            channel_sha256=hash_identifier("catalog"),
            dataset_sha256="a" * 64,
            source_snapshot_sha256="b" * 64,
            source_snapshot_table_count=2,
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
            configuration={"limit": 7},
            policy=policy,
        )
        await repo.transition_campaign(retriable, CampaignState.READY)
        await repo.transition_campaign(retriable, CampaignState.RUNNING)
        await repo.transition_campaign(retriable, CampaignState.FAILED)
        assert (await repo.resume_campaign(retriable, {"limit": 7})).status == CampaignState.READY


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
                configuration={"prompt": "never persist"},
                policy=ExperimentPolicy(),
            )
        first = await repo.create_or_get_campaign(
            campaign_key="first",
            channel_sha256=hash_identifier("catalog"),
            dataset_sha256="a" * 64,
            source_snapshot_sha256="b" * 64,
            source_snapshot_table_count=1,
            configuration={"limit": 5},
            policy=ExperimentPolicy(),
        )
        second = await repo.create_or_get_campaign(
            campaign_key="second",
            channel_sha256=hash_identifier("catalog"),
            dataset_sha256="a" * 64,
            source_snapshot_sha256="b" * 64,
            source_snapshot_table_count=1,
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

    dataset.write_text('{"id":"one","question":"changed","expected_telegram_post_ids":[1]}\n', encoding="utf-8")
    with pytest.raises(ExperimentError, match="immutable clone manifest"):
        validate_preflight(
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

    called = {}

    async def fake_execute(**kwargs):
        called.update(kwargs)
        return {"campaign_sha256": "a" * 64, "candidate_count": 3, "report_sha256": "b" * 64}

    monkeypatch.setattr(experiment_runner, "execute_experiment", fake_execute)
    assert main([
        "--experiment-root", str(root),
        "--database-url", DATABASE_URL,
        "--dataset", str(dataset),
        "--channel", "catalog",
        "--campaign-id", "batch_two",
        "--execute",
    ]) == 0
    assert called["campaign_key"] == "batch_two"
    assert "example question" not in capsys.readouterr().out


def test_candidate_selection_uses_development_only_and_short_circuit_requires_no_regression() -> None:
    baseline_metrics = EvaluationMetrics(2, RetrievalMetrics(0.8, 0.8, 0.8), 0.0, 1.0, 0)
    fts_metrics = EvaluationMetrics(2, RetrievalMetrics(1.0, 1.0, 1.0), 0.0, 1.0, 0)
    short_metrics = EvaluationMetrics(2, RetrievalMetrics(0.7, 1.0, 1.0), 0.0, 1.0, 0)
    timings = {"retrieval": {"count": 2, "p50_ms": 1.0, "p95_ms": 1.0, "p99_ms": 1.0}, "lexical": {"count": 2, "p50_ms": 1.0, "p95_ms": 1.0, "p99_ms": 1.0}}
    raw_timings = {"retrieval": [1.0, 1.0], "lexical": [1.0, 1.0]}
    baseline = CandidateOutcome(CandidateSpec("token_ilike_baseline", LexicalCandidateMode.TOKEN_ILIKE), CandidateState.RUNNING, PromotionDecision.INSUFFICIENT_EVIDENCE, "development_pending", PhaseResult(baseline_metrics, timings, raw_timings))
    fts = CandidateOutcome(CandidateSpec("russian_fts", LexicalCandidateMode.RUSSIAN_FTS), CandidateState.RUNNING, PromotionDecision.INSUFFICIENT_EVIDENCE, "development_pending", PhaseResult(fts_metrics, timings, raw_timings))
    short = CandidateOutcome(CandidateSpec("exact_short_circuit", LexicalCandidateMode.EXACT_SHORT_CIRCUIT), CandidateState.RUNNING, PromotionDecision.INSUFFICIENT_EVIDENCE, "development_pending", PhaseResult(short_metrics, timings, raw_timings))

    assert _select_on_development([baseline, fts, short], ExperimentPolicy(), baseline) is fts


def _experiment_root(tmp_path: Path) -> tuple[Path, Path]:
    root = tmp_path / "experiment"
    root.mkdir()
    subprocess.run(["git", "init", "--quiet", str(root)], check=True)
    subprocess.run(["git", "-C", str(root), "checkout", "--quiet", "-b", "feature/bl-21-rag-quality-experiments"], check=True)
    inputs = root / ".data-experiment" / "inputs"
    inputs.mkdir(parents=True)
    dataset = inputs / "cases.jsonl"
    dataset.write_text('{"id":"one","question":"example question","expected_telegram_post_ids":[1]}\n', encoding="utf-8")
    raw = dataset.read_bytes()
    snapshot = "c" * 64
    manifest = {
        "schema_version": 1,
        "dataset": {"path": "inputs/cases.jsonl", "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()},
        "logical_snapshot": {"bytes": 1, "format": "pg_dump_custom", "sha256": snapshot},
        "table_counts": {
            "snapshot_equals_test": True,
            "source_post_restore_equals_test": True,
            "table_count": 1,
            "source_at_snapshot": {"posts": "1"},
            "source_post_restore": {"posts": "1"},
            "test_after_restore": {"posts": "1"},
        },
    }
    (root / ".data-experiment" / "clone-manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return root, dataset
