"""Preflight and explicitly gated isolated BL-21 campaign execution."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
import os
import re
import stat
import time
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Mapping, Sequence

from sqlalchemy import select, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.knowledge.evaluation import EvaluationCase, load_dataset_bytes
from src.knowledge.experiment_repository import ExperimentRepository
from src.knowledge.experiment_retriever import CanonicalLexicalCandidateRetriever, LexicalCandidateMode
from src.knowledge.experiment_vector import validate_vector_execution, vector_candidate_config
from src.knowledge.experiments import (
    CampaignState,
    CandidateState,
    EvaluationMetrics,
    ExperimentError,
    ExperimentPolicy,
    PromotionDecision,
    RetrievalMetrics,
    UnsafeExperimentPath,
    config_sha256,
    create_campaign,
    duplicate_share,
    evaluation_metrics_record,
    hash_identifier,
    phase_timing_summary,
    preflight_experiment_dir,
    promotion_decision,
    require_safe_identifier,
    require_sha256,
    RUNNER_GIT_BRANCH_ENV,
    RUNNER_GIT_REVISION_ENV,
    retrieval_metrics,
    source_diversity,
    split_ids,
    write_experiment_report,
)
from src.models.channel import Channel
from src.models.knowledge import KnowledgeChannel, KnowledgeChannelState, KnowledgeEvaluationRun


EXPERIMENT_DATABASE_NAME = "telegram_bot_bl21_experiment"
EXPERIMENT_DATABASE_HOST = "db"
EXPERIMENT_DATABASE_MARKER = "bl21"
MANIFEST_RELATIVE_PATH = Path(".data-experiment/clone-manifest.json")
RESULT_LIMIT = 5
POOL_LIMIT = 30
_SAFE_TABLE_NAME = re.compile(r"[a-z][a-z0-9_]*\Z")


@dataclass(frozen=True, slots=True)
class PreflightEvidence:
    campaign_key: str
    channel_sha256: str
    dataset_sha256: str
    dataset_case_count: int
    source_snapshot_sha256: str
    source_snapshot_table_count: int
    manifest_sha256: str
    config_sha256: str
    resume_key: str
    policy_sha256: str
    dataset_cases: tuple[EvaluationCase, ...] = field(repr=False)
    snapshot_table_counts: tuple[tuple[str, int], ...] = field(repr=False)

    def record(self) -> dict[str, str | int]:
        return {
            "campaign_key": self.campaign_key,
            "channel_sha256": self.channel_sha256,
            "dataset_sha256": self.dataset_sha256,
            "dataset_case_count": self.dataset_case_count,
            "source_snapshot_sha256": self.source_snapshot_sha256,
            "source_snapshot_table_count": self.source_snapshot_table_count,
            "manifest_sha256": self.manifest_sha256,
            "config_sha256": self.config_sha256,
            "resume_key": self.resume_key,
            "policy_sha256": self.policy_sha256,
            "dry_run": "true",
        }


@dataclass(frozen=True, slots=True)
class CandidateSpec:
    hypothesis_id: str
    mode: LexicalCandidateMode

    def configuration(self) -> dict[str, object]:
        return {
            "hypothesis_id": self.hypothesis_id,
            "lexical_mode": self.mode.value,
            "source": "canonical_post_content",
            "result_limit": RESULT_LIMIT,
            "pool_limit": POOL_LIMIT,
        }


@dataclass(frozen=True, slots=True)
class PhaseResult:
    metrics: EvaluationMetrics
    timings: dict[str, dict[str, float | int]]
    raw_timings: dict[str, list[float]]


@dataclass(frozen=True, slots=True)
class BaselineSnapshot:
    """Content-free quality evidence copied once from the immutable baseline row."""

    run_id: int
    index_version: int
    recall_at_k: float
    mrr: float
    ndcg: float
    duplicate_source_share: float
    historical_latency_ms: int | None

    def record(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "index_version": self.index_version,
            "metrics": {
                "recall_at_k": self.recall_at_k,
                "mrr": self.mrr,
                "ndcg": self.ndcg,
                "duplicate_source_share": self.duplicate_source_share,
            },
            # KnowledgeEvaluationRun has only a historical mean, never phase percentiles.
            "latency": {
                "historical_mean_ms": self.historical_latency_ms,
                "phase_percentiles_available": False,
            },
        }


@dataclass(slots=True)
class CandidateOutcome:
    spec: CandidateSpec
    state: CandidateState
    decision: PromotionDecision
    decision_reason: str
    development: PhaseResult | None
    holdout: PhaseResult | None = None


CHALLENGER_LEXICAL_CANDIDATES = (
    CandidateSpec("russian_fts", LexicalCandidateMode.RUSSIAN_FTS),
    CandidateSpec("exact_short_circuit", LexicalCandidateMode.EXACT_SHORT_CIRCUIT),
)


def validate_database_url(database_url: str) -> None:
    """Accept only the internally addressed, explicitly labelled clone database."""
    _validated_experiment_database_url(database_url)


def experiment_database_url_for_engine(database_url: str) -> str:
    """Remove the validated BL-21 routing marker before SQLAlchemy sees the URL."""
    url = _validated_experiment_database_url(database_url)
    return url.set(query={}).render_as_string(hide_password=False)


def _validated_experiment_database_url(database_url: str):
    """Parse and strictly validate the only DB URL accepted by the experiment runner."""
    try:
        url = make_url(database_url)
    except Exception as exc:
        raise ExperimentError("database URL is invalid") from exc
    if url.drivername != "postgresql+asyncpg":
        raise ExperimentError("database URL must use the isolated asyncpg PostgreSQL driver")
    if url.host != EXPERIMENT_DATABASE_HOST or url.database != EXPERIMENT_DATABASE_NAME:
        raise ExperimentError("database URL does not name the isolated experiment clone")
    if dict(url.query) != {"experiment": EXPERIMENT_DATABASE_MARKER}:
        raise ExperimentError("database URL must include exactly experiment=bl21")
    return url


def validate_preflight(
    *,
    experiment_root: Path,
    database_url: str,
    dataset: Path,
    channel: str,
    campaign_key: str,
) -> PreflightEvidence:
    """Validate every immutable input without creating reports or opening a database."""
    preflight_experiment_dir(experiment_root, create=False, launcher_git_metadata=_launcher_git_metadata())
    validate_database_url(database_url)
    require_safe_identifier(campaign_key, "campaign_id")
    if not channel.strip():
        raise ExperimentError("channel must not be empty")
    manifest, manifest_hash = _load_manifest(experiment_root)
    dataset_hash, cases = _validate_dataset(experiment_root, dataset, manifest)
    snapshot = _mapping(manifest["logical_snapshot"], "logical_snapshot")
    snapshot_hash = require_sha256(snapshot.get("sha256"), "logical_snapshot.sha256")
    if snapshot.get("format") != "pg_dump_custom" or not isinstance(snapshot.get("bytes"), int) or isinstance(snapshot.get("bytes"), bool) or snapshot["bytes"] < 1:
        raise ExperimentError("clone manifest snapshot metadata is invalid")
    table_counts = _validate_table_counts(_mapping(manifest["table_counts"], "table_counts"))
    policy = ExperimentPolicy()
    configuration = _campaign_configuration(channel, dataset_hash, snapshot_hash)
    campaign = create_campaign(campaign_key, configuration, dataset_hash)
    return PreflightEvidence(
        campaign_key=campaign_key,
        channel_sha256=configuration["channel_sha256"],
        dataset_sha256=dataset_hash,
        dataset_case_count=len(cases),
        source_snapshot_sha256=snapshot_hash,
        source_snapshot_table_count=len(table_counts),
        manifest_sha256=manifest_hash,
        config_sha256=campaign.config_sha256,
        resume_key=campaign.resume_key,
        policy_sha256=config_sha256(policy),
        dataset_cases=cases,
        snapshot_table_counts=table_counts,
    )


async def execute_experiment(
    *,
    experiment_root: Path,
    database_url: str,
    dataset: Path,
    channel: str,
    campaign_key: str,
    baseline_run_id: int,
) -> dict[str, object]:
    """Run only declared zero-cost lexical candidates in the isolated clone."""
    evidence = validate_preflight(
        experiment_root=experiment_root,
        database_url=database_url,
        dataset=dataset,
        channel=channel,
        campaign_key=campaign_key,
    )
    split = split_ids(case.id for case in evidence.dataset_cases)
    by_id = {case.id: case for case in evidence.dataset_cases}
    development_cases = [by_id[case_id] for case_id in split.train_ids]
    holdout_cases = [by_id[case_id] for case_id in split.holdout_ids]
    if not development_cases or not holdout_cases:
        raise ExperimentError("dataset split must contain development and holdout cases")

    engine = create_async_engine(experiment_database_url_for_engine(database_url), pool_pre_ping=True)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with session_factory() as session:
            await _validate_database_snapshot(session, evidence.snapshot_table_counts)
            baseline = await _load_baseline_snapshot(
                session,
                baseline_run_id=baseline_run_id,
                channel=channel,
                dataset_sha256=evidence.dataset_sha256,
            )
            outcomes, campaign = await _run_campaign(
                session,
                evidence=evidence,
                channel=channel,
                development_cases=development_cases,
                holdout_cases=holdout_cases,
                baseline=baseline,
            )
            await session.commit()
    finally:
        await engine.dispose()

    report = _report(campaign, split.reportable(), outcomes)
    report_path = write_experiment_report(
        experiment_root,
        f"{campaign_key}-{campaign.config_sha256[:16]}.json",
        report,
        launcher_git_metadata=_launcher_git_metadata(),
    )
    return {
        "campaign_sha256": campaign.config_sha256,
        "candidate_count": len(outcomes),
        "report_sha256": hashlib.sha256(report_path.read_bytes()).hexdigest(),
    }


async def _run_campaign(
    session: AsyncSession,
    *,
    evidence: PreflightEvidence,
    channel: str,
    development_cases: Sequence[EvaluationCase],
    holdout_cases: Sequence[EvaluationCase],
    baseline: BaselineSnapshot,
) -> tuple[list[CandidateOutcome], object]:
    repo = ExperimentRepository(session)
    policy = ExperimentPolicy(allow_automatic_promotion=False)
    baseline_record = baseline.record()
    baseline_snapshot_sha256 = config_sha256(baseline_record)
    configuration = _campaign_configuration(
        channel,
        evidence.dataset_sha256,
        evidence.source_snapshot_sha256,
        baseline_snapshot_sha256,
    )
    campaign = await repo.create_or_get_campaign(
        campaign_key=evidence.campaign_key,
        channel_sha256=evidence.channel_sha256,
        dataset_sha256=evidence.dataset_sha256,
        source_snapshot_sha256=evidence.source_snapshot_sha256,
        source_snapshot_table_count=evidence.source_snapshot_table_count,
        baseline_run_id=baseline.run_id,
        baseline_snapshot_sha256=baseline_snapshot_sha256,
        baseline_snapshot=baseline_record,
        configuration=configuration,
        policy=policy,
    )
    if campaign.status == CampaignState.DRAFT:
        await repo.transition_campaign(campaign, CampaignState.READY)
    elif campaign.status == CampaignState.FAILED:
        await repo.resume_campaign(campaign, configuration)
    elif campaign.status != CampaignState.READY:
        raise ExperimentError("campaign is not available for lexical execution")
    await repo.transition_campaign(campaign, CampaignState.RUNNING)

    retriever = CanonicalLexicalCandidateRetriever(session, channel_username=channel, result_limit=RESULT_LIMIT, pool_limit=POOL_LIMIT)
    await retriever.resolve_channel()
    claimed = {}
    outcomes: list[CandidateOutcome] = []
    for spec in CHALLENGER_LEXICAL_CANDIDATES:
        candidate = await repo.claim_candidate(
            campaign,
            hypothesis_id=spec.hypothesis_id,
            configuration=spec.configuration(),
            index_label="canonical_post_content",
            projected_cost_usd=Decimal("0"),
        )
        if candidate is None:
            raise ExperimentError("existing candidate prevents a repeat lexical execution")
        claimed[spec.hypothesis_id] = candidate
        try:
            development = await _evaluate_phase(retriever, spec.mode, development_cases, policy)
        except ExperimentError as exc:
            await repo.fail_candidate(candidate, _failure_code(exc))
            outcomes.append(CandidateOutcome(spec, CandidateState.FAILED, PromotionDecision.FAILING, _failure_code(exc), None))
        else:
            outcomes.append(CandidateOutcome(spec, CandidateState.RUNNING, PromotionDecision.INSUFFICIENT_EVIDENCE, "development_pending", development))

    selected = _select_on_development(outcomes, policy, baseline)
    selected_ids = {selected.spec.hypothesis_id} if selected is not None else set()
    for outcome in outcomes:
        candidate = claimed[outcome.spec.hypothesis_id]
        if outcome.development is None:
            continue
        if outcome.spec.hypothesis_id not in selected_ids:
            outcome.state = CandidateState.SKIPPED
            outcome.decision = _development_decision(outcome, policy, baseline)
            outcome.decision_reason = _development_reason(outcome, baseline)
            await repo.skip_candidate(
                candidate,
                dev_metrics=outcome.development.metrics,
                phase_timings_ms=_prefixed_timings("development", outcome.development.raw_timings),
                decision=outcome.decision,
                decision_reason=outcome.decision_reason,
            )
            continue
        try:
            outcome.holdout = await _evaluate_phase(retriever, outcome.spec.mode, holdout_cases, policy)
        except ExperimentError as exc:
            outcome.state = CandidateState.FAILED
            outcome.decision = PromotionDecision.FAILING
            outcome.decision_reason = _failure_code(exc)
            await repo.fail_candidate(candidate, outcome.decision_reason)
            continue
        outcome.state = CandidateState.EVALUATED
        outcome.decision, outcome.decision_reason = _holdout_decision(outcome, baseline, policy)
        timings = _prefixed_timings("development", outcome.development.raw_timings)
        timings.update(_prefixed_timings("holdout", outcome.holdout.raw_timings))
        await repo.complete_candidate(
            candidate,
            dev_metrics=outcome.development.metrics,
            holdout_metrics=outcome.holdout.metrics,
            phase_timings_ms=timings,
            actual_cost_usd=Decimal("0"),
            decision=outcome.decision,
            decision_reason=outcome.decision_reason,
        )
    await repo.transition_campaign(campaign, CampaignState.COMPLETED)
    return outcomes, campaign


async def _evaluate_phase(
    retriever: CanonicalLexicalCandidateRetriever,
    mode: LexicalCandidateMode,
    cases: Sequence[EvaluationCase],
    policy: ExperimentPolicy,
) -> PhaseResult:
    recalls: list[float] = []
    mrrs: list[float] = []
    ndcgs: list[float] = []
    duplicate_shares: list[float] = []
    diversities: list[float] = []
    insufficient_count = 0
    retrieval_timings: list[float] = []
    lexical_timings: list[float] = []
    for case in cases:
        started = time.monotonic()
        result = await retriever.retrieve(mode=mode, query=case.question)
        retrieval_timings.append((time.monotonic() - started) * 1000)
        lexical_timings.append(result.lexical_ms)
        metrics = retrieval_metrics(case.expected_telegram_post_ids, result.telegram_post_ids, limit=RESULT_LIMIT)
        recalls.append(metrics.recall_at_k)
        mrrs.append(metrics.mrr)
        ndcgs.append(metrics.ndcg)
        duplicate_shares.append(duplicate_share(result.telegram_post_ids))
        diversities.append(source_diversity(result.telegram_post_ids))
        insufficient_count += int(len(result.telegram_post_ids) < policy.min_sources_per_case)
    aggregate = EvaluationMetrics(
        case_count=len(cases),
        retrieval=RetrievalMetrics(sum(recalls) / len(recalls), sum(mrrs) / len(mrrs), sum(ndcgs) / len(ndcgs)),
        duplicate_source_share=sum(duplicate_shares) / len(duplicate_shares),
        source_diversity=sum(diversities) / len(diversities),
        insufficient_evidence_count=insufficient_count,
    )
    raw_timings = {"retrieval": retrieval_timings, "lexical": lexical_timings}
    return PhaseResult(aggregate, phase_timing_summary(raw_timings), raw_timings)


def _select_on_development(outcomes: Sequence[CandidateOutcome], policy: ExperimentPolicy, baseline: BaselineSnapshot) -> CandidateOutcome | None:
    eligible = [outcome for outcome in outcomes if outcome.development is not None and _development_decision(outcome, policy, baseline) == PromotionDecision.PASSING_FOR_REVIEW]
    if not eligible:
        return None
    # This key is development-only. Holdout is never read before selection.
    return max(eligible, key=lambda outcome: _quality_key(outcome.development.metrics))


def _development_decision(outcome: CandidateOutcome, policy: ExperimentPolicy, baseline: BaselineSnapshot) -> PromotionDecision:
    assert outcome.development is not None
    decision = promotion_decision(outcome.development.metrics, policy, initial_dataset=True)
    if outcome.spec.mode == LexicalCandidateMode.EXACT_SHORT_CIRCUIT and not _no_regression(outcome.development.metrics, baseline):
        return PromotionDecision.FAILING
    return decision


def _development_reason(outcome: CandidateOutcome, baseline: BaselineSnapshot) -> str:
    assert outcome.development is not None
    if outcome.spec.mode == LexicalCandidateMode.EXACT_SHORT_CIRCUIT and not _no_regression(outcome.development.metrics, baseline):
        return "development_quality_regression"
    return "development_not_selected"


def _holdout_decision(outcome: CandidateOutcome, baseline: BaselineSnapshot, policy: ExperimentPolicy) -> tuple[PromotionDecision, str]:
    assert outcome.development is not None and outcome.holdout is not None
    development_decision = _development_decision(outcome, policy, baseline)
    holdout_decision = promotion_decision(outcome.holdout.metrics, policy, initial_dataset=True)
    if development_decision == PromotionDecision.INSUFFICIENT_EVIDENCE or holdout_decision == PromotionDecision.INSUFFICIENT_EVIDENCE:
        return PromotionDecision.INSUFFICIENT_EVIDENCE, "insufficient_evidence"
    if development_decision != PromotionDecision.PASSING_FOR_REVIEW or holdout_decision != PromotionDecision.PASSING_FOR_REVIEW:
        return PromotionDecision.FAILING, "quality_gate_failed"
    if outcome.spec.mode == LexicalCandidateMode.EXACT_SHORT_CIRCUIT and not _no_regression(outcome.holdout.metrics, baseline):
        return PromotionDecision.FAILING, "holdout_quality_regression"
    return PromotionDecision.PASSING_FOR_REVIEW, "development_selected_holdout_review"


def _no_regression(candidate: EvaluationMetrics, baseline: BaselineSnapshot) -> bool:
    return (
        candidate.retrieval.recall_at_k >= baseline.recall_at_k
        and candidate.retrieval.mrr >= baseline.mrr
        and candidate.retrieval.ndcg >= baseline.ndcg
        and candidate.duplicate_source_share <= baseline.duplicate_source_share
    )


def _quality_key(metrics: EvaluationMetrics) -> tuple[float, float, float, float, float, int]:
    return (
        metrics.retrieval.recall_at_k,
        metrics.retrieval.mrr,
        metrics.retrieval.ndcg,
        -metrics.duplicate_source_share,
        metrics.source_diversity,
        -metrics.insufficient_evidence_count,
    )


def _prefixed_timings(prefix: str, timings: Mapping[str, list[float]]) -> dict[str, list[float]]:
    return {f"{prefix}_{phase}": list(values) for phase, values in timings.items()}


def _failure_code(error: ExperimentError) -> str:
    if "PostgreSQL" in str(error):
        return "postgresql_required"
    if "catalog channel" in str(error):
        return "catalog_scope_rejected"
    return "lexical_evaluation_failed"


def _campaign_configuration(channel: str, dataset_sha256: str, snapshot_sha256: str, baseline_snapshot_sha256: str | None = None) -> dict[str, object]:
    configuration: dict[str, object] = {
        "runner_schema_version": 3,
        "channel_sha256": hash_identifier(channel.strip().lower()),
        "dataset_sha256": dataset_sha256,
        "source_snapshot_sha256": snapshot_sha256,
        "candidate_set_sha256": config_sha256([spec.configuration() for spec in CHALLENGER_LEXICAL_CANDIDATES]),
    }
    if baseline_snapshot_sha256 is not None:
        configuration["baseline_snapshot_sha256"] = require_sha256(baseline_snapshot_sha256, "baseline_snapshot_sha256")
    return configuration


async def _validate_database_snapshot(session: AsyncSession, table_counts: Sequence[tuple[str, int]]) -> None:
    """Compare preflight-bound manifest counts before any control-plane write."""
    for table_name, expected_count in table_counts:
        actual = await session.scalar(text(f'SELECT count(*) FROM "{table_name}"'))
        if actual != expected_count:
            raise ExperimentError("isolated database snapshot counts do not match manifest")


async def _load_baseline_snapshot(
    session: AsyncSession,
    *,
    baseline_run_id: int,
    channel: str,
    dataset_sha256: str,
) -> BaselineSnapshot:
    """Bind execution to one ready-catalog row in the isolated experiment database."""
    if not isinstance(baseline_run_id, int) or isinstance(baseline_run_id, bool) or baseline_run_id < 1:
        raise ExperimentError("baseline_run_id must be a positive integer")
    baseline = (await session.execute(
        select(KnowledgeEvaluationRun, KnowledgeChannel, Channel)
        .join(KnowledgeChannel, KnowledgeEvaluationRun.knowledge_channel_id == KnowledgeChannel.id)
        .join(Channel, KnowledgeChannel.channel_id == Channel.id)
        .where(KnowledgeEvaluationRun.id == baseline_run_id)
    )).one_or_none()
    if baseline is None:
        raise ExperimentError("baseline evaluation run is absent from the isolated experiment database")
    run, catalog, catalog_channel = baseline
    normalized_channel = channel.strip().removeprefix("@").lower()
    if catalog.state != KnowledgeChannelState.READY or catalog_channel.username.lower() != normalized_channel:
        raise ExperimentError("baseline evaluation run does not belong to the approved catalog channel")
    if run.dataset_hash != dataset_sha256:
        raise ExperimentError("baseline evaluation run dataset SHA does not match the labelled dataset")
    if catalog.active_index_version is not None and run.index_version != catalog.active_index_version:
        raise ExperimentError("baseline evaluation run index version does not match the active catalog index")
    metrics = (run.recall_at_k, run.mrr, run.ndcg, run.duplicate_source_share)
    if any(not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(value) or not 0 <= value <= 1 for value in metrics):
        raise ExperimentError("baseline evaluation run lacks required quality metrics")
    if not isinstance(run.index_version, int) or run.index_version < 1:
        raise ExperimentError("baseline evaluation run has invalid index metadata")
    if run.latency_ms is not None and (not isinstance(run.latency_ms, int) or isinstance(run.latency_ms, bool) or run.latency_ms < 0):
        raise ExperimentError("baseline evaluation run has invalid latency metadata")
    return BaselineSnapshot(
        run_id=run.id,
        index_version=run.index_version,
        recall_at_k=float(run.recall_at_k),
        mrr=float(run.mrr),
        ndcg=float(run.ndcg),
        duplicate_source_share=float(run.duplicate_source_share),
        historical_latency_ms=run.latency_ms,
    )


def _report(campaign: object, split: dict[str, object], outcomes: Sequence[CandidateOutcome]) -> dict[str, object]:
    baseline_snapshot = getattr(campaign, "baseline_snapshot")
    baseline_snapshot_sha256 = getattr(campaign, "baseline_snapshot_sha256")
    return {
        "schema_version": 4,
        "campaign": {
            "config_sha256": getattr(campaign, "config_sha256"),
            "dataset_sha256": getattr(campaign, "dataset_sha256"),
            "resume_key": getattr(campaign, "resume_key"),
            "state": getattr(campaign, "status").value,
            "split": split,
            "budget": {"limit_usd": "1.00", "reserved_usd": "0", "actual_usd": "0"},
            "baseline_run_id": getattr(campaign, "baseline_run_id"),
            "baseline_snapshot_sha256": baseline_snapshot_sha256,
            "baseline_snapshot": baseline_snapshot,
        },
        "candidates": [_candidate_report(outcome, baseline_snapshot_sha256, baseline_snapshot) for outcome in outcomes],
    }


def _candidate_report(
    outcome: CandidateOutcome,
    baseline_snapshot_sha256: str,
    baseline_snapshot: Mapping[str, object],
) -> dict[str, object]:
    return {
        "candidate_key": config_sha256(outcome.spec.configuration()),
        "state": outcome.state.value,
        "decision": outcome.decision.value,
        "decision_reason": outcome.decision_reason,
        "configuration": outcome.spec.configuration(),
        "development": _phase_report(outcome.development),
        "holdout": _phase_report(outcome.holdout),
        "comparison": {
            "baseline_snapshot_sha256": baseline_snapshot_sha256,
            "quality_deltas": {
                "development": _quality_deltas(outcome.development, baseline_snapshot),
                "holdout": _quality_deltas(outcome.holdout, baseline_snapshot),
            },
            "latency": {"status": "baseline_phase_percentiles_unavailable"},
        },
    }


def _phase_report(result: PhaseResult | None) -> dict[str, object] | None:
    if result is None:
        return None
    return {"metrics": evaluation_metrics_record(result.metrics), "timings": result.timings}


def _quality_deltas(result: PhaseResult | None, baseline_snapshot: Mapping[str, object]) -> dict[str, float] | None:
    if result is None:
        return None
    baseline_metrics = _mapping(baseline_snapshot["metrics"], "baseline snapshot metrics")
    return {
        "recall_at_k": result.metrics.retrieval.recall_at_k - float(baseline_metrics["recall_at_k"]),
        "mrr": result.metrics.retrieval.mrr - float(baseline_metrics["mrr"]),
        "ndcg": result.metrics.retrieval.ndcg - float(baseline_metrics["ndcg"]),
        "duplicate_source_share": result.metrics.duplicate_source_share - float(baseline_metrics["duplicate_source_share"]),
    }


def _load_manifest(experiment_root: Path) -> tuple[Mapping[str, object], str]:
    manifest_path = experiment_root / MANIFEST_RELATIVE_PATH
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise UnsafeExperimentPath("clone manifest must be a regular file")
    raw = _read_regular_file(manifest_path, "clone manifest")
    try:
        manifest = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ExperimentError("clone manifest is invalid JSON") from exc
    root = _mapping(manifest, "clone manifest")
    if root.get("schema_version") != 1:
        raise ExperimentError("unsupported clone manifest schema")
    _mapping(root.get("dataset"), "dataset")
    _mapping(root.get("logical_snapshot"), "logical_snapshot")
    _mapping(root.get("table_counts"), "table_counts")
    return root, hashlib.sha256(raw).hexdigest()


def _validate_dataset(experiment_root: Path, dataset: Path, manifest: Mapping[str, object]) -> tuple[str, tuple[EvaluationCase, ...]]:
    metadata = _mapping(manifest["dataset"], "dataset")
    relative_path = metadata.get("path")
    expected_hash = require_sha256(metadata.get("sha256"), "dataset.sha256")
    expected_bytes = metadata.get("bytes")
    if not isinstance(relative_path, str) or Path(relative_path).is_absolute() or ".." in Path(relative_path).parts:
        raise ExperimentError("dataset manifest path is unsafe")
    if not isinstance(expected_bytes, int) or isinstance(expected_bytes, bool) or expected_bytes < 1:
        raise ExperimentError("dataset manifest bytes are invalid")
    expected = experiment_root / ".data-experiment" / relative_path
    if dataset.is_symlink() or dataset.absolute() != expected.absolute() or not dataset.is_file():
        raise UnsafeExperimentPath("dataset must be the manifest-declared regular input")
    raw = _read_regular_file(dataset, "dataset")
    if len(raw) != expected_bytes or hashlib.sha256(raw).hexdigest() != expected_hash:
        raise ExperimentError("dataset bytes do not match the immutable clone manifest")
    cases, loaded_hash = load_dataset_bytes(raw)
    if loaded_hash != expected_hash:
        raise ExperimentError("dataset loader hash does not match the immutable clone manifest")
    return loaded_hash, tuple(cases)


def _validate_table_counts(table_counts: Mapping[str, object]) -> tuple[tuple[str, int], ...]:
    snapshots = []
    for key in ("source_at_snapshot", "source_post_restore", "test_after_restore"):
        counts = _mapping(table_counts.get(key), key)
        if not counts or any(
            not isinstance(table_name, str)
            or not _SAFE_TABLE_NAME.fullmatch(table_name)
            or not isinstance(value, str)
            or not value.isdecimal()
            for table_name, value in counts.items()
        ):
            raise ExperimentError("clone manifest table counts are invalid")
        snapshots.append(counts)
    if snapshots[0] != snapshots[1] or snapshots[0] != snapshots[2]:
        raise ExperimentError("clone manifest table counts do not match")
    if table_counts.get("table_count") != len(snapshots[0]):
        raise ExperimentError("clone manifest table count is inconsistent")
    if table_counts.get("snapshot_equals_test") is not True or table_counts.get("source_post_restore_equals_test") is not True:
        raise ExperimentError("clone manifest does not certify the isolated restore")
    return tuple(sorted((table_name, int(count)) for table_name, count in snapshots[2].items()))


def _read_regular_file(path: Path, label: str) -> bytes:
    """Read one non-symlink file descriptor so later path replacement cannot affect it."""
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    except OSError as exc:
        raise UnsafeExperimentPath(f"{label} must be a regular file") from exc
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise UnsafeExperimentPath(f"{label} must be a regular file")
        with os.fdopen(descriptor, "rb") as input_file:
            descriptor = -1
            return input_file.read()
    finally:
        if descriptor != -1:
            os.close(descriptor)


def _launcher_git_metadata() -> Mapping[str, str] | None:
    branch = os.environ.get(RUNNER_GIT_BRANCH_ENV)
    revision = os.environ.get(RUNNER_GIT_REVISION_ENV)
    if branch is None and revision is None:
        return None
    return {
        RUNNER_GIT_BRANCH_ENV: branch if branch is not None else "",
        RUNNER_GIT_REVISION_ENV: revision if revision is not None else "",
    }


def _mapping(value: object, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ExperimentError(f"{label} must be an object")
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--experiment-root", type=Path, required=True)
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--channel", required=True)
    parser.add_argument("--campaign-id", required=True)
    parser.add_argument("--baseline-run-id", type=int)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--execute", action="store_true")
    mode.add_argument("--execute-vector", action="store_true")
    parser.add_argument("--vector-candidate")
    # Explicitly reject undeclared provider/index paths rather than silently ignoring them.
    parser.add_argument("--vector", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--model", help=argparse.SUPPRESS)
    parser.add_argument("--reindex", action="store_true", help=argparse.SUPPRESS)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.vector or args.model is not None or args.reindex:
        raise ExperimentError("undeclared vector, model, and reindex selections are not permitted")
    if args.vector_candidate is not None and not args.execute_vector:
        raise ExperimentError("--vector-candidate requires --execute-vector")
    if args.dry_run:
        if args.baseline_run_id is not None:
            raise ExperimentError("--baseline-run-id is only permitted with --execute")
        evidence = validate_preflight(
            experiment_root=args.experiment_root,
            database_url=args.database_url,
            dataset=args.dataset,
            channel=args.channel,
            campaign_key=args.campaign_id,
        )
        print(json.dumps(evidence.record(), sort_keys=True), flush=True)
        return 0
    if args.baseline_run_id is None or args.baseline_run_id < 1:
        raise ExperimentError("--execute requires --baseline-run-id from the isolated experiment database")
    if args.execute_vector:
        if args.vector_candidate is None:
            raise ExperimentError("--execute-vector requires one allowlisted --vector-candidate")
        # This request deliberately stops after all isolated-root and pricing checks.
        # A real execution must inject a local vector client and an explicitly metered embedder.
        vector_candidate_config(args.vector_candidate)
        evidence = validate_preflight(
            experiment_root=args.experiment_root,
            database_url=args.database_url,
            dataset=args.dataset,
            channel=args.channel,
            campaign_key=args.campaign_id,
        )
        result = validate_vector_execution(args.vector_candidate, args.experiment_root)
        print(json.dumps({"campaign_sha256": evidence.config_sha256, **result, "execution": "vector_preflight_only"}, sort_keys=True), flush=True)
        return 0
    result = asyncio.run(execute_experiment(
        experiment_root=args.experiment_root,
        database_url=args.database_url,
        dataset=args.dataset,
        channel=args.channel,
        campaign_key=args.campaign_id,
        baseline_run_id=args.baseline_run_id,
    ))
    print(json.dumps(result, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
