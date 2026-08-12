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

from sqlalchemy import func, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.knowledge.chunking import estimate_tokens
from src.knowledge.evaluation import EvaluationCase, load_dataset_bytes, split_evaluation_cases
from src.knowledge.experiment_abstention import AbstentionSample, select_abstention_threshold, should_abstain
from src.knowledge.experiment_repository import ExperimentRepository
from src.knowledge.experiment_retriever import CanonicalLexicalCandidateRetriever, LexicalCandidateMode
from src.knowledge.experiment_hybrid import HybridMethod, HybridPost, PrivateHybridIndex, parent_dense_vectors_from_snapshot
from src.knowledge.experiment_vector import (
    DEFAULT_EMBEDDING_MODEL,
    ExperimentRepresentationRetriever,
    IsolatedExperimentVectorIndex,
    OperatorEmbeddingPricing,
    VectorCandidateConfig,
    clone_vector_snapshot,
    local_experiment_vector_index,
    vector_candidate_config,
    vector_identity,
    vector_series_identity,
)
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
from src.models.knowledge import IndexStatus, KnowledgeChannel, KnowledgeChannelState, KnowledgeEvaluationRun, KnowledgeRepresentation, RepresentationType
from src.models.post import Post
from src.llm.openrouter import OpenRouterClient


EXPERIMENT_DATABASE_NAME = "telegram_bot_bl21_experiment"
EXPERIMENT_DATABASE_HOST = "db"
EXPERIMENT_DATABASE_MARKER = "bl21"
MANIFEST_RELATIVE_PATH = Path(".data-experiment/clone-manifest.json")
RESULT_LIMIT = 5
POOL_LIMIT = 30
RERANK_MODEL = "cohere/rerank-4-pro"
RERANK_PRICE_USD = Decimal("0.0025")
RERANK_BUDGET_USD = Decimal("0.50")
_SAFE_TABLE_NAME = re.compile(r"[a-z][a-z0-9_]*\Z")
# These rows are evidence produced by the isolated runner itself.  They cannot
# be part of the immutable corpus-snapshot comparison, otherwise one completed
# measurement would make every later measurement fail before it starts.
_EXPERIMENT_MUTABLE_TABLES = frozenset({
    "knowledge_evaluation_runs",
    "experiment_campaign_locks",
    "experiment_campaigns",
    "experiment_candidates",
})


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
    category_metrics: dict[str, EvaluationMetrics] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class RerankSettings:
    model: str = RERANK_MODEL
    pool_limit: int = 20
    result_limit: int = RESULT_LIMIT

    def __post_init__(self) -> None:
        if self.model != RERANK_MODEL or self.pool_limit not in {20, 30} or self.result_limit != RESULT_LIMIT:
            raise ExperimentError("rerank configuration is not allowlisted")

    @property
    def projected_cost_usd(self) -> Decimal:
        return RERANK_PRICE_USD

    def configuration(self) -> dict[str, object]:
        return {"hypothesis_id": f"rerank_{self.pool_limit}", "source": "rerank_canonical_posts", "model": self.model, "pool_limit": self.pool_limit, "result_limit": self.result_limit, "price_per_request_usd": format(RERANK_PRICE_USD, "f")}


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


@dataclass(frozen=True, slots=True)
class VectorSnapshot:
    """Content-free manifest binding for a copied production Qdrant root."""

    relative_path: Path
    sha256: str
    collection_name: str
    embedding_model: str
    dimensions: int
    index_version: int


def _dataset_split(cases: Sequence[EvaluationCase]):
    """Prefer the labels' immutable phase assignment over the historical hash split."""
    development, holdout = split_evaluation_cases(cases)
    if development or holdout:
        split = split_ids(case.id for case in cases)
        # Keep the existing content-free report shape while binding it to the
        # explicit phase labels rather than a hash-derived reallocation.
        split = type(split)(
            train_ids=frozenset(case.id for case in development),
            holdout_ids=frozenset(case.id for case in holdout),
        )
        return split, development, holdout
    split = split_ids(case.id for case in cases)
    by_id = {case.id: case for case in cases}
    return (
        split,
        [by_id[case_id] for case_id in split.train_ids],
        [by_id[case_id] for case_id in split.holdout_ids],
    )


@dataclass(frozen=True, slots=True)
class PhaseBaseline:
    provenance: BaselineSnapshot
    development: PhaseResult
    holdout: PhaseResult

    def record(self) -> dict[str, object]:
        return {
            "provenance": self.provenance.record(),
            "phases": {
                "development": _phase_report(self.development),
                "holdout": _phase_report(self.holdout),
            },
        }


@dataclass(slots=True)
class CandidateOutcome:
    spec: CandidateSpec | VectorCandidateConfig
    state: CandidateState
    decision: PromotionDecision
    decision_reason: str
    development: PhaseResult | None
    holdout: PhaseResult | None = None
    configuration: Mapping[str, object] | None = None


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
    split, development_cases, holdout_cases = _dataset_split(evidence.dataset_cases)
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


VECTOR_CONTROL = "hybrid_all"
VECTOR_EXPERIMENT_CYCLES: dict[str, tuple[str, ...]] = {
    # The completed first cycle compared the broad production-like control
    # against the three predeclared alternatives.
    VECTOR_CONTROL: ("vector_all", "hybrid_summary", "hybrid_full"),
    # Later work remains deliberately one hypothesis at a time.
    "vector_summary": ("vector_summary",),
    "vector_full": ("vector_full",),
}
VECTOR_SERIES_CHALLENGERS = ("vector_full", "vector_chunk", "hybrid_full", "hybrid_chunk")
VECTOR_SERIES_KEY = "bl21-retrieval-matrix-v1"
PRIVATE_HYBRID_KEY = "bl21-private-hybrid-v1"
QWEN_QUERY_INSTRUCTION = "Retrieve original Telegram posts that directly answer the user question."
MINIMUM_REPRESENTATION_POINTS = 30


@dataclass(slots=True)
class SeriesCampaign:
    """Small in-memory report envelope for one sequential local series."""

    config_sha256: str
    dataset_sha256: str
    resume_key: str
    status: CampaignState
    baseline_run_id: int
    baseline_snapshot_sha256: str
    baseline_snapshot: dict[str, object]


def vector_experiment_cycle(candidate_id: str) -> tuple[str, ...]:
    try:
        return VECTOR_EXPERIMENT_CYCLES[candidate_id]
    except KeyError as exc:
        raise ExperimentError("vector candidate is not a declared experiment cycle") from exc


async def execute_vector_experiment(
    *,
    experiment_root: Path,
    database_url: str,
    dataset: Path,
    channel: str,
    campaign_key: str,
    baseline_run_id: int,
    vector_candidate: str,
) -> dict[str, object]:
    """Run the declared vector/hybrid ablations against one copied Qdrant snapshot.

    The only provider call is a single batch for the labelled questions.  Corpus
    vectors are copied from the snapshot into candidate-private roots and are
    never embedded, upserted, or read from production at execution time.
    """
    evidence = validate_preflight(
        experiment_root=experiment_root,
        database_url=database_url,
        dataset=dataset,
        channel=channel,
        campaign_key=campaign_key,
    )
    challenger_ids = vector_experiment_cycle(vector_candidate)
    vector_snapshot = _load_vector_snapshot(experiment_root)
    split, development_cases, holdout_cases = _dataset_split(evidence.dataset_cases)
    if not development_cases or not holdout_cases:
        raise ExperimentError("dataset split must contain development and holdout cases")
    query_tokens = sum(estimate_tokens(case.question) for case in evidence.dataset_cases)
    pricing = OperatorEmbeddingPricing(model_id=vector_snapshot.embedding_model)
    actual_cost = pricing.project(query_tokens)
    if actual_cost > Decimal("1.00"):
        raise ExperimentError("query embedding projection exceeds the BL-21 campaign budget")

    engine = create_async_engine(experiment_database_url_for_engine(database_url), pool_pre_ping=True)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with session_factory() as session:
            await _validate_database_snapshot(session, evidence.snapshot_table_counts)
            provenance = await _load_baseline_snapshot(
                session,
                baseline_run_id=baseline_run_id,
                channel=channel,
                dataset_sha256=evidence.dataset_sha256,
            )
            if provenance.index_version != vector_snapshot.index_version:
                raise ExperimentError("vector snapshot index version does not match baseline provenance")
            control = vector_candidate_config(VECTOR_CONTROL)
            control_retriever = await _vector_retriever(
                session,
                experiment_root=experiment_root,
                vector_snapshot=vector_snapshot,
                channel=channel,
                candidate=control,
            )
            query_vectors, embedded_query_tokens = await _embed_experiment_questions(
                evidence.dataset_cases,
                model_id=vector_snapshot.embedding_model,
                dimensions=vector_snapshot.dimensions,
            )
            if embedded_query_tokens != query_tokens:
                raise ExperimentError("query embedding token projection changed during execution")
            baseline = PhaseBaseline(
                provenance=provenance,
                development=await _evaluate_vector_phase(control_retriever, development_cases, query_vectors, embedding_ms=0.0, policy=ExperimentPolicy()),
                holdout=await _evaluate_vector_phase(control_retriever, holdout_cases, query_vectors, embedding_ms=0.0, policy=ExperimentPolicy()),
            )
            outcomes, campaign = await _run_vector_campaign(
                session,
                evidence=evidence,
                channel=channel,
                development_cases=development_cases,
                holdout_cases=holdout_cases,
                baseline=baseline,
                vector_snapshot=vector_snapshot,
                query_vectors=query_vectors,
                query_tokens=query_tokens,
                experiment_root=experiment_root,
                challenger_ids=challenger_ids,
            )
            await session.commit()
    finally:
        await engine.dispose()

    report = _report(campaign, split.reportable(), outcomes, actual_cost_usd=actual_cost)
    report_path = write_experiment_report(
        experiment_root,
        f"{campaign_key}-{campaign.config_sha256[:16]}.json",
        report,
        launcher_git_metadata=_launcher_git_metadata(),
    )
    return {
        "campaign_sha256": campaign.config_sha256,
        "candidate_count": len(outcomes),
        "actual_query_embedding_cost_usd": format(actual_cost, "f"),
        "report_sha256": hashlib.sha256(report_path.read_bytes()).hexdigest(),
    }


async def execute_vector_series(
    *,
    experiment_root: Path,
    database_url: str,
    dataset: Path,
    channel: str,
    baseline_run_id: int,
) -> dict[str, object]:
    """Run the remaining representation checks in one persistent local session.

    The series has one temporary Qdrant copy and one batch of question vectors.
    Each finished candidate replaces the same content-free progress report, so a
    later candidate failure cannot erase earlier measurements.
    """
    evidence = validate_preflight(
        experiment_root=experiment_root,
        database_url=database_url,
        dataset=dataset,
        channel=channel,
        campaign_key=VECTOR_SERIES_KEY,
    )
    vector_snapshot = _load_vector_snapshot(experiment_root)
    split, development_cases, holdout_cases = _dataset_split(evidence.dataset_cases)
    if not development_cases or not holdout_cases:
        raise ExperimentError("dataset split must contain development and holdout cases")
    query_tokens = sum(estimate_tokens(case.question) for case in evidence.dataset_cases)
    pricing = OperatorEmbeddingPricing(model_id=vector_snapshot.embedding_model)
    actual_cost = pricing.project(query_tokens)
    if actual_cost > Decimal("1.00"):
        raise ExperimentError("query embedding projection exceeds the BL-21 campaign budget")

    engine = create_async_engine(experiment_database_url_for_engine(database_url), pool_pre_ping=True)
    series_index: IsolatedExperimentVectorIndex | None = None
    try:
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        async with session_factory() as session:
            await _validate_database_snapshot(session, evidence.snapshot_table_counts)
            provenance = await _load_baseline_snapshot(
                session,
                baseline_run_id=baseline_run_id,
                channel=channel,
                dataset_sha256=evidence.dataset_sha256,
            )
            if provenance.index_version != vector_snapshot.index_version:
                raise ExperimentError("vector snapshot index version does not match baseline provenance")
            series_identity = vector_series_identity(
                experiment_root,
                series_key=VECTOR_SERIES_KEY,
                collection_name=vector_snapshot.collection_name,
            )
            if not series_identity.root.exists():
                clone_vector_snapshot(source_root=experiment_root / vector_snapshot.relative_path, identity=series_identity)
            series_index = local_experiment_vector_index(series_identity, dimensions=vector_snapshot.dimensions)
            series_index.require_snapshot_collection()
            control = vector_candidate_config(VECTOR_CONTROL)
            control_retriever = await _vector_retriever(
                session,
                experiment_root=experiment_root,
                vector_snapshot=vector_snapshot,
                channel=channel,
                candidate=control,
                index=series_index,
            )
            query_vectors, embedded_query_tokens = await _embed_experiment_questions(
                evidence.dataset_cases,
                model_id=vector_snapshot.embedding_model,
                dimensions=vector_snapshot.dimensions,
            )
            if embedded_query_tokens != query_tokens:
                raise ExperimentError("query embedding token projection changed during execution")
            # The control is remeasured with the same in-memory query vectors so
            # its latency and ranking evidence are phase-comparable.
            baseline = PhaseBaseline(
                provenance=provenance,
                development=await _evaluate_vector_phase(control_retriever, development_cases, query_vectors, embedding_ms=0.0, policy=ExperimentPolicy()),
                holdout=await _evaluate_vector_phase(control_retriever, holdout_cases, query_vectors, embedding_ms=0.0, policy=ExperimentPolicy()),
            )
            campaign = _series_campaign(evidence, baseline, vector_snapshot)
            outcomes: list[CandidateOutcome] = []
            _write_series_report(experiment_root, split.reportable(), campaign, outcomes, actual_cost_usd=actual_cost, state=CampaignState.RUNNING)
            for candidate_id in VECTOR_SERIES_CHALLENGERS:
                spec = vector_candidate_config(candidate_id)
                configuration = spec.configuration(pricing, token_total=query_tokens)
                try:
                    if not await _has_sufficient_representation_coverage(
                        session,
                        channel=channel,
                        index_version=provenance.index_version,
                        candidate=spec,
                    ):
                        outcome = CandidateOutcome(
                            spec,
                            CandidateState.SKIPPED,
                            PromotionDecision.INSUFFICIENT_EVIDENCE,
                            "representation_coverage_insufficient",
                            None,
                            configuration=configuration,
                        )
                        outcomes.append(outcome)
                        _write_series_report(experiment_root, split.reportable(), campaign, outcomes, actual_cost_usd=actual_cost, state=CampaignState.RUNNING)
                        continue
                    retriever = await _vector_retriever(
                        session,
                        experiment_root=experiment_root,
                        vector_snapshot=vector_snapshot,
                        channel=channel,
                        candidate=spec,
                        index=series_index,
                    )
                    development = await _evaluate_vector_phase(retriever, development_cases, query_vectors, embedding_ms=0.0, policy=ExperimentPolicy())
                    if _vector_development_decision(development, baseline.development) == PromotionDecision.FAILING:
                        outcome = CandidateOutcome(spec, CandidateState.SKIPPED, PromotionDecision.FAILING, "development_quality_regression", development, configuration=configuration)
                    else:
                        holdout = await _evaluate_vector_phase(retriever, holdout_cases, query_vectors, embedding_ms=0.0, policy=ExperimentPolicy())
                        decision = _vector_holdout_decision(holdout, baseline.holdout)
                        reason = "holdout_passing_for_review" if decision == PromotionDecision.PASSING_FOR_REVIEW else "holdout_promotion_threshold_not_met"
                        outcome = CandidateOutcome(spec, CandidateState.EVALUATED, decision, reason, development, holdout, configuration)
                except Exception:
                    outcome = CandidateOutcome(spec, CandidateState.FAILED, PromotionDecision.FAILING, "candidate_runtime_failure", None, configuration=configuration)
                outcomes.append(outcome)
                _write_series_report(experiment_root, split.reportable(), campaign, outcomes, actual_cost_usd=actual_cost, state=CampaignState.RUNNING)
            _write_series_report(experiment_root, split.reportable(), campaign, outcomes, actual_cost_usd=actual_cost, state=CampaignState.COMPLETED)
    finally:
        if series_index is not None:
            series_index.close()
        await engine.dispose()

    report_path = _series_report_path(experiment_root)
    return {
        "candidate_count": len(VECTOR_SERIES_CHALLENGERS),
        "actual_query_embedding_cost_usd": format(actual_cost, "f"),
        "report_sha256": hashlib.sha256(report_path.read_bytes()).hexdigest(),
    }


async def create_vector_baseline(
    *,
    experiment_root: Path,
    database_url: str,
    dataset: Path,
    channel: str,
    campaign_key: str,
    use_instruction: bool,
) -> dict[str, object]:
    """Measure and persist the new-dataset vector-only control in the clone DB."""
    evidence = validate_preflight(
        experiment_root=experiment_root,
        database_url=database_url,
        dataset=dataset,
        channel=channel,
        campaign_key=campaign_key,
    )
    split, development_cases, holdout_cases = _dataset_split(evidence.dataset_cases)
    if not development_cases or not holdout_cases:
        raise ExperimentError("baseline dataset needs both development and holdout cases")
    snapshot = _load_vector_snapshot(experiment_root)
    query_tokens = sum(estimate_tokens(_embedding_query(case.question, use_instruction=use_instruction)) for case in evidence.dataset_cases)
    pricing = OperatorEmbeddingPricing(model_id=snapshot.embedding_model)
    query_cost = pricing.project(query_tokens)
    if query_cost > Decimal("1.00"):
        raise ExperimentError("baseline query embedding projection exceeds the BL-21 budget")
    engine = create_async_engine(experiment_database_url_for_engine(database_url), pool_pre_ping=True)
    index: IsolatedExperimentVectorIndex | None = None
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as session:
            await _validate_database_snapshot(session, evidence.snapshot_table_counts)
            candidate = vector_candidate_config("vector_all")
            identity = vector_identity(experiment_root, candidate, collection_name=snapshot.collection_name)
            if not identity.root.exists():
                clone_vector_snapshot(source_root=experiment_root / snapshot.relative_path, identity=identity)
            index = local_experiment_vector_index(identity, dimensions=snapshot.dimensions)
            index.require_snapshot_collection()
            retriever = await _vector_retriever(
                session,
                experiment_root=experiment_root,
                vector_snapshot=snapshot,
                channel=channel,
                candidate=candidate,
                index=index,
            )
            vectors, embedded_tokens = await _embed_experiment_questions(
                evidence.dataset_cases,
                model_id=snapshot.embedding_model,
                dimensions=snapshot.dimensions,
                use_instruction=use_instruction,
            )
            if embedded_tokens != query_tokens:
                raise ExperimentError("baseline query token projection changed during execution")
            development_samples: list[AbstentionSample] = []
            # The first local pass exists solely to choose the fixed no-answer
            # threshold from development labels.  It reuses the already
            # received question vectors and makes no additional provider call.
            await _evaluate_vector_phase(
                retriever, development_cases, vectors, embedding_ms=0.0,
                policy=ExperimentPolicy(), confidence_samples=development_samples,
            )
            threshold = select_abstention_threshold(development_samples)
            development = await _evaluate_vector_phase(
                retriever, development_cases, vectors, embedding_ms=0.0,
                policy=ExperimentPolicy(), abstention_threshold=threshold,
            )
            holdout = await _evaluate_vector_phase(
                retriever, holdout_cases, vectors, embedding_ms=0.0,
                policy=ExperimentPolicy(), abstention_threshold=threshold,
            )
            catalog = (await session.execute(
                select(KnowledgeChannel)
                .join(Channel, Channel.id == KnowledgeChannel.channel_id)
                .where(Channel.username == channel.lstrip("@"), KnowledgeChannel.state == KnowledgeChannelState.READY)
            )).scalar_one()
            row = KnowledgeEvaluationRun(
                knowledge_channel_id=catalog.id,
                index_version=snapshot.index_version,
                dataset_hash=evidence.dataset_sha256,
                mode="bl21_vector_all_instruction" if use_instruction else "bl21_vector_all",
                recall_at_k=holdout.metrics.retrieval.recall_at_k,
                mrr=holdout.metrics.retrieval.mrr,
                ndcg=holdout.metrics.retrieval.ndcg,
                duplicate_source_share=holdout.metrics.duplicate_source_share,
                latency_ms=round(float(holdout.timings["retrieval"]["p50_ms"])),
                context_tokens=None,
                cost=float(query_cost),
            )
            session.add(row)
            await session.commit()
            return {
                "baseline_run_id": row.id,
                "dataset_sha256": evidence.dataset_sha256,
                "query_instruction": use_instruction,
                "no_answer_threshold": threshold,
                "development": evaluation_metrics_record(development.metrics),
                "holdout": evaluation_metrics_record(holdout.metrics),
                "actual_query_embedding_cost_usd": format(query_cost, "f"),
                "split": split.reportable(),
            }
    finally:
        if index is not None:
            index.close()
        await engine.dispose()


async def build_hybrid_collection(
    *,
    experiment_root: Path,
    database_url: str,
    dataset: Path,
    channel: str,
    campaign_key: str,
) -> dict[str, object]:
    """Create the private canonical-post BM25/vector collection without model calls."""
    evidence = validate_preflight(
        experiment_root=experiment_root,
        database_url=database_url,
        dataset=dataset,
        channel=channel,
        campaign_key=campaign_key,
    )
    snapshot = _load_vector_snapshot(experiment_root)
    engine = create_async_engine(experiment_database_url_for_engine(database_url), pool_pre_ping=True)
    index: PrivateHybridIndex | None = None
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as session:
            await _validate_database_snapshot(session, evidence.snapshot_table_counts)
            index = await _build_private_hybrid_index(
                session,
                experiment_root=experiment_root,
                vector_snapshot=snapshot,
                channel=channel,
            )
            return {
                "collection_sha256": config_sha256({"collection": index.collection_name}),
                "dataset_sha256": evidence.dataset_sha256,
                "vector_snapshot_sha256": snapshot.sha256,
            }
    finally:
        if index is not None:
            index.close()
        await engine.dispose()


async def execute_private_hybrid_comparisons(
    *,
    experiment_root: Path,
    database_url: str,
    dataset: Path,
    channel: str,
    campaign_key: str,
    baseline_run_id: int,
    use_instruction: bool = True,
) -> dict[str, object]:
    """Compare private BM25/dense fusions with one question-vector batch.

    Corpus vectors and post texts stay in the clone.  The only provider call is
    the same bounded batch of labelled question embeddings used by the vector
    control; BM25 and every fusion run locally in Qdrant.
    """
    evidence = validate_preflight(
        experiment_root=experiment_root, database_url=database_url, dataset=dataset,
        channel=channel, campaign_key=campaign_key,
    )
    split, development_cases, holdout_cases = _dataset_split(evidence.dataset_cases)
    if not development_cases or not holdout_cases:
        raise ExperimentError("hybrid dataset needs both development and holdout cases")
    snapshot = _load_vector_snapshot(experiment_root)
    query_tokens = sum(
        estimate_tokens(_embedding_query(case.question, use_instruction=use_instruction))
        for case in evidence.dataset_cases
    )
    pricing = OperatorEmbeddingPricing(model_id=snapshot.embedding_model)
    query_cost = pricing.project(query_tokens)
    if query_cost > Decimal("1.00"):
        raise ExperimentError("hybrid query embedding projection exceeds the BL-21 budget")
    engine = create_async_engine(experiment_database_url_for_engine(database_url), pool_pre_ping=True)
    index: PrivateHybridIndex | None = None
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as session:
            await _validate_database_snapshot(session, evidence.snapshot_table_counts)
            provenance = await _load_baseline_snapshot(
                session, baseline_run_id=baseline_run_id, channel=channel,
                dataset_sha256=evidence.dataset_sha256,
            )
            index = await _build_private_hybrid_index(
                session, experiment_root=experiment_root, vector_snapshot=snapshot, channel=channel,
            )
            vectors, embedded_tokens = await _embed_experiment_questions(
                evidence.dataset_cases, model_id=snapshot.embedding_model, dimensions=snapshot.dimensions,
                use_instruction=use_instruction,
            )
            if embedded_tokens != query_tokens:
                raise ExperimentError("hybrid query token projection changed during execution")
            samples: list[AbstentionSample] = []
            await _evaluate_hybrid_phase(
                index, development_cases, vectors, HybridMethod.DENSE,
                policy=ExperimentPolicy(), confidence_samples=samples,
            )
            threshold = select_abstention_threshold(samples)
            baseline = PhaseBaseline(
                provenance=provenance,
                development=await _evaluate_hybrid_phase(
                    index, development_cases, vectors, HybridMethod.DENSE,
                    policy=ExperimentPolicy(), abstention_threshold=threshold,
                ),
                holdout=await _evaluate_hybrid_phase(
                    index, holdout_cases, vectors, HybridMethod.DENSE,
                    policy=ExperimentPolicy(), abstention_threshold=threshold,
                ),
            )
            outcomes: list[CandidateOutcome] = []
            thresholds: dict[HybridMethod, float] = {}
            for method in HybridMethod:
                if method == HybridMethod.DENSE:
                    continue
                configuration = _private_hybrid_configuration(method)
                development_samples: list[AbstentionSample] = []
                await _evaluate_hybrid_phase(
                    index, development_cases, vectors, method, policy=ExperimentPolicy(),
                    confidence_samples=development_samples,
                )
                thresholds[method] = select_abstention_threshold(development_samples)
                development = await _evaluate_hybrid_phase(
                    index, development_cases, vectors, method, policy=ExperimentPolicy(),
                    abstention_threshold=thresholds[method],
                )
                outcomes.append(CandidateOutcome(
                    CandidateSpec(f"private_{method.value}", LexicalCandidateMode.TOKEN_ILIKE),
                    CandidateState.RUNNING, PromotionDecision.INSUFFICIENT_EVIDENCE,
                    "development_pending", development, configuration=configuration,
                ))
            selected = max(outcomes, key=lambda outcome: _quality_key(outcome.development.metrics))
            for outcome in outcomes:
                if outcome is not selected:
                    outcome.state = CandidateState.SKIPPED
                    outcome.decision = _vector_development_decision(outcome.development, baseline.development)
                    outcome.decision_reason = "development_not_selected"
                    continue
                method = HybridMethod(str(outcome.configuration["fusion_method"]))
                outcome.holdout = await _evaluate_hybrid_phase(
                    index, holdout_cases, vectors, method, policy=ExperimentPolicy(),
                    abstention_threshold=thresholds[method],
                )
                outcome.state = CandidateState.EVALUATED
                outcome.decision = _vector_holdout_decision(outcome.holdout, baseline.holdout)
                outcome.decision_reason = (
                    "development_selected_holdout_review"
                    if outcome.decision == PromotionDecision.PASSING_FOR_REVIEW
                    else "holdout_quality_regression"
                )
            campaign = _private_hybrid_campaign(evidence, baseline, snapshot, baseline_run_id)
    finally:
        if index is not None:
            index.close()
        await engine.dispose()
    report = _report(campaign, split.reportable(), outcomes, actual_cost_usd=query_cost)
    report_path = write_experiment_report(
        experiment_root, f"{campaign_key}-{campaign.config_sha256[:16]}.json", report,
        launcher_git_metadata=_launcher_git_metadata(),
    )
    return {
        "candidate_count": len(outcomes),
        "dense_no_answer_threshold": threshold,
        "candidate_no_answer_thresholds": {method.value: value for method, value in sorted(thresholds.items(), key=lambda item: item[0].value)},
        "query_instruction": use_instruction,
        "actual_query_embedding_cost_usd": format(query_cost, "f"),
        "report_sha256": hashlib.sha256(report_path.read_bytes()).hexdigest(),
    }


async def execute_rerank_experiment(*, experiment_root: Path, database_url: str, dataset: Path, channel: str, campaign_key: str, baseline_run_id: int) -> dict[str, object]:
    """Compare the approved Cohere reranker on 20/30 already-retrieved posts."""
    evidence = validate_preflight(experiment_root=experiment_root, database_url=database_url, dataset=dataset, channel=channel, campaign_key=campaign_key)
    split, development_cases, holdout_cases = _dataset_split(evidence.dataset_cases)
    snapshot = _load_vector_snapshot(experiment_root)
    settings = (RerankSettings(pool_limit=20), RerankSettings(pool_limit=30))
    rerank_cost = RERANK_PRICE_USD * (len(development_cases) * len(settings) + len(holdout_cases))
    query_tokens = sum(estimate_tokens(_embedding_query(case.question, use_instruction=True)) for case in evidence.dataset_cases)
    embedding_cost = OperatorEmbeddingPricing(model_id=snapshot.embedding_model).project(query_tokens)
    if rerank_cost + embedding_cost > RERANK_BUDGET_USD:
        raise ExperimentError("rerank projection exceeds the BL-21 budget")
    engine = create_async_engine(experiment_database_url_for_engine(database_url), pool_pre_ping=True)
    index: IsolatedExperimentVectorIndex | None = None
    client: OpenRouterClient | None = None
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as session:
            await _validate_database_snapshot(session, evidence.snapshot_table_counts)
            provenance = await _load_baseline_snapshot(session, baseline_run_id=baseline_run_id, channel=channel, dataset_sha256=evidence.dataset_sha256)
            candidate = vector_candidate_config("vector_all")
            identity = vector_identity(experiment_root, candidate, collection_name=snapshot.collection_name)
            if not identity.root.exists(): clone_vector_snapshot(source_root=experiment_root / snapshot.relative_path, identity=identity)
            index = local_experiment_vector_index(identity, dimensions=snapshot.dimensions)
            retriever = await _vector_retriever(session, experiment_root=experiment_root, vector_snapshot=snapshot, channel=channel, candidate=candidate, index=index)
            vectors, embedded_tokens = await _embed_experiment_questions(evidence.dataset_cases, model_id=snapshot.embedding_model, dimensions=snapshot.dimensions, use_instruction=True)
            if embedded_tokens != query_tokens: raise ExperimentError("rerank query token projection changed during execution")
            samples: list[AbstentionSample] = []
            await _evaluate_vector_phase(retriever, development_cases, vectors, embedding_ms=0.0, policy=ExperimentPolicy(), confidence_samples=samples)
            dense_threshold = select_abstention_threshold(samples)
            baseline = PhaseBaseline(provenance=provenance, development=await _evaluate_vector_phase(retriever, development_cases, vectors, embedding_ms=0.0, policy=ExperimentPolicy(), abstention_threshold=dense_threshold), holdout=await _evaluate_vector_phase(retriever, holdout_cases, vectors, embedding_ms=0.0, policy=ExperimentPolicy(), abstention_threshold=dense_threshold))
            api_key = os.environ.get("OPENROUTER_API_KEY", "")
            if not api_key: raise ExperimentError("OPENROUTER_API_KEY is required for reranking")
            client = OpenRouterClient(api_key)
            dev_cache: dict[int, tuple[list[tuple[EvaluationCase, tuple[int, ...], float]], dict[str, list[float]]]] = {}
            thresholds: dict[int, float] = {}
            outcomes: list[CandidateOutcome] = []
            for setting in settings:
                ranked, timings = await _collect_rerank_outcomes(retriever, development_cases, vectors, client, setting)
                dev_cache[setting.pool_limit] = (ranked, timings)
                thresholds[setting.pool_limit] = select_abstention_threshold(AbstentionSample(case.expects_answer, confidence) for case, _ids, confidence in ranked)
                development = _rerank_phase(ranked, timings, ExperimentPolicy(), thresholds[setting.pool_limit])
                outcomes.append(CandidateOutcome(CandidateSpec(f"rerank_{setting.pool_limit}", LexicalCandidateMode.TOKEN_ILIKE), CandidateState.RUNNING, PromotionDecision.INSUFFICIENT_EVIDENCE, "development_pending", development, configuration=setting.configuration()))
            selected = max(outcomes, key=lambda outcome: _quality_key(outcome.development.metrics))
            selected_setting = next(item for item in settings if item.pool_limit == selected.configuration["pool_limit"])
            ranked, timings = await _collect_rerank_outcomes(retriever, holdout_cases, vectors, client, selected_setting)
            for outcome in outcomes:
                if outcome is not selected:
                    outcome.state = CandidateState.SKIPPED; outcome.decision = _vector_development_decision(outcome.development, baseline.development); outcome.decision_reason = "development_not_selected"
                    continue
                outcome.holdout = _rerank_phase(ranked, timings, ExperimentPolicy(), thresholds[selected_setting.pool_limit])
                outcome.state = CandidateState.EVALUATED; outcome.decision = _vector_holdout_decision(outcome.holdout, baseline.holdout); outcome.decision_reason = "development_selected_holdout_review" if outcome.decision == PromotionDecision.PASSING_FOR_REVIEW else "holdout_quality_regression"
            campaign = _rerank_campaign(evidence, baseline, snapshot, baseline_run_id)
    finally:
        if client is not None: await client.close()
        if index is not None: index.close()
        await engine.dispose()
    total_cost = rerank_cost + embedding_cost
    report_path = write_experiment_report(
        experiment_root,
        f"{campaign_key}-{campaign.config_sha256[:16]}.json",
        _report(
            campaign,
            split.reportable(),
            outcomes,
            actual_cost_usd=total_cost,
            budget_limit_usd=RERANK_BUDGET_USD,
        ),
        launcher_git_metadata=_launcher_git_metadata(),
    )
    return {"candidate_count": len(outcomes), "selected_pool": selected_setting.pool_limit, "rerank_cost_usd": format(rerank_cost, "f"), "total_cost_usd": format(total_cost, "f"), "report_sha256": hashlib.sha256(report_path.read_bytes()).hexdigest()}


async def _collect_rerank_outcomes(retriever, cases, vectors, client, setting):
    outcomes = []; timings = {"embedding": [], "vector": [], "rerank": [], "retrieval": []}
    for case in cases:
        encoded = vectors.get(case.id)
        if encoded is None: raise ExperimentError("query embedding cache is incomplete")
        started = time.monotonic(); result = await retriever.retrieve(encoded[:-1], query=case.question)
        candidates = await retriever.canonical_post_candidates(result.parent_post_ids, limit=setting.pool_limit)
        ranked_ids: tuple[int, ...] = (); confidence = 0.0; rerank_ms = 0.0
        if candidates:
            rerank_started = time.monotonic(); ranked = await client.rerank(setting.model, case.question, [item[2] for item in candidates], top_n=min(setting.result_limit, len(candidates)), use_case="bl21_cohere_rerank")
            rerank_ms = (time.monotonic() - rerank_started) * 1000
            ranked_ids = tuple(candidates[item.index][1] for item in ranked); confidence = ranked[0].relevance_score if ranked else 0.0
        timings["embedding"].append(encoded[-1]); timings["vector"].append(result.vector_ms); timings["rerank"].append(rerank_ms); timings["retrieval"].append((time.monotonic() - started) * 1000)
        outcomes.append((case, ranked_ids, confidence))
    return outcomes, timings


def _rerank_phase(outcomes, timings, policy, threshold):
    filtered = [(case, () if should_abstain(confidence, threshold) else ids, confidence) for case, ids, confidence in outcomes]
    categories = {category: _metrics_from_ranked_outcomes([item for item in filtered if item[0].category == category], policy) for category in sorted({case.category for case, _ids, _confidence in filtered})}
    return PhaseResult(_metrics_from_ranked_outcomes(filtered, policy), phase_timing_summary(timings), timings, categories)


def _rerank_campaign(evidence, baseline, snapshot, baseline_run_id):
    baseline_snapshot = baseline.record(); baseline_hash = config_sha256(baseline_snapshot)
    config = {"runner_schema_version": 6, "dataset_sha256": evidence.dataset_sha256, "vector_snapshot_sha256": snapshot.sha256, "baseline_snapshot_sha256": baseline_hash, "candidate_set_sha256": config_sha256([20, 30, RERANK_MODEL])}
    digest = config_sha256(config)
    return SeriesCampaign(digest, evidence.dataset_sha256, config_sha256({"rerank": digest}), CampaignState.COMPLETED, baseline_run_id, baseline_hash, baseline_snapshot)


async def _evaluate_hybrid_phase(
    index: PrivateHybridIndex,
    cases: Sequence[EvaluationCase],
    query_vectors: Mapping[str, list[float]],
    method: HybridMethod,
    *,
    policy: ExperimentPolicy,
    abstention_threshold: float | None = None,
    confidence_samples: list[AbstentionSample] | None = None,
) -> PhaseResult:
    timings = {"embedding": [], "vector": [], "lexical": [], "fusion": [], "retrieval": []}
    outcomes: list[tuple[EvaluationCase, tuple[int, ...], float]] = []
    for case in cases:
        encoded = query_vectors.get(case.id)
        if encoded is None or len(encoded) < 2:
            raise ExperimentError("query embedding cache is incomplete")
        vector, embedding_ms = encoded[:-1], encoded[-1]
        started = time.monotonic()
        result = index.query(vector, case.question, method=method, pool_limit=POOL_LIMIT, result_limit=RESULT_LIMIT)
        elapsed = (time.monotonic() - started) * 1000
        timings["embedding"].append(embedding_ms)
        timings["retrieval"].append(elapsed)
        timings["vector"].append(elapsed if method == HybridMethod.DENSE else 0.0)
        timings["lexical"].append(elapsed if method == HybridMethod.BM25 else 0.0)
        timings["fusion"].append(elapsed if method not in {HybridMethod.DENSE, HybridMethod.BM25} else 0.0)
        if confidence_samples is not None:
            confidence_samples.append(AbstentionSample(case.expects_answer, result.confidence))
        post_ids = result.post_ids
        if abstention_threshold is not None and should_abstain(result.confidence, abstention_threshold):
            post_ids = ()
        outcomes.append((case, post_ids, result.confidence))
    metrics = _metrics_from_ranked_outcomes(outcomes, policy)
    categories = {
        category: _metrics_from_ranked_outcomes(
            [outcome for outcome in outcomes if outcome[0].category == category], policy,
        )
        for category in sorted({case.category for case, _post_ids, _confidence in outcomes})
    }
    return PhaseResult(metrics, phase_timing_summary(timings), timings, categories)


def _private_hybrid_configuration(method: HybridMethod) -> dict[str, object]:
    return {
        "hypothesis_id": f"private_{method.value}",
        "source": "private_parent_hybrid",
        "result_limit": RESULT_LIMIT,
        "pool_limit": POOL_LIMIT,
        "fusion_method": method.value,
    }


def _private_hybrid_campaign(
    evidence: PreflightEvidence, baseline: PhaseBaseline, snapshot: VectorSnapshot, baseline_run_id: int,
) -> SeriesCampaign:
    baseline_snapshot = baseline.record()
    baseline_snapshot_sha256 = config_sha256(baseline_snapshot)
    configuration = {
        "runner_schema_version": 6,
        "dataset_sha256": evidence.dataset_sha256,
        "source_snapshot_sha256": evidence.source_snapshot_sha256,
        "vector_snapshot_sha256": snapshot.sha256,
        "candidate_set_sha256": config_sha256([method.value for method in HybridMethod]),
        "baseline_snapshot_sha256": baseline_snapshot_sha256,
    }
    digest = config_sha256(configuration)
    return SeriesCampaign(
        config_sha256=digest, dataset_sha256=evidence.dataset_sha256,
        resume_key=config_sha256({"series": PRIVATE_HYBRID_KEY, "config_sha256": digest}),
        status=CampaignState.COMPLETED, baseline_run_id=baseline_run_id,
        baseline_snapshot_sha256=baseline_snapshot_sha256, baseline_snapshot=baseline_snapshot,
    )


def _series_campaign(evidence: PreflightEvidence, baseline: PhaseBaseline, vector_snapshot: VectorSnapshot) -> SeriesCampaign:
    baseline_snapshot = baseline.record()
    baseline_snapshot_sha256 = config_sha256(baseline_snapshot)
    configuration = {
        "runner_schema_version": 5,
        "dataset_sha256": evidence.dataset_sha256,
        "source_snapshot_sha256": evidence.source_snapshot_sha256,
        "vector_snapshot_sha256": vector_snapshot.sha256,
        "candidate_set_sha256": config_sha256(list(VECTOR_SERIES_CHALLENGERS)),
        "baseline_snapshot_sha256": baseline_snapshot_sha256,
    }
    config_digest = config_sha256(configuration)
    return SeriesCampaign(
        config_sha256=config_digest,
        dataset_sha256=evidence.dataset_sha256,
        resume_key=config_sha256({"series": VECTOR_SERIES_KEY, "config_sha256": config_digest}),
        status=CampaignState.RUNNING,
        baseline_run_id=baseline.provenance.run_id,
        baseline_snapshot_sha256=baseline_snapshot_sha256,
        baseline_snapshot=baseline_snapshot,
    )


def _series_report_path(experiment_root: Path) -> Path:
    return experiment_root / ".data-experiment" / "experiments" / f"{VECTOR_SERIES_KEY}.json"


def _write_series_report(
    experiment_root: Path,
    split: dict[str, object],
    campaign: SeriesCampaign,
    outcomes: Sequence[CandidateOutcome],
    *,
    actual_cost_usd: Decimal,
    state: CampaignState,
) -> None:
    campaign.status = state
    report = _report(campaign, split, outcomes, actual_cost_usd=actual_cost_usd)
    write_experiment_report(experiment_root, f"{VECTOR_SERIES_KEY}.json", report, launcher_git_metadata=_launcher_git_metadata())


async def _embed_experiment_questions(
    cases: Sequence[EvaluationCase],
    *,
    model_id: str,
    dimensions: int,
    use_instruction: bool = False,
) -> tuple[dict[str, list[float]], int]:
    api_key = os.environ.get("OPENROUTER_API_KEY", "")
    if not api_key:
        raise ExperimentError("OPENROUTER_API_KEY is required for vector execution")
    questions = [_embedding_query(case.question, use_instruction=use_instruction) for case in cases]
    if len(questions) != len(set(case.id for case in cases)):
        raise ExperimentError("evaluation case identifiers must be unique")
    client = OpenRouterClient(api_key)
    started = time.monotonic()
    try:
        vectors = await client.embeddings(model_id, questions, use_case="bl21_query_embedding")
    finally:
        await client.close()
    elapsed_ms = (time.monotonic() - started) * 1000
    if len(vectors) != len(cases) or any(
        len(vector) != dimensions or any(not math.isfinite(value) for value in vector)
        for vector in vectors
    ):
        raise ExperimentError("query embeddings do not match the snapshot dimensions")
    # The one batch is amortized only in memory; no question or vector is persisted.
    per_case_ms = elapsed_ms / len(cases)
    return {case.id: [*vector, per_case_ms] for case, vector in zip(cases, vectors, strict=True)}, sum(estimate_tokens(question) for question in questions)


def _embedding_query(question: str, *, use_instruction: bool) -> str:
    """Build the fixed Qwen query form without modifying indexed post text."""
    if not question.strip():
        raise ExperimentError("evaluation question must not be empty")
    if not use_instruction:
        return question
    return f"Instruct: {QWEN_QUERY_INSTRUCTION}\nQuery: {question}"


async def _vector_retriever(
    session: AsyncSession,
    *,
    experiment_root: Path,
    vector_snapshot: VectorSnapshot,
    channel: str,
    candidate: VectorCandidateConfig,
    index: IsolatedExperimentVectorIndex | None = None,
) -> ExperimentRepresentationRetriever:
    if index is None:
        identity = vector_identity(experiment_root, candidate, collection_name=vector_snapshot.collection_name)
        if not identity.root.exists():
            clone_vector_snapshot(source_root=experiment_root / vector_snapshot.relative_path, identity=identity)
        index = local_experiment_vector_index(identity, dimensions=vector_snapshot.dimensions)
    index.require_snapshot_collection()
    retriever = ExperimentRepresentationRetriever(
        session,
        index,
        channel_username=channel,
        candidate=candidate,
        required_index_version=vector_snapshot.index_version,
    )
    _channel_id, index_version = await retriever.resolve_channel()
    if index_version != vector_snapshot.index_version:
        raise ExperimentError("isolated database index version does not match vector snapshot")
    return retriever


async def _build_private_hybrid_index(
    session: AsyncSession,
    *,
    experiment_root: Path,
    vector_snapshot: VectorSnapshot,
    channel: str,
) -> PrivateHybridIndex:
    """Materialise one parent-level hybrid collection below .data-experiment only."""
    candidate = vector_candidate_config("vector_all")
    identity = vector_identity(experiment_root, candidate, collection_name=vector_snapshot.collection_name)
    if not identity.root.exists():
        clone_vector_snapshot(source_root=experiment_root / vector_snapshot.relative_path, identity=identity)
    dense_by_parent = parent_dense_vectors_from_snapshot(
        identity.root,
        collection_name=vector_snapshot.collection_name,
        dimensions=vector_snapshot.dimensions,
    )
    channel_id = (await session.execute(
        select(KnowledgeChannel.channel_id)
        .join(Channel, Channel.id == KnowledgeChannel.channel_id)
        .where(Channel.username == channel.lstrip("@"), KnowledgeChannel.state == KnowledgeChannelState.READY)
    )).scalar_one_or_none()
    if not isinstance(channel_id, int):
        raise ExperimentError("approved hybrid channel was not found")
    rows = (await session.execute(
        select(Post.id, Post.post_id, Post.content)
        .where(Post.channel_id == channel_id)
        .order_by(Post.id)
    )).all()
    posts = [
        HybridPost(int(telegram_id), dense_by_parent[int(parent_id)], str(content))
        for parent_id, telegram_id, content in rows
        if int(parent_id) in dense_by_parent and isinstance(content, str) and content.strip()
    ]
    if len(posts) < RESULT_LIMIT:
        raise ExperimentError("hybrid source has insufficient parent vectors")
    hybrid_root = experiment_root / ".data-experiment" / "hybrid" / config_sha256({
        "dataset": "canonical_post_content",
        "vector_snapshot": vector_snapshot.sha256,
        "channel": hash_identifier(channel.lstrip("@").lower()),
    })
    if hybrid_root.exists() and (hybrid_root.is_symlink() or not hybrid_root.is_dir()):
        raise ExperimentError("private hybrid root is unsafe")
    index = PrivateHybridIndex(
        hybrid_root,
        dimensions=vector_snapshot.dimensions,
        identity={"snapshot": vector_snapshot.sha256, "channel": hash_identifier(channel.lstrip("@").lower())},
    )
    index.build(posts)
    return index


async def _evaluate_vector_phase(
    retriever: ExperimentRepresentationRetriever,
    cases: Sequence[EvaluationCase],
    query_vectors: Mapping[str, list[float]],
    *,
    embedding_ms: float,
    policy: ExperimentPolicy,
    abstention_threshold: float | None = None,
    confidence_samples: list[AbstentionSample] | None = None,
) -> PhaseResult:
    recalls: list[float] = []
    mrrs: list[float] = []
    ndcgs: list[float] = []
    duplicate_shares: list[float] = []
    diversities: list[float] = []
    insufficient_count = 0
    timings = {"embedding": [], "vector": [], "lexical": [], "fusion": [], "retrieval": []}
    outcomes: list[tuple[EvaluationCase, tuple[int, ...], float]] = []
    for case in cases:
        encoded = query_vectors.get(case.id)
        if encoded is None or len(encoded) < 2:
            raise ExperimentError("query embedding cache is incomplete")
        vector, cached_embedding_ms = encoded[:-1], encoded[-1]
        started = time.monotonic()
        result = await retriever.retrieve(vector, query=case.question)
        timings["retrieval"].append((time.monotonic() - started) * 1000)
        timings["embedding"].append(cached_embedding_ms + embedding_ms)
        timings["vector"].append(result.vector_ms)
        timings["lexical"].append(result.lexical_ms)
        timings["fusion"].append(result.fusion_ms)
        if confidence_samples is not None:
            confidence_samples.append(AbstentionSample(case.expects_answer, result.confidence))
        post_ids = result.telegram_post_ids
        if abstention_threshold is not None and should_abstain(result.confidence, abstention_threshold):
            post_ids = ()
        outcomes.append((case, post_ids, result.confidence))
    aggregate = _metrics_from_ranked_outcomes(outcomes, policy)
    categories = {
        category: _metrics_from_ranked_outcomes(
            [outcome for outcome in outcomes if outcome[0].category == category], policy,
        )
        for category in sorted({case.category for case, _post_ids, _confidence in outcomes})
    }
    return PhaseResult(aggregate, phase_timing_summary(timings), timings, categories)


def _metrics_from_ranked_outcomes(
    outcomes: Sequence[tuple[EvaluationCase, tuple[int, ...], float]],
    policy: ExperimentPolicy,
) -> EvaluationMetrics:
    """Aggregate answerable retrieval and no-answer behaviour without retaining text."""
    recalls: list[float] = []
    mrrs: list[float] = []
    ndcgs: list[float] = []
    duplicate_shares: list[float] = []
    diversities: list[float] = []
    insufficient_count = 0
    no_answer_case_count = 0
    correct_no_answer_count = 0
    false_no_answer_count = 0
    for case, post_ids, _confidence in outcomes:
        if not case.expects_answer:
            no_answer_case_count += 1
            if post_ids:
                false_no_answer_count += 1
            else:
                correct_no_answer_count += 1
            continue
        metrics = retrieval_metrics(case.expected_telegram_post_ids, post_ids, limit=RESULT_LIMIT)
        recalls.append(metrics.recall_at_k)
        mrrs.append(metrics.mrr)
        ndcgs.append(metrics.ndcg)
        duplicate_shares.append(duplicate_share(post_ids))
        diversities.append(source_diversity(post_ids))
        insufficient_count += int(len(post_ids) < policy.min_sources_per_case)
    return EvaluationMetrics(
        case_count=len(recalls),
        retrieval=RetrievalMetrics(_mean(recalls), _mean(mrrs), _mean(ndcgs)),
        duplicate_source_share=_mean(duplicate_shares),
        source_diversity=_mean(diversities),
        insufficient_evidence_count=insufficient_count,
        no_answer_case_count=no_answer_case_count,
        correct_no_answer_count=correct_no_answer_count,
        false_no_answer_count=false_no_answer_count,
    )


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


async def _run_vector_campaign(
    session: AsyncSession,
    *,
    evidence: PreflightEvidence,
    channel: str,
    development_cases: Sequence[EvaluationCase],
    holdout_cases: Sequence[EvaluationCase],
    baseline: PhaseBaseline,
    vector_snapshot: VectorSnapshot,
    query_vectors: Mapping[str, list[float]],
    query_tokens: int,
    experiment_root: Path,
    challenger_ids: tuple[str, ...],
) -> tuple[list[CandidateOutcome], object]:
    repo = ExperimentRepository(session)
    policy = ExperimentPolicy(allow_automatic_promotion=False)
    baseline_record = baseline.record()
    baseline_snapshot_sha256 = config_sha256(baseline_record)
    configuration = _vector_campaign_configuration(
        channel,
        evidence.dataset_sha256,
        evidence.source_snapshot_sha256,
        baseline_snapshot_sha256,
        vector_snapshot,
        challenger_ids,
    )
    campaign = await repo.create_or_get_campaign(
        campaign_key=evidence.campaign_key,
        channel_sha256=evidence.channel_sha256,
        dataset_sha256=evidence.dataset_sha256,
        source_snapshot_sha256=evidence.source_snapshot_sha256,
        source_snapshot_table_count=evidence.source_snapshot_table_count,
        baseline_run_id=baseline.provenance.run_id,
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
        raise ExperimentError("campaign is not available for vector execution")
    await repo.transition_campaign(campaign, CampaignState.RUNNING)

    pricing = OperatorEmbeddingPricing(model_id=vector_snapshot.embedding_model)
    projected_cost = pricing.project(query_tokens)
    claimed: dict[str, object] = {}
    outcomes: list[CandidateOutcome] = []
    for candidate_id in challenger_ids:
        spec = vector_candidate_config(candidate_id)
        candidate_configuration = spec.configuration(pricing, token_total=query_tokens)
        candidate = await repo.claim_candidate(
            campaign,
            hypothesis_id=spec.hypothesis_id,
            configuration=candidate_configuration,
            index_label="snapshot_qdrant",
            projected_cost_usd=projected_cost,
            embedding_model_id=pricing.model_id,
            embedding_pricing_version=pricing.version,
            embedding_pricing_source=pricing.source,
            embedding_input_tokens=query_tokens,
        )
        if candidate is None:
            raise ExperimentError("existing candidate prevents a repeat vector execution")
        claimed[spec.hypothesis_id] = candidate
        try:
            retriever = await _vector_retriever(
                session,
                experiment_root=experiment_root,
                vector_snapshot=vector_snapshot,
                channel=channel,
                candidate=spec,
            )
            development = await _evaluate_vector_phase(retriever, development_cases, query_vectors, embedding_ms=0.0, policy=policy)
        except ExperimentError as exc:
            await repo.fail_candidate(candidate, _failure_code(exc))
            outcomes.append(CandidateOutcome(spec, CandidateState.FAILED, PromotionDecision.FAILING, _failure_code(exc), None, configuration=candidate_configuration))
        else:
            outcomes.append(CandidateOutcome(spec, CandidateState.RUNNING, PromotionDecision.INSUFFICIENT_EVIDENCE, "development_pending", development, configuration=candidate_configuration))

    selected = _select_vector_on_development(outcomes, baseline)
    selected_id = selected.spec.hypothesis_id if selected is not None else None
    for outcome in outcomes:
        candidate = claimed[outcome.spec.hypothesis_id]
        if outcome.development is None:
            continue
        if outcome.spec.hypothesis_id != selected_id:
            outcome.state = CandidateState.SKIPPED
            outcome.decision = _vector_development_decision(outcome.development, baseline.development)
            outcome.decision_reason = "development_not_selected" if outcome.decision == PromotionDecision.PASSING_FOR_REVIEW else "development_quality_regression"
            await repo.skip_candidate(
                candidate,
                dev_metrics=outcome.development.metrics,
                phase_timings_ms=_prefixed_timings("development", outcome.development.raw_timings),
                decision=outcome.decision,
                decision_reason=outcome.decision_reason,
            )
            continue
        try:
            retriever = await _vector_retriever(
                session,
                experiment_root=experiment_root,
                vector_snapshot=vector_snapshot,
                channel=channel,
                candidate=outcome.spec,
            )
            outcome.holdout = await _evaluate_vector_phase(retriever, holdout_cases, query_vectors, embedding_ms=0.0, policy=policy)
        except ExperimentError as exc:
            outcome.state = CandidateState.FAILED
            outcome.decision = PromotionDecision.FAILING
            outcome.decision_reason = _failure_code(exc)
            await repo.fail_candidate(candidate, outcome.decision_reason)
            continue
        outcome.state = CandidateState.EVALUATED
        outcome.decision = _vector_holdout_decision(outcome.holdout, baseline.holdout)
        outcome.decision_reason = "development_selected_holdout_review" if outcome.decision == PromotionDecision.PASSING_FOR_REVIEW else "holdout_quality_regression"
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


def _select_vector_on_development(outcomes: Sequence[CandidateOutcome], baseline: PhaseBaseline) -> CandidateOutcome | None:
    eligible = [
        outcome for outcome in outcomes
        if outcome.development is not None
        and _vector_development_decision(outcome.development, baseline.development) == PromotionDecision.PASSING_FOR_REVIEW
    ]
    return max(eligible, key=lambda outcome: _quality_key(outcome.development.metrics)) if eligible else None


def _vector_development_decision(candidate: PhaseResult, baseline: PhaseResult) -> PromotionDecision:
    return PromotionDecision.PASSING_FOR_REVIEW if _metrics_no_regression(candidate.metrics, baseline.metrics) else PromotionDecision.FAILING


def _vector_holdout_decision(candidate: PhaseResult, baseline: PhaseResult) -> PromotionDecision:
    """Apply BL-21's documented relative holdout gate, never auto-promoting."""
    candidate_metrics = candidate.metrics.retrieval
    baseline_metrics = baseline.metrics.retrieval
    passes = (
        candidate_metrics.recall_at_k >= baseline_metrics.recall_at_k + 0.05
        and candidate_metrics.ndcg >= baseline_metrics.ndcg + 0.05
        and candidate_metrics.mrr >= baseline_metrics.mrr - 0.02
        and candidate.metrics.duplicate_source_share <= baseline.metrics.duplicate_source_share
        and candidate.metrics.false_no_answer_count <= baseline.metrics.false_no_answer_count
    )
    return PromotionDecision.PASSING_FOR_REVIEW if passes else PromotionDecision.FAILING


async def _has_sufficient_representation_coverage(
    session: AsyncSession,
    *,
    channel: str,
    index_version: int,
    candidate: VectorCandidateConfig,
) -> bool:
    """Reject an ablation before quality scoring when its snapshot lacks data."""
    if candidate.representations.value == "all":
        return True
    statement = (
        select(func.count(KnowledgeRepresentation.id))
        .join(Post, Post.id == KnowledgeRepresentation.post_id)
        .join(Channel, Channel.id == Post.channel_id)
        .where(
            Channel.username == channel.lstrip("@").lower(),
            KnowledgeRepresentation.index_version == index_version,
            KnowledgeRepresentation.index_status == IndexStatus.INDEXED,
            KnowledgeRepresentation.representation_type == RepresentationType(candidate.representations.value),
        )
    )
    count = await session.scalar(statement)
    return isinstance(count, int) and count >= max(MINIMUM_REPRESENTATION_POINTS, candidate.pool_limit)


def _metrics_no_regression(candidate: EvaluationMetrics, baseline: EvaluationMetrics) -> bool:
    return (
        candidate.retrieval.recall_at_k >= baseline.retrieval.recall_at_k
        and candidate.retrieval.mrr >= baseline.retrieval.mrr
        and candidate.retrieval.ndcg >= baseline.retrieval.ndcg
        and candidate.duplicate_source_share <= baseline.duplicate_source_share
        and candidate.false_no_answer_count <= baseline.false_no_answer_count
    )


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
    no_answer_case_count = 0
    correct_no_answer_count = 0
    false_no_answer_count = 0
    retrieval_timings: list[float] = []
    lexical_timings: list[float] = []
    for case in cases:
        started = time.monotonic()
        result = await retriever.retrieve(mode=mode, query=case.question)
        retrieval_timings.append((time.monotonic() - started) * 1000)
        lexical_timings.append(result.lexical_ms)
        telegram_post_ids = await retriever.canonical_telegram_post_ids(result.parent_post_ids)
        if not case.expects_answer:
            no_answer_case_count += 1
            if telegram_post_ids:
                false_no_answer_count += 1
            else:
                correct_no_answer_count += 1
            continue
        metrics = retrieval_metrics(case.expected_telegram_post_ids, telegram_post_ids, limit=RESULT_LIMIT)
        recalls.append(metrics.recall_at_k)
        mrrs.append(metrics.mrr)
        ndcgs.append(metrics.ndcg)
        duplicate_shares.append(duplicate_share(telegram_post_ids))
        diversities.append(source_diversity(telegram_post_ids))
        insufficient_count += int(len(telegram_post_ids) < policy.min_sources_per_case)
    aggregate = EvaluationMetrics(
        case_count=len(recalls),
        retrieval=RetrievalMetrics(_mean(recalls), _mean(mrrs), _mean(ndcgs)),
        duplicate_source_share=_mean(duplicate_shares),
        source_diversity=_mean(diversities),
        insufficient_evidence_count=insufficient_count,
        no_answer_case_count=no_answer_case_count,
        correct_no_answer_count=correct_no_answer_count,
        false_no_answer_count=false_no_answer_count,
    )
    raw_timings = {"retrieval": retrieval_timings, "lexical": lexical_timings}
    return PhaseResult(aggregate, phase_timing_summary(raw_timings), raw_timings)


def _mean(values: Sequence[float]) -> float:
    return sum(values) / len(values) if values else 0.0


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
    """Compare immutable source rows, excluding runner-produced evidence rows."""
    for table_name, expected_count in table_counts:
        if table_name in _EXPERIMENT_MUTABLE_TABLES:
            continue
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


def _report(
    campaign: object,
    split: dict[str, object],
    outcomes: Sequence[CandidateOutcome],
    *,
    actual_cost_usd: Decimal = Decimal("0"),
    budget_limit_usd: Decimal = Decimal("1.00"),
) -> dict[str, object]:
    baseline_snapshot = getattr(campaign, "baseline_snapshot")
    baseline_snapshot_sha256 = getattr(campaign, "baseline_snapshot_sha256")
    return {
        "schema_version": 6,
        "campaign": {
            "config_sha256": getattr(campaign, "config_sha256"),
            "dataset_sha256": getattr(campaign, "dataset_sha256"),
            "resume_key": getattr(campaign, "resume_key"),
            "state": getattr(campaign, "status").value,
            "split": split,
            "budget": {"limit_usd": format(budget_limit_usd, "f"), "reserved_usd": "0", "actual_usd": format(actual_cost_usd, "f")},
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
        "candidate_key": config_sha256(_outcome_configuration(outcome)),
        "state": outcome.state.value,
        "decision": outcome.decision.value,
        "decision_reason": outcome.decision_reason,
        "configuration": _outcome_configuration(outcome),
        "development": _phase_report(outcome.development),
        "holdout": _phase_report(outcome.holdout),
        "comparison": {
            "baseline_snapshot_sha256": baseline_snapshot_sha256,
            "quality_deltas": {
                "development": _quality_deltas(outcome.development, baseline_snapshot, phase_name="development"),
                "holdout": _quality_deltas(outcome.holdout, baseline_snapshot, phase_name="holdout"),
            },
            "latency": {"status": "phase_percentiles_available" if "phases" in baseline_snapshot else "baseline_phase_percentiles_unavailable"},
        },
    }


def _phase_report(result: PhaseResult | None) -> dict[str, object] | None:
    if result is None:
        return None
    return {
        "metrics": evaluation_metrics_record(result.metrics),
        "timings": result.timings,
        "categories": {
            category: evaluation_metrics_record(metrics)
            for category, metrics in sorted(result.category_metrics.items())
        },
    }


def _quality_deltas(
    result: PhaseResult | None,
    baseline_snapshot: Mapping[str, object],
    *,
    phase_name: str,
) -> dict[str, float] | None:
    if result is None:
        return None
    if "phases" in baseline_snapshot:
        phases = _mapping(baseline_snapshot["phases"], "baseline snapshot phases")
        phase = _mapping(phases[phase_name], f"baseline {phase_name} phase")
        baseline_metrics = _mapping(phase["metrics"], f"baseline {phase_name} metrics")
    else:
        baseline_metrics = _mapping(baseline_snapshot["metrics"], "baseline snapshot metrics")
    return {
        "recall_at_k": result.metrics.retrieval.recall_at_k - float(baseline_metrics["recall_at_k"]),
        "mrr": result.metrics.retrieval.mrr - float(baseline_metrics["mrr"]),
        "ndcg": result.metrics.retrieval.ndcg - float(baseline_metrics["ndcg"]),
        "duplicate_source_share": result.metrics.duplicate_source_share - float(baseline_metrics["duplicate_source_share"]),
    }


def _outcome_configuration(outcome: CandidateOutcome) -> Mapping[str, object]:
    if outcome.configuration is not None:
        return outcome.configuration
    if isinstance(outcome.spec, CandidateSpec):
        return outcome.spec.configuration()
    raise ExperimentError("vector candidate outcome is missing immutable configuration")


def _load_vector_snapshot(experiment_root: Path) -> VectorSnapshot:
    manifest, _manifest_hash = _load_manifest(experiment_root)
    raw = _mapping(manifest.get("vector_snapshot"), "vector_snapshot")
    relative_path = raw.get("path")
    if not isinstance(relative_path, str) or not relative_path.startswith(".data-experiment/snapshots/"):
        raise ExperimentError("vector snapshot path is invalid")
    path = Path(relative_path)
    if path.is_absolute() or ".." in path.parts:
        raise ExperimentError("vector snapshot path is invalid")
    snapshot_root = experiment_root / path
    if snapshot_root.is_symlink() or not snapshot_root.is_dir():
        raise ExperimentError("vector snapshot is unavailable")
    collection_name = raw.get("collection_name")
    embedding_model = raw.get("embedding_model")
    dimensions = raw.get("dimensions")
    index_version = raw.get("index_version")
    if (
        not isinstance(collection_name, str)
        or not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{2,127}", collection_name)
        or not isinstance(embedding_model, str)
        or embedding_model != DEFAULT_EMBEDDING_MODEL
        or not isinstance(dimensions, int)
        or isinstance(dimensions, bool)
        or dimensions < 1
        or not isinstance(index_version, int)
        or isinstance(index_version, bool)
        or index_version < 1
    ):
        raise ExperimentError("vector snapshot metadata is invalid")
    return VectorSnapshot(
        relative_path=path,
        sha256=require_sha256(raw.get("sha256"), "vector_snapshot.sha256"),
        collection_name=collection_name,
        embedding_model=embedding_model,
        dimensions=dimensions,
        index_version=index_version,
    )


def _vector_campaign_configuration(
    channel: str,
    dataset_sha256: str,
    snapshot_sha256: str,
    baseline_snapshot_sha256: str,
    vector_snapshot: VectorSnapshot,
    challenger_ids: Sequence[str],
) -> dict[str, object]:
    if not challenger_ids:
        raise ExperimentError("vector experiment cycle must contain a challenger")
    return {
        "runner_schema_version": 4,
        "channel_sha256": hash_identifier(channel.strip().lower()),
        "dataset_sha256": dataset_sha256,
        "source_snapshot_sha256": snapshot_sha256,
        "vector_snapshot_sha256": vector_snapshot.sha256,
        "vector_index_version": vector_snapshot.index_version,
        "candidate_set_sha256": config_sha256([vector_candidate_config(candidate_id).hypothesis_id for candidate_id in challenger_ids]),
        "baseline_snapshot_sha256": require_sha256(baseline_snapshot_sha256, "baseline_snapshot_sha256"),
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
    if root.get("schema_version") not in {1, 2}:
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
    mode.add_argument("--execute-vector-series", action="store_true")
    mode.add_argument("--create-vector-baseline", action="store_true")
    mode.add_argument("--build-hybrid", action="store_true")
    mode.add_argument("--execute-private-hybrid", action="store_true")
    mode.add_argument("--execute-rerank", action="store_true")
    parser.add_argument("--vector-candidate")
    parser.add_argument("--query-instruction", action="store_true")
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
    if args.query_instruction and not args.create_vector_baseline:
        raise ExperimentError("--query-instruction requires --create-vector-baseline")
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
    if args.execute and (args.baseline_run_id is None or args.baseline_run_id < 1):
        raise ExperimentError("--execute requires --baseline-run-id from the isolated experiment database")
    if args.create_vector_baseline:
        if args.baseline_run_id is not None:
            raise ExperimentError("--create-vector-baseline does not accept --baseline-run-id")
        result = asyncio.run(create_vector_baseline(
            experiment_root=args.experiment_root,
            database_url=args.database_url,
            dataset=args.dataset,
            channel=args.channel,
            campaign_key=args.campaign_id,
            use_instruction=args.query_instruction,
        ))
        print(json.dumps(result, sort_keys=True), flush=True)
        return 0
    if args.build_hybrid:
        if args.baseline_run_id is not None:
            raise ExperimentError("--build-hybrid does not accept --baseline-run-id")
        result = asyncio.run(build_hybrid_collection(
            experiment_root=args.experiment_root,
            database_url=args.database_url,
            dataset=args.dataset,
            channel=args.channel,
            campaign_key=args.campaign_id,
        ))
        print(json.dumps(result, sort_keys=True), flush=True)
        return 0
    if args.execute_private_hybrid:
        if args.baseline_run_id is None or args.baseline_run_id < 1:
            raise ExperimentError("--execute-private-hybrid requires --baseline-run-id from the isolated experiment database")
        result = asyncio.run(execute_private_hybrid_comparisons(
            experiment_root=args.experiment_root,
            database_url=args.database_url,
            dataset=args.dataset,
            channel=args.channel,
            campaign_key=args.campaign_id,
            baseline_run_id=args.baseline_run_id,
        ))
        print(json.dumps(result, sort_keys=True), flush=True)
        return 0
    if args.execute_rerank:
        if args.baseline_run_id is None or args.baseline_run_id < 1:
            raise ExperimentError("--execute-rerank requires --baseline-run-id from the isolated experiment database")
        result = asyncio.run(execute_rerank_experiment(experiment_root=args.experiment_root, database_url=args.database_url, dataset=args.dataset, channel=args.channel, campaign_key=args.campaign_id, baseline_run_id=args.baseline_run_id))
        print(json.dumps(result, sort_keys=True), flush=True)
        return 0
    if args.execute_vector:
        if args.baseline_run_id is None or args.baseline_run_id < 1:
            raise ExperimentError("--execute-vector requires --baseline-run-id from the isolated experiment database")
        if args.vector_candidate is None:
            raise ExperimentError("--execute-vector requires one allowlisted --vector-candidate")
        vector_candidate_config(args.vector_candidate)
        vector_experiment_cycle(args.vector_candidate)
        result = asyncio.run(execute_vector_experiment(
            experiment_root=args.experiment_root,
            database_url=args.database_url,
            dataset=args.dataset,
            channel=args.channel,
            campaign_key=args.campaign_id,
            baseline_run_id=args.baseline_run_id,
            vector_candidate=args.vector_candidate,
        ))
        print(json.dumps(result, sort_keys=True), flush=True)
        return 0
    if args.execute_vector_series:
        if args.baseline_run_id is None or args.baseline_run_id < 1:
            raise ExperimentError("--execute-vector-series requires --baseline-run-id from the isolated experiment database")
        result = asyncio.run(execute_vector_series(
            experiment_root=args.experiment_root,
            database_url=args.database_url,
            dataset=args.dataset,
            channel=args.channel,
            baseline_run_id=args.baseline_run_id,
        ))
        print(json.dumps(result, sort_keys=True), flush=True)
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
