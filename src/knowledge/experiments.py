"""Pure, content-free primitives for isolated retrieval-quality experiments."""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import subprocess
import tempfile
from dataclasses import asdict, dataclass, is_dataclass, replace
from decimal import Decimal, InvalidOperation
from enum import Enum
from pathlib import Path
from typing import Iterable, Mapping


FEATURE_BRANCH = "feature/bl-21-rag-quality-experiments"
RUNNER_GIT_BRANCH_ENV = "BL21_EXPERIMENT_GIT_BRANCH"
RUNNER_GIT_REVISION_ENV = "BL21_EXPERIMENT_GIT_REVISION"
SOURCE_WORKTREE = Path("/opt/telegram-parser-bot")
SOURCE_KNOWLEDGE_INDEX = SOURCE_WORKTREE / ".data" / "knowledge"
REPORT_SCHEMA_VERSION = 3
_HASH_LENGTH = 64
_GIT_REVISION = re.compile(r"[0-9a-f]{40}\Z")
_PROHIBITED_CONTENT_KEYS = frozenset({
    "answer", "answers", "body", "bodies", "chunk", "chunks", "content",
    "context", "document", "documents", "message", "messages", "post", "posts",
    "prompt", "prompts", "question", "questions", "query", "queries", "text",
    "apikey", "authorization", "credential", "credentials", "databaseurl", "dsn",
    "endpoint", "password", "secret", "token", "uri", "url",
})
_SAFE_REPORT_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*\.json\Z")
_SAFE_PHASE_NAME = re.compile(r"[a-z][a-z0-9_]*\Z")
_METRIC_JSON_FIELDS = frozenset({"dev_metrics", "holdout_metrics"})
_METRIC_JSON_KEYS = {
    "case_count", "recall_at_k", "mrr", "ndcg", "duplicate_source_share",
    "source_diversity", "insufficient_evidence_count",
}
_PERCENTILE_JSON_KEYS = {"count", "p50_ms", "p95_ms", "p99_ms"}


class CampaignState(str, Enum):
    DRAFT = "draft"
    READY = "ready"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class CandidateState(str, Enum):
    PLANNED = "planned"
    RUNNING = "running"
    EVALUATED = "evaluated"
    FAILED = "failed"
    SKIPPED = "skipped"


class PromotionDecision(str, Enum):
    INSUFFICIENT_EVIDENCE = "insufficient_evidence"
    FAILING = "failing"
    PASSING_FOR_REVIEW = "passing_for_review"
    PROMOTED = "promoted"


class ExperimentError(ValueError):
    """Base error for a rejected experiment operation."""


class StateTransitionError(ExperimentError):
    """Raised when a state cannot move to the requested next state."""


class BudgetExceeded(ExperimentError):
    """Raised before an operation could exceed the fixed experiment budget."""


class UnsafeExperimentPath(ExperimentError):
    """Raised when report output is not contained in the isolated experiment root."""


@dataclass(frozen=True, slots=True)
class GitIdentity:
    """The feature branch and immutable commit that identify one experiment clone."""

    branch: str
    revision: str


@dataclass(frozen=True, slots=True)
class ExperimentPolicy:
    """Auditable experiment limits and promotion thresholds, without retrieval content."""

    budget_usd: Decimal = Decimal("1.00")
    holdout_fraction: Decimal = Decimal("0.30")
    min_evaluation_cases: int = 1
    min_recall_at_k: float = 0.80
    min_mrr: float = 0.70
    min_ndcg: float = 0.75
    max_duplicate_source_share: float = 0.20
    min_source_diversity: float = 0.50
    min_sources_per_case: int = 1
    allow_automatic_promotion: bool = False

    def __post_init__(self) -> None:
        budget = _money(self.budget_usd)
        holdout = _decimal_fraction(self.holdout_fraction, "holdout_fraction")
        if budget <= 0:
            raise ExperimentError("budget_usd must be positive")
        if not 0 < holdout < 1:
            raise ExperimentError("holdout_fraction must be between zero and one")
        if self.min_evaluation_cases < 1 or self.min_sources_per_case < 1:
            raise ExperimentError("minimum counts must be positive")
        for name, value in (
            ("min_recall_at_k", self.min_recall_at_k),
            ("min_mrr", self.min_mrr),
            ("min_ndcg", self.min_ndcg),
            ("max_duplicate_source_share", self.max_duplicate_source_share),
            ("min_source_diversity", self.min_source_diversity),
        ):
            if not isinstance(value, (int, float)) or isinstance(value, bool) or not 0 <= value <= 1:
                raise ExperimentError(f"{name} must be a finite fraction")
        object.__setattr__(self, "budget_usd", budget)
        object.__setattr__(self, "holdout_fraction", holdout)


@dataclass(frozen=True, slots=True)
class DatasetSplit:
    """The raw IDs remain process-local; ``reportable`` exposes hashes and counts only."""

    train_ids: frozenset[str]
    holdout_ids: frozenset[str]

    def reportable(self) -> dict[str, object]:
        return {
            "train_count": len(self.train_ids),
            "holdout_count": len(self.holdout_ids),
            "train_id_hashes": sorted(hash_identifier(value) for value in self.train_ids),
            "holdout_id_hashes": sorted(hash_identifier(value) for value in self.holdout_ids),
        }


@dataclass(frozen=True, slots=True)
class Campaign:
    campaign_id: str
    config_sha256: str
    dataset_sha256: str
    resume_key: str
    state: CampaignState = CampaignState.DRAFT

    def __post_init__(self) -> None:
        if not self.campaign_id:
            raise ExperimentError("campaign_id must not be empty")
        _require_sha256(self.config_sha256, "config_sha256")
        _require_sha256(self.dataset_sha256, "dataset_sha256")
        _require_sha256(self.resume_key, "resume_key")
        if not isinstance(self.state, CampaignState):
            raise ExperimentError("campaign state must be a CampaignState")


@dataclass(frozen=True, slots=True)
class Candidate:
    candidate_key: str
    campaign_resume_key: str
    state: CandidateState = CandidateState.PLANNED

    def __post_init__(self) -> None:
        _require_sha256(self.candidate_key, "candidate_key")
        _require_sha256(self.campaign_resume_key, "campaign_resume_key")
        if not isinstance(self.state, CandidateState):
            raise ExperimentError("candidate state must be a CandidateState")


@dataclass(frozen=True, slots=True)
class BudgetReservation:
    key: str
    projected_usd: Decimal


@dataclass(frozen=True, slots=True)
class RetrievalMetrics:
    recall_at_k: float
    mrr: float
    ndcg: float


@dataclass(frozen=True, slots=True)
class EvaluationMetrics:
    case_count: int
    retrieval: RetrievalMetrics
    duplicate_source_share: float
    source_diversity: float
    insufficient_evidence_count: int


def evaluation_metrics_record(metrics: EvaluationMetrics) -> dict[str, float | int]:
    """Return the closed, content-free aggregate metric shape used by candidates."""
    validate_evaluation_metrics(metrics)
    return {
        "case_count": metrics.case_count,
        "recall_at_k": metrics.retrieval.recall_at_k,
        "mrr": metrics.retrieval.mrr,
        "ndcg": metrics.retrieval.ndcg,
        "duplicate_source_share": metrics.duplicate_source_share,
        "source_diversity": metrics.source_diversity,
        "insufficient_evidence_count": metrics.insufficient_evidence_count,
    }


def validate_evaluation_metrics(metrics: EvaluationMetrics) -> None:
    """Reject non-aggregate or invalid metric values before persistence."""
    if not isinstance(metrics.case_count, int) or isinstance(metrics.case_count, bool) or metrics.case_count < 0:
        raise ExperimentError("case_count must be a non-negative integer")
    if not isinstance(metrics.insufficient_evidence_count, int) or isinstance(metrics.insufficient_evidence_count, bool) or metrics.insufficient_evidence_count < 0:
        raise ExperimentError("insufficient_evidence_count must be a non-negative integer")
    for name, value in (
        ("recall_at_k", metrics.retrieval.recall_at_k),
        ("mrr", metrics.retrieval.mrr),
        ("ndcg", metrics.retrieval.ndcg),
        ("duplicate_source_share", metrics.duplicate_source_share),
        ("source_diversity", metrics.source_diversity),
    ):
        _fraction(value, name)


def require_safe_identifier(value: str, label: str) -> str:
    """Allow compact operator labels while excluding user or corpus text."""
    if not isinstance(value, str) or not _SAFE_PHASE_NAME.fullmatch(value.replace("-", "_")):
        raise ExperimentError(f"{label} must be a safe non-content identifier")
    return value


def reject_content_fields(value: object) -> None:
    """Reject known corpus, prompt, and answer field names recursively."""
    _reject_content_keys(value)


def validate_experiment_json(field_name: str, value: object) -> None:
    """Validate one persisted experiment JSON value against its closed safe schema.

    Experiment JSON stores aggregate numeric metrics only. String values are
    refused before schema validation so content, DSNs, URLs, and credentials
    cannot be nested in otherwise plausible metric payloads.
    """
    _reject_unsafe_experiment_json(value)
    if field_name in _METRIC_JSON_FIELDS:
        _validate_metric_record(_mapping(value, field_name))
        return
    if field_name == "phase_percentiles":
        _validate_percentile_records(_mapping(value, field_name))
        return
    if field_name == "baseline_snapshot":
        _validate_baseline_snapshot(_mapping(value, field_name))
        return
    raise ExperimentError("unknown experiment JSON field")


def require_sha256(value: object, label: str) -> str:
    """Validate a content-free SHA-256 identity exposed by control-plane callers."""
    return _require_sha256(value, label)


def normalize_money(value: Decimal | int | float | str) -> Decimal:
    """Normalize persisted content-free cost values consistently with policy budgets."""
    return _money(value)


def canonical_json(value: object) -> str:
    """Return a stable JSON representation suitable for SHA-256 identity hashes."""
    return json.dumps(_canonical_value(value), sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def config_sha256(config: object) -> str:
    return hashlib.sha256(canonical_json(config).encode("utf-8")).hexdigest()


def resume_key(config: object, dataset_sha256: str) -> str:
    """Bind resumability to exact canonical configuration and dataset bytes."""
    dataset = _require_sha256(dataset_sha256, "dataset_sha256")
    return hashlib.sha256(canonical_json({"config_sha256": config_sha256(config), "dataset_sha256": dataset}).encode("utf-8")).hexdigest()


def hash_identifier(identifier: str | int) -> str:
    value = str(identifier)
    if not value:
        raise ExperimentError("identifier must not be empty")
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def split_ids(ids: Iterable[str | int], *, holdout_fraction: Decimal = Decimal("0.30")) -> DatasetSplit:
    """Deterministically partition unique IDs by their SHA-256 value into 70/30 sets."""
    fraction = _decimal_fraction(holdout_fraction, "holdout_fraction")
    if not 0 < fraction < 1:
        raise ExperimentError("holdout_fraction must be between zero and one")
    threshold = int(fraction * (1 << 256))
    train: set[str] = set()
    holdout: set[str] = set()
    seen_hashes: set[str] = set()
    for identifier in ids:
        value = str(identifier)
        digest = hash_identifier(value)
        if digest in seen_hashes:
            raise ExperimentError("evaluation IDs must be unique")
        seen_hashes.add(digest)
        (holdout if int(digest, 16) < threshold else train).add(value)
    if not seen_hashes:
        raise ExperimentError("evaluation IDs must not be empty")
    return DatasetSplit(frozenset(train), frozenset(holdout))


def create_campaign(campaign_id: str, config: object, dataset_sha256: str) -> Campaign:
    require_safe_identifier(campaign_id, "campaign_id")
    config_hash = config_sha256(config)
    dataset_hash = _require_sha256(dataset_sha256, "dataset_sha256")
    return Campaign(campaign_id, config_hash, dataset_hash, resume_key(config, dataset_hash))


def transition_campaign(campaign: Campaign, target: CampaignState) -> Campaign:
    if campaign.state == target:
        return campaign
    allowed = {
        CampaignState.DRAFT: {CampaignState.READY, CampaignState.CANCELLED},
        CampaignState.READY: {CampaignState.RUNNING, CampaignState.CANCELLED},
        CampaignState.RUNNING: {CampaignState.COMPLETED, CampaignState.FAILED, CampaignState.CANCELLED},
        CampaignState.COMPLETED: set(),
        CampaignState.FAILED: set(),
        CampaignState.CANCELLED: set(),
    }
    if target not in allowed[campaign.state]:
        raise StateTransitionError(f"campaign cannot transition from {campaign.state.value} to {target.value}")
    return replace(campaign, state=target)


def resume_campaign(campaign: Campaign, config: object, dataset_sha256: str) -> Campaign:
    """Resume only the failed campaign identified by the same config and dataset."""
    if campaign.resume_key != resume_key(config, dataset_sha256):
        raise StateTransitionError("resume key does not match campaign config and dataset")
    if campaign.state == CampaignState.COMPLETED:
        return campaign
    if campaign.state != CampaignState.FAILED:
        raise StateTransitionError("only failed campaigns may be resumed")
    return replace(campaign, state=CampaignState.READY)


def transition_candidate(candidate: Candidate, target: CandidateState) -> Candidate:
    if candidate.state == target:
        return candidate
    allowed = {
        CandidateState.PLANNED: {CandidateState.RUNNING, CandidateState.SKIPPED},
        CandidateState.RUNNING: {CandidateState.EVALUATED, CandidateState.FAILED, CandidateState.SKIPPED},
        CandidateState.EVALUATED: set(),
        CandidateState.FAILED: set(),
        CandidateState.SKIPPED: set(),
    }
    if target not in allowed[candidate.state]:
        raise StateTransitionError(f"candidate cannot transition from {candidate.state.value} to {target.value}")
    return replace(candidate, state=target)


class BudgetLedger:
    """Local reservation ledger; callers must reserve before any chargeable operation."""

    def __init__(self, limit_usd: Decimal = Decimal("1.00")) -> None:
        self.limit_usd = _money(limit_usd)
        if self.limit_usd <= 0:
            raise ExperimentError("limit_usd must be positive")
        self._reservations: dict[str, BudgetReservation] = {}
        self._actual_usd = Decimal("0")

    @property
    def reserved_usd(self) -> Decimal:
        return sum((reservation.projected_usd for reservation in self._reservations.values()), Decimal("0"))

    @property
    def actual_usd(self) -> Decimal:
        return self._actual_usd

    @property
    def available_usd(self) -> Decimal:
        return self.limit_usd - self.reserved_usd - self.actual_usd

    def reserve(self, key: str, projected_usd: Decimal) -> BudgetReservation:
        projected = _money(projected_usd)
        if not key:
            raise ExperimentError("reservation key must not be empty")
        if projected <= 0:
            raise ExperimentError("projected_usd must be positive")
        existing = self._reservations.get(key)
        if existing is not None:
            if existing.projected_usd != projected:
                raise BudgetExceeded("reservation key already has a different projection")
            return existing
        if projected > self.available_usd:
            raise BudgetExceeded("projected charge exceeds experiment budget")
        reservation = BudgetReservation(key, projected)
        self._reservations[key] = reservation
        return reservation

    def settle(self, key: str, actual_usd: Decimal) -> Decimal:
        actual = _money(actual_usd)
        reservation = self._reservations.get(key)
        if reservation is None:
            raise BudgetExceeded("actual charge has no reservation")
        if actual < 0 or actual > reservation.projected_usd:
            raise BudgetExceeded("actual charge exceeds its reserved projection")
        del self._reservations[key]
        self._actual_usd += actual
        return actual

    def release(self, key: str) -> None:
        self._reservations.pop(key, None)


def retrieval_metrics(expected_ids: Iterable[str | int], ranked_ids: Iterable[str | int], *, limit: int) -> RetrievalMetrics:
    """Calculate binary metrics over the first occurrence of each canonical parent."""
    if limit < 1:
        raise ExperimentError("limit must be positive")
    expected = {str(value) for value in expected_ids}
    if not expected:
        raise ExperimentError("expected IDs must not be empty")
    retrieved = _unique_ranked_ids(ranked_ids)[:limit]
    relevant_ranks = [rank for rank, value in enumerate(retrieved, start=1) if value in expected]
    recall = len(set(retrieved) & expected) / len(expected)
    mrr = 1 / relevant_ranks[0] if relevant_ranks else 0.0
    dcg = sum(1 / math.log2(rank + 1) for rank in relevant_ranks)
    ideal = sum(1 / math.log2(rank + 1) for rank in range(1, min(limit, len(expected)) + 1))
    return RetrievalMetrics(recall, mrr, dcg / ideal if ideal else 0.0)


def duplicate_share(source_ids: Iterable[str | int]) -> float:
    sources = [str(value) for value in source_ids]
    return (len(sources) - len(set(sources))) / len(sources) if sources else 0.0


def source_diversity(source_ids: Iterable[str | int]) -> float:
    sources = [str(value) for value in source_ids]
    return len(set(sources)) / len(sources) if sources else 0.0


def insufficient_evidence(source_count: int, *, minimum_sources: int) -> bool:
    if source_count < 0 or minimum_sources < 1:
        raise ExperimentError("source counts must be non-negative and minimum_sources positive")
    return source_count < minimum_sources


def phase_timing_summary(phase_timings_ms: Mapping[str, Iterable[float]]) -> dict[str, dict[str, float | int]]:
    """Summarize phase timing with nearest-rank p50/p95/p99 percentiles."""
    summary: dict[str, dict[str, float | int]] = {}
    for phase, values in phase_timings_ms.items():
        if not _SAFE_PHASE_NAME.fullmatch(phase) or _normalise_key(phase) in _PROHIBITED_CONTENT_KEYS:
            raise ExperimentError("phase name is unsafe")
        timings = sorted(float(value) for value in values)
        if not timings or any(not math.isfinite(value) or value < 0 for value in timings):
            raise ExperimentError("phase timings must be non-empty finite non-negative values")
        summary[phase] = {
            "count": len(timings),
            "p50_ms": _nearest_rank(timings, 0.50),
            "p95_ms": _nearest_rank(timings, 0.95),
            "p99_ms": _nearest_rank(timings, 0.99),
        }
    return summary


def promotion_decision(metrics: EvaluationMetrics, policy: ExperimentPolicy, *, initial_dataset: bool) -> PromotionDecision:
    if metrics.case_count < policy.min_evaluation_cases or metrics.insufficient_evidence_count:
        return PromotionDecision.INSUFFICIENT_EVIDENCE
    quality_passes = (
        metrics.retrieval.recall_at_k >= policy.min_recall_at_k
        and metrics.retrieval.mrr >= policy.min_mrr
        and metrics.retrieval.ndcg >= policy.min_ndcg
        and metrics.duplicate_source_share <= policy.max_duplicate_source_share
        and metrics.source_diversity >= policy.min_source_diversity
    )
    if not quality_passes:
        return PromotionDecision.FAILING
    # BL-21's initial corpus is review-only even when every quantitative gate passes.
    if initial_dataset or not policy.allow_automatic_promotion:
        return PromotionDecision.PASSING_FOR_REVIEW
    return PromotionDecision.PROMOTED


def preflight_experiment_dir(
    project_root: Path,
    *,
    create: bool = True,
    launcher_git_metadata: Mapping[str, str] | None = None,
) -> Path:
    """Validate the one allowed report directory, optionally without creating it."""
    requested_path = Path(os.path.abspath(project_root))
    if _paths_overlap(requested_path, SOURCE_WORKTREE) or _paths_overlap(requested_path, SOURCE_KNOWLEDGE_INDEX):
        raise UnsafeExperimentPath("the source working tree is never an experiment root")
    requested_root = _resolve_without_symlinks(project_root)
    try:
        root = _git_repository_root(requested_root)
    except FileNotFoundError:
        root = requested_root
        _launcher_git_identity(launcher_git_metadata)
    else:
        if root != requested_root:
            raise UnsafeExperimentPath("experiment root must be the resolved git repository root")
        direct_identity = _git_identity(root)
        if launcher_git_metadata is not None and _launcher_git_identity(launcher_git_metadata) != direct_identity:
            raise UnsafeExperimentPath("launcher Git metadata does not match the experiment clone")
    if _paths_overlap(root, SOURCE_WORKTREE) or _paths_overlap(root, SOURCE_KNOWLEDGE_INDEX):
        raise UnsafeExperimentPath("the source working tree is never an experiment root")
    if _contains_knowledge_data_path(root):
        raise UnsafeExperimentPath(".data/knowledge is not an experiment report root")
    data_dir = root / ".data-experiment"
    experiments_dir = data_dir / "experiments"
    for path in (data_dir, experiments_dir):
        if path.is_symlink() or (path.exists() and not path.is_dir()):
            raise UnsafeExperimentPath("experiment report path contains an unsafe component")
    if create:
        experiments_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    if experiments_dir.is_symlink() or experiments_dir.resolve() != experiments_dir:
        raise UnsafeExperimentPath("experiment report path resolves outside the clone")
    return experiments_dir


def write_experiment_report(
    project_root: Path,
    report_name: str,
    report: Mapping[str, object],
    *,
    launcher_git_metadata: Mapping[str, str] | None = None,
) -> Path:
    """Atomically write one schema-validated, content-free JSON report."""
    if not _SAFE_REPORT_NAME.fullmatch(report_name) or "/" in report_name or "\\" in report_name:
        raise UnsafeExperimentPath("report name must be a simple .json filename")
    validate_report(report)
    destination_dir = preflight_experiment_dir(project_root, launcher_git_metadata=launcher_git_metadata)
    destination = destination_dir / report_name
    if destination.is_symlink() or (destination.exists() and not destination.is_file()):
        raise UnsafeExperimentPath("refusing to replace a symlinked report")
    payload = json.dumps(report, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("utf-8") + b"\n"
    descriptor, temporary_name = tempfile.mkstemp(prefix=".report-", suffix=".tmp", dir=destination_dir)
    try:
        with os.fdopen(descriptor, "wb") as temporary:
            temporary.write(payload)
            temporary.flush()
            os.fsync(temporary.fileno())
        os.replace(temporary_name, destination)
        directory_descriptor = os.open(destination_dir, os.O_RDONLY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    finally:
        if os.path.exists(temporary_name):
            os.unlink(temporary_name)
    return destination


def validate_report(report: Mapping[str, object]) -> None:
    """Enforce a closed report schema so corpus and prompt content cannot persist."""
    _reject_content_keys(report)
    _exact_keys(report, {"schema_version", "campaign", "candidates"}, "report")
    if report["schema_version"] != REPORT_SCHEMA_VERSION:
        raise ExperimentError("unsupported report schema version")
    campaign = _mapping(report["campaign"], "campaign")
    _exact_keys(campaign, {"config_sha256", "dataset_sha256", "resume_key", "state", "split", "budget", "baseline_run_id", "baseline_snapshot_sha256", "baseline_snapshot"}, "campaign")
    _require_sha256(campaign["config_sha256"], "config_sha256")
    _require_sha256(campaign["dataset_sha256"], "dataset_sha256")
    _require_sha256(campaign["resume_key"], "resume_key")
    _enum_value(campaign["state"], CampaignState, "campaign state")
    if not isinstance(campaign["baseline_run_id"], int) or isinstance(campaign["baseline_run_id"], bool) or campaign["baseline_run_id"] < 1:
        raise ExperimentError("baseline_run_id must be a positive integer")
    _require_sha256(campaign["baseline_snapshot_sha256"], "baseline_snapshot_sha256")
    _validate_baseline_snapshot(_mapping(campaign["baseline_snapshot"], "baseline_snapshot"))
    if config_sha256(campaign["baseline_snapshot"]) != campaign["baseline_snapshot_sha256"]:
        raise ExperimentError("baseline snapshot hash does not match")
    _validate_split(_mapping(campaign["split"], "split"))
    _validate_budget(_mapping(campaign["budget"], "budget"))
    candidates = report["candidates"]
    if not isinstance(candidates, list):
        raise ExperimentError("candidates must be a list")
    for candidate in candidates:
        _validate_candidate_report(_mapping(candidate, "candidate"))


def _validate_candidate_report(candidate: Mapping[str, object]) -> None:
    _exact_keys(candidate, {"candidate_key", "state", "decision", "decision_reason", "configuration", "development", "holdout", "comparison"}, "candidate")
    _require_sha256(candidate["candidate_key"], "candidate_key")
    _enum_value(candidate["state"], CandidateState, "candidate state")
    _enum_value(candidate["decision"], PromotionDecision, "promotion decision")
    require_safe_identifier(candidate["decision_reason"], "decision_reason")
    _validate_candidate_configuration(_mapping(candidate["configuration"], "candidate configuration"))
    _validate_candidate_comparison(_mapping(candidate["comparison"], "candidate comparison"))
    development = candidate["development"]
    if development is None:
        if candidate["state"] != CandidateState.FAILED.value or candidate["holdout"] is not None:
            raise ExperimentError("only failed candidates may omit phase metrics")
        return
    _validate_candidate_phase(_mapping(development, "candidate development"))
    holdout = candidate["holdout"]
    if candidate["state"] == CandidateState.EVALUATED.value and holdout is None:
        raise ExperimentError("evaluated candidate requires holdout metrics")
    if holdout is not None:
        _validate_candidate_phase(_mapping(holdout, "candidate holdout"))


def _validate_candidate_configuration(configuration: Mapping[str, object]) -> None:
    _exact_keys(configuration, {"hypothesis_id", "lexical_mode", "source", "result_limit", "pool_limit"}, "candidate configuration")
    for key in ("hypothesis_id", "lexical_mode", "source"):
        require_safe_identifier(configuration[key], key)
    for key in ("result_limit", "pool_limit"):
        value = configuration[key]
        if not isinstance(value, int) or isinstance(value, bool) or value < 1:
            raise ExperimentError(f"{key} must be a positive integer")
    if configuration["pool_limit"] < configuration["result_limit"]:
        raise ExperimentError("candidate pool_limit must cover result_limit")


def _validate_candidate_phase(phase: Mapping[str, object]) -> None:
    _exact_keys(phase, {"metrics", "timings"}, "candidate phase")
    validate_experiment_json("dev_metrics", phase["metrics"])
    validate_experiment_json("phase_percentiles", phase["timings"])


def _validate_baseline_snapshot(snapshot: Mapping[str, object]) -> None:
    _exact_keys(snapshot, {"run_id", "index_version", "metrics", "latency"}, "baseline snapshot")
    for key in ("run_id", "index_version"):
        if not isinstance(snapshot[key], int) or isinstance(snapshot[key], bool) or snapshot[key] < 1:
            raise ExperimentError(f"baseline snapshot {key} must be a positive integer")
    metrics = _mapping(snapshot["metrics"], "baseline snapshot metrics")
    _exact_keys(metrics, {"recall_at_k", "mrr", "ndcg", "duplicate_source_share"}, "baseline snapshot metrics")
    for key, value in metrics.items():
        _fraction(value, key)
    latency = _mapping(snapshot["latency"], "baseline snapshot latency")
    _exact_keys(latency, {"historical_mean_ms", "phase_percentiles_available"}, "baseline snapshot latency")
    if latency["historical_mean_ms"] is not None and (
        not isinstance(latency["historical_mean_ms"], int)
        or isinstance(latency["historical_mean_ms"], bool)
        or latency["historical_mean_ms"] < 0
    ):
        raise ExperimentError("baseline snapshot historical latency must be a non-negative integer or null")
    if latency["phase_percentiles_available"] is not False:
        raise ExperimentError("historical baseline phase percentiles are unavailable")


def _validate_candidate_comparison(comparison: Mapping[str, object]) -> None:
    _exact_keys(comparison, {"baseline_snapshot_sha256", "quality_deltas", "latency"}, "candidate comparison")
    _require_sha256(comparison["baseline_snapshot_sha256"], "comparison baseline snapshot sha256")
    deltas = _mapping(comparison["quality_deltas"], "candidate quality deltas")
    _exact_keys(deltas, {"development", "holdout"}, "candidate quality deltas")
    for phase, value in deltas.items():
        if value is not None:
            metrics = _mapping(value, f"candidate {phase} quality deltas")
            _exact_keys(metrics, {"recall_at_k", "mrr", "ndcg", "duplicate_source_share"}, "candidate quality deltas")
            for metric_value in metrics.values():
                if not isinstance(metric_value, (int, float)) or isinstance(metric_value, bool) or not math.isfinite(metric_value):
                    raise ExperimentError("candidate quality delta must be finite")
    latency = _mapping(comparison["latency"], "candidate latency comparison")
    _exact_keys(latency, {"status"}, "candidate latency comparison")
    if latency["status"] != "baseline_phase_percentiles_unavailable":
        raise ExperimentError("candidate latency comparison is invalid")


def _validate_metric_record(metrics: Mapping[str, object]) -> None:
    _exact_keys(metrics, _METRIC_JSON_KEYS, "metrics")
    for key in ("case_count", "insufficient_evidence_count"):
        if not isinstance(metrics[key], int) or isinstance(metrics[key], bool) or metrics[key] < 0:
            raise ExperimentError(f"{key} must be a non-negative integer")
    for key in ("recall_at_k", "mrr", "ndcg", "duplicate_source_share", "source_diversity"):
        _fraction(metrics[key], key)


def _validate_percentile_records(timings: Mapping[str, object]) -> None:
    for phase, summary in timings.items():
        if not isinstance(phase, str) or not _SAFE_PHASE_NAME.fullmatch(phase) or _normalise_key(phase) in _PROHIBITED_CONTENT_KEYS:
            raise ExperimentError("timing phase name is invalid")
        values = _mapping(summary, "timing summary")
        _exact_keys(values, _PERCENTILE_JSON_KEYS, "timing summary")
        if not isinstance(values["count"], int) or isinstance(values["count"], bool) or values["count"] < 1:
            raise ExperimentError("timing count must be a positive integer")
        for key in ("p50_ms", "p95_ms", "p99_ms"):
            value = values[key]
            if not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(value) or value < 0:
                raise ExperimentError(f"{key} must be finite and non-negative")


def _validate_split(split: Mapping[str, object]) -> None:
    _exact_keys(split, {"train_count", "holdout_count", "train_id_hashes", "holdout_id_hashes"}, "split")
    for count_key, hashes_key in (("train_count", "train_id_hashes"), ("holdout_count", "holdout_id_hashes")):
        count = split[count_key]
        hashes = split[hashes_key]
        if not isinstance(count, int) or isinstance(count, bool) or count < 0:
            raise ExperimentError(f"{count_key} must be a non-negative integer")
        if not isinstance(hashes, list) or len(hashes) != count:
            raise ExperimentError(f"{hashes_key} must match its count")
        if len(set(hashes)) != len(hashes):
            raise ExperimentError(f"{hashes_key} must be unique")
        for value in hashes:
            _require_sha256(value, hashes_key)


def _validate_budget(budget: Mapping[str, object]) -> None:
    _exact_keys(budget, {"limit_usd", "reserved_usd", "actual_usd"}, "budget")
    values = {key: _money(value) for key, value in budget.items()}
    if values["limit_usd"] <= 0 or values["reserved_usd"] < 0 or values["actual_usd"] < 0:
        raise ExperimentError("budget values are invalid")
    if values["reserved_usd"] + values["actual_usd"] > values["limit_usd"]:
        raise ExperimentError("reported budget exceeds its limit")


def _canonical_value(value: object) -> object:
    if is_dataclass(value) and not isinstance(value, type):
        return _canonical_value(asdict(value))
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Decimal):
        return format(value, "f")
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ExperimentError("configuration floats must be finite")
        return value
    if isinstance(value, Mapping):
        normalized: dict[str, object] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise ExperimentError("configuration keys must be strings")
            normalized[key] = _canonical_value(item)
        return normalized
    if isinstance(value, (list, tuple)):
        return [_canonical_value(item) for item in value]
    raise ExperimentError(f"unsupported canonical configuration value: {type(value).__name__}")


def _decimal_fraction(value: Decimal, name: str) -> Decimal:
    try:
        decimal = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ExperimentError(f"{name} must be decimal") from exc
    if not decimal.is_finite():
        raise ExperimentError(f"{name} must be finite")
    return decimal


def _money(value: Decimal | int | float | str) -> Decimal:
    try:
        decimal = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ExperimentError("money must be decimal") from exc
    if not decimal.is_finite():
        raise ExperimentError("money must be finite")
    return decimal.quantize(Decimal("0.000001"))


def _nearest_rank(values: list[float], percentile: float) -> float:
    return values[max(0, math.ceil(percentile * len(values)) - 1)]


def _unique_ranked_ids(ranked_ids: Iterable[str | int]) -> list[str]:
    unique: list[str] = []
    seen: set[str] = set()
    for identifier in ranked_ids:
        value = str(identifier)
        if value not in seen:
            seen.add(value)
            unique.append(value)
    return unique


def _normalise_key(key: str) -> str:
    return re.sub(r"[^a-z0-9]", "", key.lower())


def _reject_content_keys(value: object) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                raise ExperimentError("report keys must be strings")
            if _normalise_key(key) in _PROHIBITED_CONTENT_KEYS:
                raise ExperimentError(f"prohibited content key: {key}")
            _reject_content_keys(item)
    elif isinstance(value, list):
        for item in value:
            _reject_content_keys(item)


def _reject_unsafe_experiment_json(value: object) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                raise ExperimentError("experiment JSON keys must be strings")
            if _normalise_key(key) in _PROHIBITED_CONTENT_KEYS:
                raise ExperimentError(f"prohibited experiment content key: {key}")
            _reject_unsafe_experiment_json(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _reject_unsafe_experiment_json(item)
    elif isinstance(value, str):
        raise ExperimentError("experiment JSON values must not contain strings")


def _exact_keys(value: Mapping[str, object], expected: set[str], label: str) -> None:
    if set(value) != expected:
        raise ExperimentError(f"{label} schema keys are invalid")


def _mapping(value: object, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ExperimentError(f"{label} must be an object")
    return value


def _require_sha256(value: object, label: str) -> str:
    if not isinstance(value, str) or len(value) != _HASH_LENGTH or any(char not in "0123456789abcdef" for char in value):
        raise ExperimentError(f"{label} must be a lowercase SHA-256 hash")
    return value


def _enum_value(value: object, enum_type: type[Enum], label: str) -> None:
    try:
        enum_type(value)
    except (TypeError, ValueError) as exc:
        raise ExperimentError(f"invalid {label}") from exc


def _fraction(value: object, label: str) -> None:
    if not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(value) or not 0 <= value <= 1:
        raise ExperimentError(f"{label} must be a finite fraction")


def _resolve_without_symlinks(path: Path) -> Path:
    if not path.exists() or not path.is_dir():
        raise UnsafeExperimentPath("experiment root must be an existing directory")
    if ".." in path.parts:
        raise UnsafeExperimentPath("experiment root must not contain parent traversal")
    current = Path(path.anchor) if path.is_absolute() else Path.cwd()
    for part in path.parts[1:] if path.is_absolute() else path.parts:
        current /= part
        if current.is_symlink():
            raise UnsafeExperimentPath("experiment root must not traverse symlinks")
    return path.resolve()


def _contains_knowledge_data_path(path: Path) -> bool:
    if _is_knowledge_data_path(path):
        return True
    for current, directories, files in os.walk(path, followlinks=False, onerror=_raise_unsafe_walk_error):
        current_path = Path(current)
        if _is_knowledge_data_path(current_path):
            return True
        if any(_is_knowledge_data_path(current_path / directory) for directory in directories):
            return True
        if current_path.name == ".data" and "knowledge" in files:
            return True
        if any((current_path / directory).is_symlink() for directory in directories):
            raise UnsafeExperimentPath("experiment root must not contain symlinked descendants")
        directories[:] = [directory for directory in directories if directory != ".git"]
    return False


def _is_knowledge_data_path(path: Path) -> bool:
    parts = path.parts
    return any(parts[index:index + 2] == (".data", "knowledge") for index in range(len(parts) - 1))


def _paths_overlap(first: Path, second: Path) -> bool:
    return first == second or first in second.parents or second in first.parents


def _raise_unsafe_walk_error(error: OSError) -> None:
    raise UnsafeExperimentPath("experiment root descendants must be readable") from error


def _git_repository_root(project_root: Path) -> Path:
    completed = _git(project_root, "rev-parse", "--show-toplevel")
    if completed.returncode != 0 or not completed.stdout.strip():
        raise UnsafeExperimentPath("experiment root must be a git working tree")
    reported_root = Path(os.path.abspath(completed.stdout.strip()))
    if _paths_overlap(reported_root, SOURCE_WORKTREE) or _paths_overlap(reported_root, SOURCE_KNOWLEDGE_INDEX):
        raise UnsafeExperimentPath("the source working tree is never an experiment root")
    return _resolve_without_symlinks(reported_root)


def _git_branch(project_root: Path) -> str:
    completed = _git(project_root, "symbolic-ref", "--quiet", "--short", "HEAD")
    if completed.returncode != 0 or not completed.stdout.strip():
        raise UnsafeExperimentPath("experiment root must be on a named branch")
    return completed.stdout.strip()


def _git_revision(project_root: Path) -> str:
    completed = _git(project_root, "rev-parse", "--verify", "HEAD^{commit}")
    revision = completed.stdout.strip()
    if completed.returncode != 0 or not _GIT_REVISION.fullmatch(revision):
        raise UnsafeExperimentPath("experiment root must resolve to a full commit revision")
    return revision


def _git_identity(project_root: Path) -> GitIdentity:
    branch = _git_branch(project_root)
    if branch != FEATURE_BRANCH:
        raise UnsafeExperimentPath("experiment reports require the BL-21 feature branch")
    return GitIdentity(branch=branch, revision=_git_revision(project_root))


def _launcher_git_identity(metadata: Mapping[str, str] | None) -> GitIdentity:
    if metadata is None:
        raise UnsafeExperimentPath("experiment runner requires launcher Git metadata when Git is unavailable")
    branch = metadata.get(RUNNER_GIT_BRANCH_ENV)
    revision = metadata.get(RUNNER_GIT_REVISION_ENV)
    if branch != FEATURE_BRANCH:
        raise UnsafeExperimentPath("launcher Git metadata must name the BL-21 feature branch")
    if not isinstance(revision, str) or not _GIT_REVISION.fullmatch(revision):
        raise UnsafeExperimentPath("launcher Git metadata must contain a full lowercase commit revision")
    return GitIdentity(branch=branch, revision=revision)


def _git(project_root: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
    environment = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    return subprocess.run(
        ["git", "-C", str(project_root), *arguments],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )
