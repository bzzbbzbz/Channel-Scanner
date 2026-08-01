"""Pure coverage for the isolated BL-21 experiment foundation."""

from decimal import Decimal
from pathlib import Path
import subprocess

import pytest

from src.knowledge.experiments import (
    FEATURE_BRANCH,
    BudgetExceeded,
    BudgetLedger,
    CampaignState,
    Candidate,
    CandidateState,
    EvaluationMetrics,
    ExperimentError,
    ExperimentPolicy,
    PromotionDecision,
    RetrievalMetrics,
    StateTransitionError,
    UnsafeExperimentPath,
    canonical_json,
    config_sha256,
    create_campaign,
    duplicate_share,
    hash_identifier,
    insufficient_evidence,
    phase_timing_summary,
    preflight_experiment_dir,
    promotion_decision,
    retrieval_metrics,
    resume_campaign,
    source_diversity,
    split_ids,
    transition_campaign,
    transition_candidate,
    validate_report,
    write_experiment_report,
)


def _report() -> dict[str, object]:
    dataset_hash = "a" * 64
    return {
        "schema_version": 1,
        "campaign": {
            "config_sha256": "b" * 64,
            "dataset_sha256": dataset_hash,
            "resume_key": "c" * 64,
            "state": "completed",
            "split": {
                "train_count": 1,
                "holdout_count": 1,
                "train_id_hashes": [hash_identifier("case-a")],
                "holdout_id_hashes": [hash_identifier("case-b")],
            },
            "budget": {"limit_usd": "1.00", "reserved_usd": "0", "actual_usd": "0.4"},
        },
        "candidates": [{
            "candidate_key": "d" * 64,
            "state": "evaluated",
            "decision": "passing_for_review",
            "metrics": {
                "case_count": 2,
                "recall_at_k": 1.0,
                "mrr": 1.0,
                "ndcg": 1.0,
                "duplicate_source_share": 0.0,
                "source_diversity": 1.0,
                "insufficient_evidence_count": 0,
            },
            "timings": {"ranking": {"count": 2, "p50_ms": 4.0, "p95_ms": 9.0, "p99_ms": 9.0}},
        }],
    }


def test_config_hash_is_canonical_and_resume_key_changes_with_dataset() -> None:
    first = {"retrieval": {"limit": 5, "enabled": True}, "model": "local"}
    second = {"model": "local", "retrieval": {"enabled": True, "limit": 5}}

    assert canonical_json(first) == canonical_json(second)
    assert config_sha256(first) == config_sha256(second)
    campaign = create_campaign("bl21_batch", first, "a" * 64)
    changed = create_campaign("bl21_batch", first, "b" * 64)
    assert campaign.resume_key != changed.resume_key


def test_id_hash_split_is_repeatable_and_reportable_without_raw_ids() -> None:
    ids = ["case-1", "case-2", "case-3", "case-4", "case-5", "case-6", "case-7", "case-8", "case-9", "case-10"]

    first = split_ids(ids)
    second = split_ids(reversed(ids))
    report = first.reportable()

    assert first == second
    assert first.train_ids | first.holdout_ids == set(ids)
    assert report["train_count"] + report["holdout_count"] == len(ids)
    assert all(value not in str(report) for value in ids)
    assert {len(value) for value in report["train_id_hashes"] + report["holdout_id_hashes"]} == {64}


def test_campaign_and_candidate_transitions_are_strict_and_terminal_idempotent() -> None:
    campaign = create_campaign("batch_one", {"limit": 5}, "a" * 64)
    ready = transition_campaign(campaign, CampaignState.READY)
    running = transition_campaign(ready, CampaignState.RUNNING)
    completed = transition_campaign(running, CampaignState.COMPLETED)

    assert transition_campaign(completed, CampaignState.COMPLETED) is completed
    with pytest.raises(StateTransitionError):
        transition_campaign(completed, CampaignState.RUNNING)
    failed = transition_campaign(running, CampaignState.FAILED)
    assert resume_campaign(failed, {"limit": 5}, "a" * 64).state == CampaignState.READY
    with pytest.raises(StateTransitionError):
        resume_campaign(failed, {"limit": 6}, "a" * 64)

    candidate = Candidate("b" * 64, campaign.resume_key)
    evaluated = transition_candidate(transition_candidate(candidate, CandidateState.RUNNING), CandidateState.EVALUATED)
    assert transition_candidate(evaluated, CandidateState.EVALUATED) is evaluated
    with pytest.raises(StateTransitionError):
        transition_candidate(evaluated, CandidateState.RUNNING)


def test_budget_reservations_block_projected_and_actual_overage() -> None:
    ledger = BudgetLedger()
    ledger.reserve("first", Decimal("0.60"))

    with pytest.raises(BudgetExceeded):
        ledger.reserve("second", Decimal("0.41"))
    with pytest.raises(BudgetExceeded):
        ledger.settle("first", Decimal("0.61"))

    assert ledger.settle("first", Decimal("0.40")) == Decimal("0.400000")
    ledger.reserve("second", Decimal("0.60"))
    assert ledger.actual_usd == Decimal("0.400000")
    assert ledger.reserved_usd == Decimal("0.600000")


def test_metrics_match_legacy_retrieval_semantics_and_nearest_rank_timings() -> None:
    metrics = retrieval_metrics([101, 102], [999, 102, 101], limit=3)

    assert metrics.recall_at_k == 1
    assert metrics.mrr == 0.5
    assert metrics.ndcg == pytest.approx(0.6934264036)
    deduplicated = retrieval_metrics([101, 102], [101, 101, 102], limit=3)
    assert deduplicated == RetrievalMetrics(1.0, 1.0, 1.0)
    assert duplicate_share(["a", "a", "b", "c"]) == 0.25
    assert source_diversity(["a", "a", "b", "c"]) == 0.75
    assert insufficient_evidence(1, minimum_sources=2)
    assert phase_timing_summary({"ranking": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]})["ranking"] == {
        "count": 10, "p50_ms": 5.0, "p95_ms": 10.0, "p99_ms": 10.0,
    }


def test_initial_dataset_never_auto_promotes_even_when_all_gates_pass() -> None:
    metrics = EvaluationMetrics(2, RetrievalMetrics(1, 1, 1), 0, 1, 0)
    policy = ExperimentPolicy(min_evaluation_cases=2, allow_automatic_promotion=True)

    assert promotion_decision(metrics, policy, initial_dataset=True) == PromotionDecision.PASSING_FOR_REVIEW
    assert promotion_decision(metrics, policy, initial_dataset=False) == PromotionDecision.PROMOTED
    assert promotion_decision(EvaluationMetrics(1, RetrievalMetrics(1, 1, 1), 0, 1, 0), policy, initial_dataset=True) == PromotionDecision.INSUFFICIENT_EVIDENCE


def test_report_writer_is_atomic_and_rejects_content_and_unsafe_paths(tmp_path) -> None:
    report = _report()
    validate_report(report)
    clone = _feature_clone(tmp_path)
    destination = write_experiment_report(clone, "batch-1.json", report)

    assert destination == clone / ".data" / "experiments" / "batch-1.json"
    assert destination.read_text(encoding="utf-8").endswith("\n")
    leaking = _report()
    leaking["questions"] = ["must never persist"]
    with pytest.raises(ExperimentError, match="prohibited content key"):
        validate_report(leaking)
    with pytest.raises(UnsafeExperimentPath):
        write_experiment_report(clone, "../escape.json", report)
    destination.unlink()
    destination.symlink_to(clone / "outside.json")
    with pytest.raises(UnsafeExperimentPath):
        write_experiment_report(clone, "batch-1.json", report)


def test_preflight_derives_git_root_and_branch_and_rejects_knowledge_paths(tmp_path) -> None:
    clone = _feature_clone(tmp_path)

    assert preflight_experiment_dir(clone) == clone / ".data" / "experiments"
    with pytest.raises(TypeError):
        preflight_experiment_dir(clone, branch=FEATURE_BRANCH)  # type: ignore[call-arg]
    with pytest.raises(UnsafeExperimentPath):
        preflight_experiment_dir(tmp_path)
    with pytest.raises(UnsafeExperimentPath):
        preflight_experiment_dir(clone / ".data" / "experiments")

    _git(clone, "checkout", "-b", "main")
    with pytest.raises(UnsafeExperimentPath):
        preflight_experiment_dir(clone)

    nested_clone = _feature_clone(tmp_path / ".data" / "knowledge")
    with pytest.raises(UnsafeExperimentPath):
        preflight_experiment_dir(nested_clone)

    descendant_clone = _feature_clone(tmp_path / "descendant")
    (descendant_clone / "nested" / ".data" / "knowledge").mkdir(parents=True)
    with pytest.raises(UnsafeExperimentPath):
        preflight_experiment_dir(descendant_clone)

    symlinked_clone = _feature_clone(tmp_path / "symlinked")
    knowledge_parent = tmp_path / "external-knowledge"
    (knowledge_parent / "knowledge").mkdir(parents=True)
    (symlinked_clone / "nested").mkdir()
    (symlinked_clone / "nested" / ".data").symlink_to(knowledge_parent, target_is_directory=True)
    with pytest.raises(UnsafeExperimentPath):
        preflight_experiment_dir(symlinked_clone)


def test_preflight_rejects_symlinks_and_source_worktree(tmp_path, monkeypatch) -> None:
    clone = _feature_clone(tmp_path)
    linked = tmp_path / "linked"
    linked.symlink_to(clone, target_is_directory=True)
    with pytest.raises(UnsafeExperimentPath):
        preflight_experiment_dir(linked)

    source_clone = _feature_clone(tmp_path / "source-worktree")
    monkeypatch.setattr("src.knowledge.experiments.SOURCE_WORKTREE", source_clone)
    with pytest.raises(UnsafeExperimentPath):
        preflight_experiment_dir(source_clone)


def test_experiment_compose_replaces_inherited_host_mounts() -> None:
    compose = (Path(__file__).parents[2] / "docker-compose.experiment.yml").read_text(encoding="utf-8")

    assert "volumes: !override" in compose
    assert "./.planning/evaluations" not in compose
    assert ".data/knowledge" not in compose
    assert "caddy_proxy: !reset null" in compose
    assert "ports: !reset []" in compose


def _feature_clone(parent: Path) -> Path:
    parent.mkdir(parents=True, exist_ok=True)
    source = parent / "source"
    clone = parent / "clone"
    source.mkdir()
    _git(source, "init")
    _git(source, "checkout", "-b", FEATURE_BRANCH)
    subprocess.run(["git", "clone", "--quiet", "--local", str(source), str(clone)], check=True)
    return clone


def _git(directory: Path, *arguments: str) -> None:
    subprocess.run(["git", "-C", str(directory), *arguments], check=True, capture_output=True, text=True)
