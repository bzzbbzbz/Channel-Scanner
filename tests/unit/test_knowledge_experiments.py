"""Pure coverage for the isolated BL-21 experiment foundation."""

from decimal import Decimal
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import socket
import stat
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
    report: dict[str, object] = {
        "schema_version": 3,
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
            "baseline_run_id": 7,
            "baseline_snapshot_sha256": "e" * 64,
            "baseline_snapshot": {
                "run_id": 7,
                "index_version": 1,
                "metrics": {"recall_at_k": 1.0, "mrr": 1.0, "ndcg": 1.0, "duplicate_source_share": 0.0},
                "latency": {"historical_mean_ms": 4, "phase_percentiles_available": False},
            },
        },
        "candidates": [{
            "candidate_key": "d" * 64,
            "state": "evaluated",
            "decision": "passing_for_review",
            "decision_reason": "development_selected_holdout_review",
            "configuration": {
                "hypothesis_id": "token_ilike_baseline",
                "lexical_mode": "token_ilike",
                "source": "canonical_post_content",
                "result_limit": 5,
                "pool_limit": 30,
            },
            "development": {
                "metrics": {
                    "case_count": 2,
                    "recall_at_k": 1.0,
                    "mrr": 1.0,
                    "ndcg": 1.0,
                    "duplicate_source_share": 0.0,
                    "source_diversity": 1.0,
                    "insufficient_evidence_count": 0,
                },
                "timings": {"retrieval": {"count": 2, "p50_ms": 4.0, "p95_ms": 9.0, "p99_ms": 9.0}, "lexical": {"count": 2, "p50_ms": 4.0, "p95_ms": 9.0, "p99_ms": 9.0}},
            },
            "holdout": {
                "metrics": {
                    "case_count": 2,
                    "recall_at_k": 1.0,
                    "mrr": 1.0,
                    "ndcg": 1.0,
                    "duplicate_source_share": 0.0,
                    "source_diversity": 1.0,
                    "insufficient_evidence_count": 0,
                },
                "timings": {"retrieval": {"count": 2, "p50_ms": 4.0, "p95_ms": 9.0, "p99_ms": 9.0}, "lexical": {"count": 2, "p50_ms": 4.0, "p95_ms": 9.0, "p99_ms": 9.0}},
            },
            "comparison": {
                "baseline_snapshot_sha256": "e" * 64,
                "quality_deltas": {
                    "development": {"recall_at_k": 0.0, "mrr": 0.0, "ndcg": 0.0, "duplicate_source_share": 0.0},
                    "holdout": {"recall_at_k": 0.0, "mrr": 0.0, "ndcg": 0.0, "duplicate_source_share": 0.0},
                },
                "latency": {"status": "baseline_phase_percentiles_unavailable"},
            },
        }],
    }
    snapshot_hash = config_sha256(report["campaign"]["baseline_snapshot"])  # type: ignore[index]
    report["campaign"]["baseline_snapshot_sha256"] = snapshot_hash  # type: ignore[index]
    report["candidates"][0]["comparison"]["baseline_snapshot_sha256"] = snapshot_hash  # type: ignore[index]
    return report


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

    assert destination == clone / ".data-experiment" / "experiments" / "batch-1.json"
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

    assert preflight_experiment_dir(clone) == clone / ".data-experiment" / "experiments"
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


def test_experiment_compose_mounts_the_safe_clone_and_replaces_inherited_host_mounts() -> None:
    compose = (Path(__file__).parents[2] / "docker-compose.experiment.yml").read_text(encoding="utf-8")

    assert "volumes: !override" in compose
    assert "- ./.git:/app/.git:ro" in compose
    assert "- ./.data-experiment/clone-manifest.json:/app/.data-experiment/clone-manifest.json:ro" in compose
    assert "- ./.data-experiment/inputs:/app/.data-experiment/inputs:ro" in compose
    assert "- ./.data-experiment/experiments:/app/.data-experiment/experiments" in compose
    assert "- ./:/app:ro" not in compose
    assert "- ./.data-experiment:/app/.data-experiment:ro" not in compose
    assert "- ./.data-experiment:/app/.data\n" not in compose
    assert "- ./.data:/app/.data\n" not in compose
    assert "/app/.data/experiments" not in compose
    assert "- ./.data-experiment/snapshots/bl21-local:/bl21-snapshot:ro" in compose
    app_overlay = compose.split("\n  app:\n", maxsplit=1)[1].split("\n  pgadmin:\n", maxsplit=1)[0]
    assert "snapshots/bl21-local" not in app_overlay
    assert "./.planning/evaluations" not in compose
    assert ".data/knowledge" not in compose
    assert "caddy_proxy: !reset null" in compose
    assert "ports: !reset []" in compose
    launcher = (Path(__file__).parents[2] / "docker/bl21-experiment-compose.sh").read_text(encoding="utf-8")
    assert "readonly LOCAL_DOCKER_SOCKET='/var/run/docker.sock'" in launcher


def test_experiment_compose_identity_is_clone_stable_and_distinct_per_canonical_path(tmp_path) -> None:
    source_root = Path(__file__).parents[2]
    first_clone = _launcher_clone(tmp_path / "first clone", source_root)
    second_clone = _launcher_clone(tmp_path / "second clone", source_root)
    first_alias = tmp_path / "first-alias"
    first_alias.symlink_to(first_clone, target_is_directory=True)

    first = _compose_identity(first_clone)
    second = _compose_identity(second_clone)
    aliased_first = _compose_identity(first_alias)
    existing_first = _compose_identity(source_root)
    existing_second = _compose_identity(source_root)

    assert first != second
    assert first["BL21_COMPOSE_PROJECT_NAME"] != second["BL21_COMPOSE_PROJECT_NAME"]
    assert first["BL21_EXPERIMENT_PGDATA_VOLUME"] != second["BL21_EXPERIMENT_PGDATA_VOLUME"]
    assert aliased_first == first
    assert existing_first == existing_second
    assert re.fullmatch(r"telegram-parser-bl21-[0-9a-f]{16}", first["BL21_COMPOSE_PROJECT_NAME"])
    assert re.fullmatch(r"telegram-parser-bl21-[0-9a-f]{16}-pgdata", first["BL21_EXPERIMENT_PGDATA_VOLUME"])
    assert first["BL21_EXPERIMENT_PGDATA_VOLUME"] == f'{first["BL21_COMPOSE_PROJECT_NAME"]}-pgdata'

    compose = (source_root / "docker-compose.experiment.yml").read_text(encoding="utf-8")
    assert "${BL21_EXPERIMENT_PGDATA_VOLUME:?" in compose
    assert "telegram-parser-bl21-experiments-pgdata" not in compose


def test_experiment_launcher_constructs_only_fixed_commands(tmp_path) -> None:
    source_root = Path(__file__).parents[2]
    clone = _launcher_clone(tmp_path / "clone", source_root)
    identity = _compose_identity(clone)
    capture = tmp_path / "docker-arguments.txt"
    fake_bin = _fake_docker(tmp_path, capture)

    build = _run_launcher(clone, fake_bin, "build-app")
    assert build.returncode == 0
    assert _captured_docker_arguments(capture) == _expected_compose_prefix(clone, identity) + ["build", "app"]

    config = _run_launcher(clone, fake_bin, "config")
    assert config.returncode == 0
    assert _captured_docker_arguments(capture) == _expected_compose_prefix(clone, identity) + ["config"]

    db_up = _run_launcher(clone, fake_bin, "db-up")
    assert db_up.returncode == 0
    db_up_calls = _captured_docker_calls(capture)[-4:]
    assert db_up_calls[0] == _expected_compose_prefix(clone, identity) + [
        "up", "--detach", "--no-deps", "db",
    ]
    assert db_up_calls[1] == _expected_compose_prefix(clone, identity) + ["ps", "--quiet", "db"]
    assert db_up_calls[2] == [
        "inspect", "--format", "{{json .}}", "0" * 64,
    ]
    assert db_up_calls[3] == [
        "inspect", "--format", "{{if .State.Health}}{{.State.Health.Status}}{{else}}missing{{end}}", "0" * 64,
    ]

    migrate = _run_launcher(clone, fake_bin, "migrate")
    assert migrate.returncode == 0
    assert _captured_docker_arguments(capture) == _expected_compose_prefix(clone, identity) + [
        "run", "--rm", "--no-deps", *_expected_one_off_git_metadata(clone), "--entrypoint", "alembic", "app", "upgrade", "head",
    ]

    evaluate = _run_launcher(
        clone,
        fake_bin,
        "evaluate",
        "--",
        "--experiment-root", "/app",
        "--database-url", "postgresql+asyncpg://bot:experiment-only-password@db:5432/telegram_bot_bl21_experiment?experiment=bl21",
        "--dataset", "/app/.data-experiment/inputs/turboproject-ai-2025-2026.jsonl",
        "--channel", "turboproject_ai",
        "--campaign-id", "bl21_smoke",
        "--dry-run",
    )
    assert evaluate.returncode == 0
    assert _captured_docker_arguments(capture) == _expected_compose_prefix(clone, identity) + [
        "run", "--rm", "--no-deps", *_expected_one_off_git_metadata(clone), "--entrypoint", "python", "app", "-m", "src.knowledge.experiment_runner",
        "--experiment-root", "/app",
        "--database-url", "postgresql+asyncpg://bot:experiment-only-password@db:5432/telegram_bot_bl21_experiment?experiment=bl21",
        "--dataset", "/app/.data-experiment/inputs/turboproject-ai-2025-2026.jsonl",
        "--channel", "turboproject_ai",
        "--campaign-id", "bl21_smoke",
        "--dry-run",
    ]
    captured = capture.read_text(encoding="utf-8")
    assert "LEAK=unset" in captured
    assert f"PROJECT={identity['BL21_COMPOSE_PROJECT_NAME']}" in captured
    assert f"VOLUME={identity['BL21_EXPERIMENT_PGDATA_VOLUME']}" in captured
    assert f"DOCKER_HOST=unix://{_test_socket_path(clone)}" in captured
    assert "DOCKER_CONTEXT=default" in captured
    assert f"HOME={clone}/.data-experiment/launcher-home" in captured
    assert f"DOCKER_CONFIG={clone}/.data-experiment/launcher-home/docker-config" in captured

    execute = _run_launcher(
        clone,
        fake_bin,
        "evaluate",
        "--",
        "--experiment-root", "/app",
        "--database-url", "postgresql+asyncpg://bot:experiment-only-password@db:5432/telegram_bot_bl21_experiment?experiment=bl21",
        "--dataset", "/app/.data-experiment/inputs/turboproject-ai-2025-2026.jsonl",
        "--channel", "turboproject_ai",
        "--campaign-id", "bl21_smoke",
        "--baseline-run-id", "42",
        "--execute",
    )
    assert execute.returncode == 0
    assert _captured_docker_arguments(capture)[-1] == "--execute"


def test_experiment_launcher_db_up_times_out_after_fixed_isolated_health_polls(tmp_path) -> None:
    source_root = Path(__file__).parents[2]
    clone = _launcher_clone(tmp_path / "clone", source_root)
    capture = tmp_path / "docker-arguments.txt"
    fake_bin = _fake_docker(tmp_path, capture, health="unhealthy")

    completed = _run_launcher(clone, fake_bin, "db-up")

    assert completed.returncode == 2
    assert "timed out waiting for isolated db health" in completed.stderr
    calls = _captured_docker_calls(capture)
    assert calls[0][-4:] == ["up", "--detach", "--no-deps", "db"]
    assert calls[1][-3:] == ["ps", "--quiet", "db"]
    assert len([call for call in calls if call[0] == "inspect"]) == 31
    assert all(call[-1] == "0" * 64 for call in calls if call[0] == "inspect")


def test_experiment_launcher_restores_only_the_validated_current_generation(tmp_path) -> None:
    source_root = Path(__file__).parents[2]
    clone = _launcher_clone(tmp_path / "clone", source_root)
    identity = _compose_identity(clone)
    generation_id = _write_current_generation(clone)
    capture = tmp_path / "docker-arguments.txt"
    fake_bin = _fake_docker(tmp_path, capture)

    restored = _run_launcher(clone, fake_bin, "db-restore")
    assert restored.returncode == 0
    restore_calls = _captured_docker_calls(capture)
    assert restore_calls[-4] == _expected_compose_prefix(clone, identity) + ["ps", "--quiet", "db"]
    assert restore_calls[-3] == ["inspect", "--format", "{{json .}}", "0" * 64]
    assert restore_calls[-2] == ["inspect", "--format", "{{if .State.Health}}{{.State.Health.Status}}{{else}}missing{{end}}", "0" * 64]
    assert _captured_docker_arguments(capture) == _expected_compose_prefix(clone, identity) + [
        "exec", "-T", "-e", "PGPASSWORD=experiment-only-password", "db",
        "pg_restore", "--exit-on-error", "--clean", "--if-exists", "--no-owner", "--no-privileges",
        "--host=127.0.0.1", "--port=5432", "--username=bot", "--dbname=telegram_bot_bl21_experiment",
        f"/bl21-snapshot/generations/{generation_id}/snapshot.pgdump",
    ]

    generation = clone / ".data-experiment/snapshots/bl21-local/generations" / generation_id
    (generation / "snapshot.pgdump").write_bytes(b"tampered")
    rejected_digest = _run_launcher(clone, fake_bin, "db-restore")
    assert rejected_digest.returncode == 2
    assert "snapshot dump SHA-256 does not match manifest" in rejected_digest.stderr
    assert len(_captured_docker_calls(capture)) == len(restore_calls)

    _write_current_generation(clone, generation_id=generation_id)
    (generation / "manifest.json").write_text("{}", encoding="utf-8")
    rejected = _run_launcher(clone, fake_bin, "db-restore")
    assert rejected.returncode == 2
    assert "snapshot manifest has an invalid schema" in rejected.stderr
    assert len(_captured_docker_calls(capture)) == len(restore_calls)


def test_experiment_launcher_exports_a_private_generation_then_switches_current(tmp_path) -> None:
    source_root = Path(__file__).parents[2]
    clone = _launcher_clone(tmp_path / "clone", source_root)
    identity = _compose_identity(clone)
    capture = tmp_path / "docker-arguments.txt"
    fake_bin = _fake_docker(tmp_path, capture)

    exported = _run_launcher(clone, fake_bin, "snapshot-export")

    assert exported.returncode == 0, exported.stderr
    snapshot_dir = clone / ".data-experiment/snapshots/bl21-local"
    generation_id = (snapshot_dir / "current").read_text(encoding="ascii").strip()
    assert re.fullmatch(r"g-[A-Za-z0-9]{16}", generation_id)
    generation = snapshot_dir / "generations" / generation_id
    dump = generation / "snapshot.pgdump"
    manifest = json.loads((generation / "manifest.json").read_text(encoding="utf-8"))
    assert stat.S_IMODE(dump.stat().st_mode) == 0o600
    assert stat.S_IMODE((generation / "manifest.json").stat().st_mode) == 0o600
    assert stat.S_IMODE((snapshot_dir / "current").stat().st_mode) == 0o600
    assert all(stat.S_IMODE(path.stat().st_mode) == 0o700 for path in (
        clone / ".data-experiment", clone / ".data-experiment/snapshots", snapshot_dir, snapshot_dir / "generations", generation,
    ))
    assert manifest == {
        "schema_version": 1,
        "content_free": True,
        "snapshot": {
            "format": "pg_dump_custom",
            "path": "snapshot.pgdump",
            "bytes": len(dump.read_bytes()),
            "sha256": hashlib.sha256(dump.read_bytes()).hexdigest(),
        },
        "target": {"service": "db", "host": "127.0.0.1", "port": 5432, "database": "telegram_bot_bl21_experiment", "user": "bot", "marker": "bl21"},
        "postgresql": {"server_version_num": "160001"},
        "schema": {"alembic_version": "0016_experiment_control_plane"},
        "table_counts": {"alembic_version": 1, "posts": 2},
    }
    calls = _captured_docker_calls(capture)
    assert calls[0] == _expected_compose_prefix(clone, identity) + ["ps", "--quiet", "db"]
    assert calls[1] == [
        "inspect", "--format", "{{json .}}", "0" * 64,
    ]
    assert calls[2] == [
        "inspect", "--format", "{{if .State.Health}}{{.State.Health.Status}}{{else}}missing{{end}}", "0" * 64,
    ]
    assert calls[3] == _expected_compose_prefix(clone, identity) + [
        "exec", "-T", "-e", "PGPASSWORD=experiment-only-password", "db", "pg_dump",
        "--format=custom", "--compress=6", "--no-owner", "--no-privileges", "--host=127.0.0.1",
        "--port=5432", "--username=bot", "--dbname=telegram_bot_bl21_experiment",
    ]
    assert all("app" not in call and "pg_restore" not in call for call in calls)


def test_experiment_launcher_rejects_mismatched_isolated_db_identity_before_export(tmp_path) -> None:
    source_root = Path(__file__).parents[2]
    clone = _launcher_clone(tmp_path / "clone", source_root)
    identity = _compose_identity(clone)
    capture = tmp_path / "docker-arguments.txt"
    metadata = _db_identity_metadata(clone, identity)
    metadata["Config"]["Labels"]["com.docker.compose.project"] = "wrong-project"  # type: ignore[index]
    fake_bin = _fake_docker(tmp_path, capture, inspect_metadata=metadata)

    rejected = _run_launcher(clone, fake_bin, "snapshot-export")

    assert rejected.returncode == 2
    assert "isolated db project, service, image, container, or mount identity is invalid" in rejected.stderr
    assert not (clone / ".data-experiment/snapshots/bl21-local/current").exists()


def test_experiment_launcher_failure_leaves_current_generation_untouched(tmp_path) -> None:
    source_root = Path(__file__).parents[2]
    clone = _launcher_clone(tmp_path / "clone", source_root)
    capture = tmp_path / "docker-arguments.txt"
    assert _run_launcher(clone, _fake_docker(tmp_path / "initial", capture, dump_payload="old dump\n"), "snapshot-export").returncode == 0
    snapshot_dir = clone / ".data-experiment/snapshots/bl21-local"
    previous_current = (snapshot_dir / "current").read_bytes()
    previous_generation = snapshot_dir / "generations" / previous_current.decode().strip()
    previous_dump = (previous_generation / "snapshot.pgdump").read_bytes()
    fake_bin = _fake_docker(tmp_path / "failure", capture, dump_failure=True)

    failed = _run_launcher(clone, fake_bin, "snapshot-export")

    assert failed.returncode == 2
    assert "isolated db logical dump failed" in failed.stderr
    assert (snapshot_dir / "current").read_bytes() == previous_current
    assert (previous_generation / "snapshot.pgdump").read_bytes() == previous_dump
    assert any(path.name != previous_generation.name for path in (snapshot_dir / "generations").iterdir())
    assert _run_snapshot_validator(clone, "--current").stdout.strip() == previous_generation.name


def test_experiment_launcher_switches_current_without_overwriting_prior_generation(tmp_path) -> None:
    source_root = Path(__file__).parents[2]
    clone = _launcher_clone(tmp_path / "clone", source_root)
    capture = tmp_path / "docker-arguments.txt"
    snapshot_dir = clone / ".data-experiment/snapshots/bl21-local"
    assert _run_launcher(clone, _fake_docker(tmp_path / "old", capture, dump_payload="old dump\n"), "snapshot-export").returncode == 0
    old_generation = (snapshot_dir / "current").read_text(encoding="ascii").strip()
    old_dump = (snapshot_dir / "generations" / old_generation / "snapshot.pgdump").read_bytes()

    switched = _run_launcher(clone, _fake_docker(tmp_path / "new", capture, dump_payload="new dump\n"), "snapshot-export")

    assert switched.returncode == 0
    new_generation = (snapshot_dir / "current").read_text(encoding="ascii").strip()
    assert new_generation != old_generation
    assert (snapshot_dir / "generations" / old_generation / "snapshot.pgdump").read_bytes() == old_dump
    assert (snapshot_dir / "generations" / new_generation / "snapshot.pgdump").read_bytes() == b"new dump\n"
    assert not list(snapshot_dir.glob(".current.*"))
    assert _run_snapshot_validator(clone, "--current").stdout.strip() == new_generation


@pytest.mark.parametrize("boundary, switched", [
    ("after_generation", False), ("after_dump", False), ("after_manifest", False),
    ("after_validation", False), ("before_switch", False), ("after_switch", True),
])
def test_experiment_launcher_sigkill_boundaries_preserve_a_valid_current_generation(tmp_path, boundary: str, switched: bool) -> None:
    source_root = Path(__file__).parents[2]
    clone = _launcher_clone(tmp_path / boundary / "clone", source_root)
    capture = tmp_path / boundary / "docker-arguments.txt"
    assert _run_launcher(clone, _fake_docker(tmp_path / boundary / "old", capture, dump_payload="old dump\n"), "snapshot-export").returncode == 0
    snapshot_dir = clone / ".data-experiment/snapshots/bl21-local"
    old_current = (snapshot_dir / "current").read_text(encoding="ascii")
    _inject_sigkill_boundary(clone, boundary)

    crashed = _run_launcher(clone, _fake_docker(tmp_path / boundary / "new", capture, dump_payload="new dump\n"), "snapshot-export")

    assert crashed.returncode == -9
    current = (snapshot_dir / "current").read_text(encoding="ascii")
    assert (current != old_current) if switched else (current == old_current)
    selected = snapshot_dir / "generations" / current.strip()
    assert (selected / "snapshot.pgdump").read_bytes() == (b"new dump\n" if switched else b"old dump\n")
    assert _run_snapshot_validator(clone, "--current").returncode == 0


def test_experiment_launcher_rejects_malformed_pointer_generations_and_concurrent_export(tmp_path) -> None:
    source_root = Path(__file__).parents[2]
    clone = _launcher_clone(tmp_path / "clone", source_root)
    snapshot_dir = clone / ".data-experiment/snapshots/bl21-local"
    _write_current_generation(clone)
    capture = tmp_path / "docker-arguments.txt"
    fake_bin = _fake_docker(tmp_path, capture)
    calls_before = 0

    (snapshot_dir / "current").write_text("../escape\n", encoding="ascii")
    rejected_pointer = _run_launcher(clone, fake_bin, "db-restore")
    assert rejected_pointer.returncode == 2
    assert "current snapshot generation is invalid" in rejected_pointer.stderr
    assert (len(_captured_docker_calls(capture)) if capture.exists() else 0) == calls_before

    _write_current_generation(clone)
    (snapshot_dir / "current").unlink()
    (snapshot_dir / "current").symlink_to("generations/g-AAAAAAAAAAAAAAAA")
    rejected_symlink_pointer = _run_launcher(clone, fake_bin, "db-restore")
    assert rejected_symlink_pointer.returncode == 2
    assert (len(_captured_docker_calls(capture)) if capture.exists() else 0) == calls_before

    (snapshot_dir / "current").unlink()
    _write_current_generation(clone)
    current = (snapshot_dir / "current").read_text(encoding="ascii").strip()
    generation = snapshot_dir / "generations" / current
    (generation / "snapshot.pgdump").unlink()
    (generation / "snapshot.pgdump").symlink_to("/etc/passwd")
    rejected_generation = _run_launcher(clone, fake_bin, "db-restore")
    assert rejected_generation.returncode == 2
    assert (len(_captured_docker_calls(capture)) if capture.exists() else 0) == calls_before

    _write_current_generation(clone)
    lock = snapshot_dir / ".export.lock"
    with lock.open("w", encoding="ascii") as lock_file:
        lock.chmod(0o600)
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        concurrent = _run_launcher(clone, fake_bin, "snapshot-export")
    assert concurrent.returncode == 2
    assert "snapshot export is already running" in concurrent.stderr
    assert (len(_captured_docker_calls(capture)) if capture.exists() else 0) == calls_before


def test_experiment_launcher_rejects_adversarial_db_metadata_before_export(tmp_path) -> None:
    source_root = Path(__file__).parents[2]
    mutations = {
        "wrong_service": lambda metadata: metadata["Config"]["Labels"].__setitem__("com.docker.compose.service", "app"),
        "wrong_volume": lambda metadata: metadata["Mounts"][0].__setitem__("Name", "wrong-pgdata"),
        "pgdata_bind_mount": lambda metadata: metadata["Mounts"][0].update({"Type": "bind", "Name": "", "Source": "/tmp/unsafe"}),
        "unexpected_snapshot_bind": lambda metadata: metadata["Mounts"][1].__setitem__("Source", "/tmp/unsafe"),
        "extra_named_mount": lambda metadata: metadata["Mounts"].append({"Type": "volume", "Name": "extra", "Source": "/var/lib/docker/volumes/extra/_data", "Destination": "/extra", "RW": True}),
        "extra_bind_mount": lambda metadata: metadata["Mounts"].append({"Type": "bind", "Name": "", "Source": "/tmp/unsafe", "Destination": "/unsafe", "RW": False}),
        "altered_image": lambda metadata: metadata["Config"].__setitem__("Image", "postgres:17"),
        "altered_container": lambda metadata: metadata.__setitem__("Name", "/unrelated-db-1"),
    }

    for name, mutate in mutations.items():
        clone = _launcher_clone(tmp_path / name / "clone", source_root)
        identity = _compose_identity(clone)
        metadata = _db_identity_metadata(clone, identity)
        mutate(metadata)  # type: ignore[arg-type]
        capture = tmp_path / name / "docker-arguments.txt"
        fake_bin = _fake_docker(tmp_path / name, capture, inspect_metadata=metadata)

        rejected = _run_launcher(clone, fake_bin, "snapshot-export")

        assert rejected.returncode == 2, name
        assert "isolated db project, service, image, container, or mount identity is invalid" in rejected.stderr
        assert not (clone / ".data-experiment/snapshots/bl21-local/source.pgdump").exists()


def test_experiment_launcher_rejects_non_feature_clone_before_one_off_command(tmp_path) -> None:
    source_root = Path(__file__).parents[2]
    clone = _launcher_clone(tmp_path / "clone", source_root)
    capture = tmp_path / "docker-arguments.txt"
    fake_bin = _fake_docker(tmp_path, capture)
    _git(clone, "checkout", "--quiet", "-b", "main")

    completed = _run_launcher(clone, fake_bin, "migrate")

    assert completed.returncode == 2
    assert "must be on the BL-21 feature branch" in completed.stderr
    assert not capture.exists()


@pytest.mark.parametrize("arguments", [
    ("up",),
    ("down",),
    ("ps",),
    ("logs",),
    ("build-app", "--no-cache"),
    ("db-up", "--build"),
    ("db-up", "app"),
    ("db-restore", "/tmp/unsafe.pgdump"),
    ("snapshot-export", "--database", "source"),
    ("migrate", "--detach"),
    ("evaluate", "--experiment-root", "/app"),
    ("evaluate", "--", "--experiment-root", "/app", "--database-url", "postgresql+asyncpg://bot:experiment-only-password@db:5432/telegram_bot_bl21_experiment?experiment=bl21", "--dataset", "/app/.data-experiment/inputs/turboproject-ai-2025-2026.jsonl", "--channel", "turboproject_ai", "--campaign-id", "bl21_smoke", "--vector", "--dry-run"),
    ("evaluate", "--", "--experiment-root", "/app", "--database-url", "postgresql+asyncpg://bot:experiment-only-password@db:5432/telegram_bot_bl21_experiment?experiment=bl21", "--dataset", "/app/.data-experiment/inputs/turboproject-ai-2025-2026.jsonl", "--channel", "turboproject_ai;touch-pwned", "--campaign-id", "bl21_smoke", "--dry-run"),
])
def test_experiment_launcher_rejects_arbitrary_compose_and_runner_injection(tmp_path, arguments: tuple[str, ...]) -> None:
    source_root = Path(__file__).parents[2]
    clone = _launcher_clone(tmp_path / "clone", source_root)
    capture = tmp_path / "docker-arguments.txt"
    fake_bin = _fake_docker(tmp_path, capture)

    completed = _run_launcher(clone, fake_bin, *arguments)

    assert completed.returncode == 2
    assert not capture.exists()
    assert "Usage:" in completed.stderr


def _feature_clone(parent: Path) -> Path:
    parent.mkdir(parents=True, exist_ok=True)
    source = parent / "source"
    clone = parent / "clone"
    source.mkdir()
    _git(source, "init")
    _git(source, "checkout", "-b", FEATURE_BRANCH)
    _git(source, "-c", "user.name=BL21 Test", "-c", "user.email=bl21@example.invalid", "commit", "--quiet", "--allow-empty", "-m", "feature fixture")
    subprocess.run(["git", "clone", "--quiet", "--local", str(source), str(clone)], check=True)
    return clone


def _launcher_clone(path: Path, source_root: Path) -> Path:
    (path / "docker").mkdir(parents=True)
    socket_path = _test_socket_path(path)
    for relative_path in ("docker-compose.yml", "docker-compose.experiment.yml", "docker/bl21-experiment-compose.sh", "docker/bl21-validate-db-identity.py", "docker/bl21-validate-snapshot.py", "docker/bl21-write-snapshot-manifest.py", "docker/bl21-switch-current-generation.py"):
        destination = path / relative_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_root / relative_path, destination)
    launcher = path / "docker/bl21-experiment-compose.sh"
    launcher.write_text(
        launcher.read_text(encoding="utf-8")
        .replace("readonly LOCAL_DOCKER_SOCKET='/var/run/docker.sock'", f"readonly LOCAL_DOCKER_SOCKET='{socket_path}'")
        .replace("[[ \"$owner\" != '0' ]]", f"[[ \"$owner\" != '{os.getuid()}' ]]"),
        encoding="utf-8",
    )
    _git(path, "init", "--quiet")
    _git(path, "checkout", "--quiet", "-b", FEATURE_BRANCH)
    _git(path, "add", ".")
    _git(path, "-c", "user.name=BL21 Test", "-c", "user.email=bl21@example.invalid", "commit", "--quiet", "-m", "launcher fixture")
    return path


def _compose_identity(root: Path) -> dict[str, str]:
    completed = subprocess.run(
        ["bash", str(root / "docker/bl21-experiment-compose.sh"), "identity"],
        check=True,
        capture_output=True,
        text=True,
        cwd=root,
    )
    return dict(line.split("=", maxsplit=1) for line in completed.stdout.splitlines())


def _fake_docker(
    tmp_path: Path,
    capture: Path,
    *,
    health: str = "healthy",
    inspect_metadata: dict[str, object] | None = None,
    dump_failure: bool = False,
    dump_payload: str = "BL21 custom dump\n",
) -> Path:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir(parents=True)
    docker = fake_bin / "docker"
    metadata_line = (
        f"metadata={shlex.quote(json.dumps(inspect_metadata, separators=(',', ':')))}\n"
        if inspect_metadata is not None
        else """metadata="$(printf '{\"Id\":\"%064d\",\"Name\":\"/%s-db-1\",\"Config\":{\"Image\":\"postgres:16\",\"Labels\":{\"com.docker.compose.project\":\"%s\",\"com.docker.compose.service\":\"db\"}},\"Mounts\":[{\"Type\":\"volume\",\"Name\":\"%s\",\"Source\":\"/var/lib/docker/volumes/%s/_data\",\"Destination\":\"/var/lib/postgresql/data\",\"RW\":true},{\"Type\":\"bind\",\"Name\":\"\",\"Source\":\"%s/.data-experiment/snapshots/bl21-local\",\"Destination\":\"/bl21-snapshot\",\"RW\":false}]}' 0 \"$BL21_COMPOSE_PROJECT_NAME\" \"$BL21_COMPOSE_PROJECT_NAME\" \"$BL21_EXPERIMENT_PGDATA_VOLUME\" \"$BL21_EXPERIMENT_PGDATA_VOLUME\" \"$PWD\")"
"""
    )
    dump_line = "  *\" pg_dump \"*) exit 1 ;;\n" if dump_failure else f"  *\" pg_dump \"*) printf %s {shlex.quote(dump_payload)} ;;\n"
    docker.write_text(
        f"""#!/bin/sh
{{
  printf '__ARG__%s\\n' "$@"
  printf 'PROJECT=%s\\n' "$BL21_COMPOSE_PROJECT_NAME"
  printf 'VOLUME=%s\\n' "$BL21_EXPERIMENT_PGDATA_VOLUME"
  printf 'LEAK=%s\\n' "${{BL21_TEST_LEAK-unset}}"
  printf 'DOCKER_HOST=%s\\n' "$DOCKER_HOST"
  printf 'DOCKER_CONTEXT=%s\\n' "$DOCKER_CONTEXT"
  printf 'HOME=%s\\n' "$HOME"
  printf 'DOCKER_CONFIG=%s\\n' "$DOCKER_CONFIG"
  printf '__CALL_END__\\n'
}} >> {shlex.quote(str(capture))}
 {metadata_line}case " $* " in
   *" ps --quiet db "*) printf '%064d\\n' 0 ;;
   *"{{json .}}"*) printf '%s\\n' "$metadata" ;;
  *" inspect "*) printf '{health}\\n' ;;
{dump_line}  *"SHOW server_version_num"*) printf '160001\\n' ;;
  *"SELECT version_num FROM alembic_version"*) printf '0016_experiment_control_plane\\n' ;;
  *"SELECT tablename FROM pg_catalog.pg_tables"*) printf 'alembic_version\\nposts\\n' ;;
  *"SELECT count(*) FROM public.\\"alembic_version\\""*) printf '1\\n' ;;
  *"SELECT count(*) FROM public.\\"posts\\""*) printf '2\\n' ;;
esac
""",
        encoding="utf-8",
    )
    docker.chmod(0o755)
    (fake_bin / "sleep").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    (fake_bin / "sleep").chmod(0o755)
    return fake_bin


def _db_identity_metadata(root: Path, identity: dict[str, str]) -> dict[str, object]:
    project = identity["BL21_COMPOSE_PROJECT_NAME"]
    volume = identity["BL21_EXPERIMENT_PGDATA_VOLUME"]
    return {
        "Id": "0" * 64,
        "Name": f"/{project}-db-1",
        "Config": {
            "Image": "postgres:16",
            "Labels": {
                "com.docker.compose.project": project,
                "com.docker.compose.service": "db",
            },
        },
        "Mounts": [
            {
                "Type": "volume",
                "Name": volume,
                "Source": f"/var/lib/docker/volumes/{volume}/_data",
                "Destination": "/var/lib/postgresql/data",
                "RW": True,
            },
            {
                "Type": "bind",
                "Name": "",
                "Source": str(root / ".data-experiment/snapshots/bl21-local"),
                "Destination": "/bl21-snapshot",
                "RW": False,
            },
        ],
    }


def _run_launcher(root: Path, fake_bin: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
    environment = os.environ | {
        "PATH": f"{fake_bin}:{os.environ['PATH']}", "BL21_TEST_LEAK": "must-not-reach-docker",
        "DOCKER_HOST": "tcp://caller-must-not-leak", "DOCKER_CONTEXT": "caller-must-not-leak", "HOME": "/caller-must-not-leak",
    }
    socket_path = _test_socket_path(root)
    test_socket = socket.socket(socket.AF_UNIX)
    test_socket.bind(str(socket_path))
    socket_path.chmod(0o660)
    try:
        return subprocess.run(
            ["bash", str(root / "docker/bl21-experiment-compose.sh"), *arguments],
            capture_output=True,
            text=True,
            cwd=root,
            env=environment,
        )
    finally:
        test_socket.close()
        socket_path.unlink(missing_ok=True)


def _captured_docker_arguments(capture: Path) -> list[str]:
    return _captured_docker_calls(capture)[-1]


def _captured_docker_calls(capture: Path) -> list[list[str]]:
    calls: list[list[str]] = []
    arguments: list[str] = []
    for line in capture.read_text(encoding="utf-8").splitlines():
        if line == "__CALL_END__":
            calls.append(arguments)
            arguments = []
        elif line.startswith("__ARG__"):
            arguments.append(line.removeprefix("__ARG__"))
    return calls


def _write_current_generation(clone: Path, *, generation_id: str = "g-AAAAAAAAAAAAAAAA") -> str:
    snapshot_dir = clone / ".data-experiment/snapshots/bl21-local"
    generations = snapshot_dir / "generations"
    generations.mkdir(parents=True, exist_ok=True)
    for directory in (clone / ".data-experiment", clone / ".data-experiment/snapshots", snapshot_dir, generations):
        directory.chmod(0o700)
    generation = generations / generation_id
    generation.mkdir(exist_ok=True)
    generation.chmod(0o700)
    dump = b"BL21 local test snapshot only\n"
    digest = hashlib.sha256(dump).hexdigest()
    dump_path = generation / "snapshot.pgdump"
    manifest_path = generation / "manifest.json"
    dump_path.unlink(missing_ok=True)
    manifest_path.unlink(missing_ok=True)
    dump_path.write_bytes(dump)
    dump_path.chmod(0o600)
    manifest_path.write_text(json.dumps({
        "schema_version": 1,
        "content_free": True,
        "snapshot": {"format": "pg_dump_custom", "path": "snapshot.pgdump", "bytes": len(dump), "sha256": digest},
        "target": {"service": "db", "host": "127.0.0.1", "port": 5432, "database": "telegram_bot_bl21_experiment", "user": "bot", "marker": "bl21"},
        "postgresql": {"server_version_num": "160001"},
        "schema": {"alembic_version": "0016_experiment_control_plane"},
        "table_counts": {"alembic_version": 1, "posts": 2},
    }, sort_keys=True), encoding="utf-8")
    manifest_path.chmod(0o600)
    current = snapshot_dir / "current"
    current.write_text(f"{generation_id}\n", encoding="ascii")
    current.chmod(0o600)
    return generation_id


def _run_snapshot_validator(clone: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["python3", str(clone / "docker/bl21-validate-snapshot.py"), *arguments],
        capture_output=True,
        text=True,
        cwd=clone,
    )


def _inject_sigkill_boundary(clone: Path, boundary: str) -> None:
    if boundary == "after_generation":
        path = clone / "docker/bl21-experiment-compose.sh"
        original = '  chmod 700 "$generation_dir"\n'
        replacement = original + '  kill -KILL "$$"\n'
    elif boundary == "after_dump":
        path = clone / "docker/bl21-experiment-compose.sh"
        original = '  [[ -s "$temporary_dump" ]] || die \'isolated db logical dump is empty\'\n  chmod 600 "$temporary_dump"\n'
        replacement = original + '  kill -KILL "$$"\n'
    elif boundary == "after_manifest":
        path = clone / "docker/bl21-write-snapshot-manifest.py"
        original = '        os.replace(temporary, generation / "manifest.json")\n'
        replacement = original + '        os.kill(os.getppid(), 9)\n'
    elif boundary == "after_validation":
        path = clone / "docker/bl21-validate-snapshot.py"
        original = '            _validate_generation(arguments[1])\n'
        replacement = original + '            os.kill(os.getppid(), 9)\n'
    elif boundary == "before_switch":
        path = clone / "docker/bl21-experiment-compose.sh"
        original = '  python3 "$SNAPSHOT_POINTER_SWITCHER" --generation "$generation_id" || die \'isolated db snapshot current-pointer switch failed\'\n'
        replacement = '  kill -KILL "$$"\n' + original
    elif boundary == "after_switch":
        path = clone / "docker/bl21-experiment-compose.sh"
        original = '  python3 "$SNAPSHOT_POINTER_SWITCHER" --generation "$generation_id" || die \'isolated db snapshot current-pointer switch failed\'\n'
        replacement = original + '  kill -KILL "$$"\n'
    else:
        raise AssertionError(f"unknown SIGKILL boundary: {boundary}")
    contents = path.read_text(encoding="utf-8")
    assert original in contents
    path.write_text(contents.replace(original, replacement, 1), encoding="utf-8")


def _test_socket_path(path: Path) -> Path:
    return Path("/tmp") / f"bl21-{hashlib.sha256(str(path).encode()).hexdigest()[:20]}.sock"


def _expected_compose_prefix(root: Path, identity: dict[str, str]) -> list[str]:
    return [
        "compose",
        "--project-name", identity["BL21_COMPOSE_PROJECT_NAME"],
        "--file", str(root / "docker-compose.yml"),
        "--file", str(root / "docker-compose.experiment.yml"),
    ]


def _expected_one_off_git_metadata(root: Path) -> list[str]:
    revision = subprocess.run(
        ["git", "-C", str(root), "rev-parse", "--verify", "HEAD^{commit}"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    return [
        "--env", f"BL21_EXPERIMENT_GIT_BRANCH={FEATURE_BRANCH}",
        "--env", f"BL21_EXPERIMENT_GIT_REVISION={revision}",
    ]


def _git(directory: Path, *arguments: str) -> None:
    subprocess.run(["git", "-C", str(directory), *arguments], check=True, capture_output=True, text=True)
