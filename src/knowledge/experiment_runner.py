"""Dry-run-only preflight for isolated, content-free BL-21 experiment campaigns."""

from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from sqlalchemy.engine import make_url

from src.knowledge.evaluation import load_dataset
from src.knowledge.experiments import (
    ExperimentError,
    ExperimentPolicy,
    UnsafeExperimentPath,
    config_sha256,
    create_campaign,
    hash_identifier,
    preflight_experiment_dir,
    require_safe_identifier,
    require_sha256,
)


EXPERIMENT_DATABASE_NAME = "telegram_bot_bl21_experiment"
EXPERIMENT_DATABASE_HOST = "db"
EXPERIMENT_DATABASE_MARKER = "bl21"
MANIFEST_RELATIVE_PATH = Path(".data-experiment/clone-manifest.json")


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
    policy_sha256: str

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
            "policy_sha256": self.policy_sha256,
            "dry_run": "true",
        }


def validate_database_url(database_url: str) -> None:
    """Accept only the internally addressed, explicitly labelled clone database."""
    try:
        url = make_url(database_url)
    except Exception as exc:
        raise ExperimentError("database URL is invalid") from exc
    if url.drivername != "postgresql+asyncpg":
        raise ExperimentError("database URL must use the isolated asyncpg PostgreSQL driver")
    if url.host != EXPERIMENT_DATABASE_HOST or url.database != EXPERIMENT_DATABASE_NAME:
        raise ExperimentError("database URL does not name the isolated experiment clone")
    if url.query.get("experiment") != EXPERIMENT_DATABASE_MARKER:
        raise ExperimentError("database URL must include experiment=bl21")


def validate_preflight(
    *,
    experiment_root: Path,
    database_url: str,
    dataset: Path,
    channel: str,
    campaign_key: str,
) -> PreflightEvidence:
    """Validate every immutable input without creating reports or opening a database."""
    preflight_experiment_dir(experiment_root, create=False)
    validate_database_url(database_url)
    require_safe_identifier(campaign_key, "campaign_id")
    if not channel.strip():
        raise ExperimentError("channel must not be empty")
    manifest, manifest_hash = _load_manifest(experiment_root)
    dataset_hash, case_count = _validate_dataset(experiment_root, dataset, manifest)
    snapshot = _mapping(manifest["logical_snapshot"], "logical_snapshot")
    snapshot_hash = require_sha256(snapshot.get("sha256"), "logical_snapshot.sha256")
    if snapshot.get("format") != "pg_dump_custom" or not isinstance(snapshot.get("bytes"), int) or isinstance(snapshot.get("bytes"), bool) or snapshot["bytes"] < 1:
        raise ExperimentError("clone manifest snapshot metadata is invalid")
    table_count = _validate_table_counts(_mapping(manifest["table_counts"], "table_counts"))
    policy = ExperimentPolicy()
    configuration = {
        "runner_schema_version": 1,
        "channel_sha256": hash_identifier(channel.strip().lower()),
        "dataset_sha256": dataset_hash,
        "source_snapshot_sha256": snapshot_hash,
    }
    campaign = create_campaign(campaign_key, configuration, dataset_hash)
    return PreflightEvidence(
        campaign_key=campaign_key,
        channel_sha256=configuration["channel_sha256"],
        dataset_sha256=dataset_hash,
        dataset_case_count=case_count,
        source_snapshot_sha256=snapshot_hash,
        source_snapshot_table_count=table_count,
        manifest_sha256=manifest_hash,
        config_sha256=campaign.config_sha256,
        policy_sha256=config_sha256(policy),
    )


def _load_manifest(experiment_root: Path) -> tuple[Mapping[str, object], str]:
    manifest_path = experiment_root / MANIFEST_RELATIVE_PATH
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise UnsafeExperimentPath("clone manifest must be a regular file")
    raw = manifest_path.read_bytes()
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


def _validate_dataset(experiment_root: Path, dataset: Path, manifest: Mapping[str, object]) -> tuple[str, int]:
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
    raw = dataset.read_bytes()
    if len(raw) != expected_bytes or hashlib.sha256(raw).hexdigest() != expected_hash:
        raise ExperimentError("dataset bytes do not match the immutable clone manifest")
    cases, loaded_hash = load_dataset(dataset)
    if loaded_hash != expected_hash:
        raise ExperimentError("dataset loader hash does not match the immutable clone manifest")
    return loaded_hash, len(cases)


def _validate_table_counts(table_counts: Mapping[str, object]) -> int:
    snapshots = []
    for key in ("source_at_snapshot", "source_post_restore", "test_after_restore"):
        counts = _mapping(table_counts.get(key), key)
        if not counts or any(not isinstance(value, str) or not value.isdecimal() for value in counts.values()):
            raise ExperimentError("clone manifest table counts are invalid")
        snapshots.append(counts)
    if snapshots[0] != snapshots[1] or snapshots[0] != snapshots[2]:
        raise ExperimentError("clone manifest table counts do not match")
    if table_counts.get("table_count") != len(snapshots[0]):
        raise ExperimentError("clone manifest table count is inconsistent")
    if table_counts.get("snapshot_equals_test") is not True or table_counts.get("source_post_restore_equals_test") is not True:
        raise ExperimentError("clone manifest does not certify the isolated restore")
    return len(snapshots[0])


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
    parser.add_argument("--dry-run", action="store_true", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    evidence = validate_preflight(
        experiment_root=args.experiment_root,
        database_url=args.database_url,
        dataset=args.dataset,
        channel=args.channel,
        campaign_key=args.campaign_id,
    )
    print(json.dumps(evidence.record(), sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
