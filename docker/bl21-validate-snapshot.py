#!/usr/bin/env python3
"""Fail-closed validator for the one local BL-21 restore snapshot."""

from __future__ import annotations

import hashlib
import json
import os
import stat
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
SNAPSHOT_DIR = Path(".data-experiment/snapshots/bl21-local")
SNAPSHOT_MANIFEST = SNAPSHOT_DIR / "snapshot-manifest.json"
SNAPSHOT_DUMP = SNAPSHOT_DIR / "snapshot.pgdump"
CLONE_MANIFEST = Path(".data-experiment/clone-manifest.json")
SHA256_LENGTH = 64
TARGET = {
    "service": "db",
    "host": "127.0.0.1",
    "port": 5432,
    "database": "telegram_bot_bl21_experiment",
    "user": "bot",
    "marker": "bl21",
}


class SnapshotValidationError(ValueError):
    """The fixed local snapshot cannot safely be restored."""


def _read_regular(relative_path: Path) -> bytes:
    path = ROOT / relative_path
    current = ROOT
    for component in relative_path.parts:
        current /= component
        if current.is_symlink():
            raise SnapshotValidationError("snapshot path must not contain symlinks")
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    except OSError as exc:
        raise SnapshotValidationError("snapshot input must be a regular file") from exc
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise SnapshotValidationError("snapshot input must be a regular file")
        with os.fdopen(descriptor, "rb") as input_file:
            descriptor = -1
            return input_file.read()
    finally:
        if descriptor != -1:
            os.close(descriptor)


def _object(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise SnapshotValidationError(f"{label} must be an object")
    return value


def _exact_keys(value: dict[str, Any], expected: set[str], label: str) -> None:
    if set(value) != expected:
        raise SnapshotValidationError(f"{label} has an invalid schema")


def _sha256(value: object, label: str) -> str:
    if not isinstance(value, str) or len(value) != SHA256_LENGTH:
        raise SnapshotValidationError(f"{label} is not a SHA-256")
    try:
        int(value, 16)
    except ValueError as exc:
        raise SnapshotValidationError(f"{label} is not a SHA-256") from exc
    return value


def _positive_int(value: object, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise SnapshotValidationError(f"{label} must be a positive integer")
    return value


def validate_snapshot() -> None:
    """Validate the exact local, content-free dump convention before pg_restore."""
    try:
        manifest = json.loads(_read_regular(SNAPSHOT_MANIFEST))
    except json.JSONDecodeError as exc:
        raise SnapshotValidationError("snapshot manifest is invalid JSON") from exc
    manifest = _object(manifest, "snapshot manifest")
    _exact_keys(manifest, {"schema_version", "content_free", "snapshot", "target"}, "snapshot manifest")
    if manifest["schema_version"] != 1 or manifest["content_free"] is not True:
        raise SnapshotValidationError("snapshot manifest is not content-free schema version one")
    snapshot = _object(manifest["snapshot"], "snapshot")
    _exact_keys(snapshot, {"format", "path", "bytes", "sha256"}, "snapshot")
    if snapshot["format"] != "pg_dump_custom" or snapshot["path"] != "snapshot.pgdump":
        raise SnapshotValidationError("snapshot format or path is invalid")
    expected_bytes = _positive_int(snapshot["bytes"], "snapshot bytes")
    expected_sha256 = _sha256(snapshot["sha256"], "snapshot sha256")
    target = _object(manifest["target"], "snapshot target")
    if target != TARGET:
        raise SnapshotValidationError("snapshot target is not the isolated database")

    clone_manifest = _object(json.loads(_read_regular(CLONE_MANIFEST)), "clone manifest")
    clone_snapshot = _object(clone_manifest.get("logical_snapshot"), "clone logical snapshot")
    if clone_snapshot.get("format") != "pg_dump_custom":
        raise SnapshotValidationError("clone snapshot format is invalid")
    if _positive_int(clone_snapshot.get("bytes"), "clone snapshot bytes") != expected_bytes:
        raise SnapshotValidationError("snapshot bytes do not match clone manifest")
    if _sha256(clone_snapshot.get("sha256"), "clone snapshot sha256") != expected_sha256:
        raise SnapshotValidationError("snapshot SHA-256 does not match clone manifest")

    dump = _read_regular(SNAPSHOT_DUMP)
    if len(dump) != expected_bytes or hashlib.sha256(dump).hexdigest() != expected_sha256:
        raise SnapshotValidationError("snapshot dump SHA-256 does not match manifest")


def main() -> int:
    try:
        validate_snapshot()
    except SnapshotValidationError as exc:
        print(f"error: {exc}", file=os.sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
