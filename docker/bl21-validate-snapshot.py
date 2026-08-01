#!/usr/bin/env python3
"""Fail-closed validator for versioned clone-local BL-21 snapshots."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
SNAPSHOT_ROOT = Path(".data-experiment/snapshots/bl21-local")
GENERATIONS = SNAPSHOT_ROOT / "generations"
CURRENT = SNAPSHOT_ROOT / "current"
SHA256_LENGTH = 64
GENERATION_ID = re.compile(r"^g-[A-Za-z0-9]{16}$")
TARGET = {
    "service": "db",
    "host": "127.0.0.1",
    "port": 5432,
    "database": "telegram_bot_bl21_experiment",
    "user": "bot",
    "marker": "bl21",
}
SAFE_TABLE_NAME = re.compile(r"^[a-z][a-z0-9_]{0,62}$")


class SnapshotValidationError(ValueError):
    """A snapshot generation cannot safely be selected or restored."""


def _private_directory(relative_path: Path) -> Path:
    path = ROOT / relative_path
    current = ROOT
    for component in relative_path.parts:
        current /= component
        if current.is_symlink() or not current.is_dir() or stat.S_IMODE(current.stat().st_mode) != 0o700:
            raise SnapshotValidationError("snapshot directory is unsafe")
    return path


def _read_regular(relative_path: Path, *, mode: int = 0o600) -> bytes:
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
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) != mode:
            raise SnapshotValidationError("snapshot input permissions are unsafe")
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


def _integer(value: object, label: str, *, positive: bool) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < (1 if positive else 0):
        raise SnapshotValidationError(f"{label} must be a {'positive' if positive else 'non-negative'} integer")
    return value


def _validate_generation_id(generation_id: str) -> None:
    if not GENERATION_ID.fullmatch(generation_id):
        raise SnapshotValidationError("snapshot generation identifier is invalid")


def _validate_generation(generation_id: str) -> None:
    _validate_generation_id(generation_id)
    _private_directory(Path(".data-experiment"))
    _private_directory(Path(".data-experiment/snapshots"))
    _private_directory(SNAPSHOT_ROOT)
    generation_dir = _private_directory(GENERATIONS / generation_id)
    entries = {entry.name for entry in generation_dir.iterdir()}
    if entries != {"snapshot.pgdump", "manifest.json"}:
        raise SnapshotValidationError("snapshot generation has unexpected files")
    try:
        manifest = json.loads(_read_regular(GENERATIONS / generation_id / "manifest.json"))
    except json.JSONDecodeError as exc:
        raise SnapshotValidationError("snapshot manifest is invalid JSON") from exc
    manifest = _object(manifest, "snapshot manifest")
    _exact_keys(
        manifest,
        {"schema_version", "content_free", "snapshot", "target", "postgresql", "schema", "table_counts"},
        "snapshot manifest",
    )
    if manifest["schema_version"] != 1 or manifest["content_free"] is not True:
        raise SnapshotValidationError("snapshot manifest is not content-free schema version one")
    snapshot = _object(manifest["snapshot"], "snapshot")
    _exact_keys(snapshot, {"format", "path", "bytes", "sha256"}, "snapshot")
    if snapshot["format"] != "pg_dump_custom" or snapshot["path"] != "snapshot.pgdump":
        raise SnapshotValidationError("snapshot format or path is invalid")
    expected_bytes = _integer(snapshot["bytes"], "snapshot bytes", positive=True)
    expected_sha256 = _sha256(snapshot["sha256"], "snapshot sha256")
    if _object(manifest["target"], "snapshot target") != TARGET:
        raise SnapshotValidationError("snapshot target is not the isolated database")
    postgresql = _object(manifest["postgresql"], "snapshot PostgreSQL")
    _exact_keys(postgresql, {"server_version_num"}, "snapshot PostgreSQL")
    if not isinstance(postgresql["server_version_num"], str) or not re.fullmatch(r"[0-9]{6}", postgresql["server_version_num"]):
        raise SnapshotValidationError("snapshot PostgreSQL version is invalid")
    schema = _object(manifest["schema"], "snapshot schema")
    _exact_keys(schema, {"alembic_version"}, "snapshot schema")
    if not isinstance(schema["alembic_version"], str) or not re.fullmatch(r"[a-z0-9][a-z0-9_]{0,127}", schema["alembic_version"]):
        raise SnapshotValidationError("snapshot Alembic version is invalid")
    table_counts = _object(manifest["table_counts"], "snapshot table counts")
    if not table_counts or list(table_counts) != sorted(table_counts):
        raise SnapshotValidationError("snapshot table counts are invalid")
    for table_name, count in table_counts.items():
        if not isinstance(table_name, str) or not SAFE_TABLE_NAME.fullmatch(table_name):
            raise SnapshotValidationError("snapshot table counts are invalid")
        _integer(count, "snapshot table count", positive=False)
    dump = _read_regular(GENERATIONS / generation_id / "snapshot.pgdump")
    if len(dump) != expected_bytes or hashlib.sha256(dump).hexdigest() != expected_sha256:
        raise SnapshotValidationError("snapshot dump SHA-256 does not match manifest")


def _current_generation() -> str:
    _private_directory(Path(".data-experiment"))
    _private_directory(Path(".data-experiment/snapshots"))
    _private_directory(SNAPSHOT_ROOT)
    pointer = _read_regular(CURRENT)
    if not pointer.endswith(b"\n") or pointer.count(b"\n") != 1:
        raise SnapshotValidationError("snapshot current pointer is malformed")
    try:
        generation_id = pointer[:-1].decode("ascii")
    except UnicodeDecodeError as exc:
        raise SnapshotValidationError("snapshot current pointer is malformed") from exc
    _validate_generation(generation_id)
    return generation_id


def main() -> int:
    try:
        arguments = sys.argv[1:]
        if arguments == ["--current"]:
            print(_current_generation())
        elif len(arguments) == 2 and arguments[0] == "--generation":
            _validate_generation(arguments[1])
        else:
            raise SnapshotValidationError("snapshot validator accepts only --current or --generation <id>")
    except SnapshotValidationError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
