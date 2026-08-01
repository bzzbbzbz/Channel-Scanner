#!/usr/bin/env python3
"""Atomically write a content-free manifest inside one BL-21 generation."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
SNAPSHOT_ROOT = ROOT / ".data-experiment/snapshots/bl21-local"
GENERATIONS = SNAPSHOT_ROOT / "generations"
GENERATION_ID = re.compile(r"^g-[A-Za-z0-9]{16}$")
TARGET = {"service": "db", "host": "127.0.0.1", "port": 5432, "database": "telegram_bot_bl21_experiment", "user": "bot", "marker": "bl21"}
POSTGRES_VERSION = re.compile(r"^[0-9]{6}$")
ALEMBIC_VERSION = re.compile(r"^[a-z0-9][a-z0-9_]{0,127}$")
TABLE_NAME = re.compile(r"^[a-z][a-z0-9_]{0,62}$")


class ManifestWriteError(ValueError):
    """The manifest cannot be safely produced."""


def _private_directory(path: Path) -> None:
    if path.is_symlink() or not path.is_dir() or stat.S_IMODE(path.stat().st_mode) != 0o700:
        raise ManifestWriteError("snapshot directory is unsafe")


def _generation(arguments: list[str]) -> Path:
    if len(arguments) != 2 or arguments[0] != "--generation" or not GENERATION_ID.fullmatch(arguments[1]):
        raise ManifestWriteError("manifest writer accepts exactly one generation identifier")
    for directory in (ROOT / ".data-experiment", ROOT / ".data-experiment/snapshots", SNAPSHOT_ROOT, GENERATIONS):
        _private_directory(directory)
    generation = GENERATIONS / arguments[1]
    _private_directory(generation)
    return generation


def _read_dump(path: Path) -> bytes:
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    except OSError as exc:
        raise ManifestWriteError("snapshot dump is unavailable") from exc
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) != 0o600:
            raise ManifestWriteError("snapshot dump permissions are unsafe")
        with os.fdopen(descriptor, "rb") as dump_file:
            descriptor = -1
            dump = dump_file.read()
    finally:
        if descriptor != -1:
            os.close(descriptor)
    if not dump:
        raise ManifestWriteError("snapshot dump is empty")
    return dump


def _metadata() -> tuple[str, str, dict[str, int]]:
    postgres_version = os.environ.get("BL21_POSTGRES_VERSION", "")
    alembic_version = os.environ.get("BL21_ALEMBIC_VERSION", "")
    try:
        table_counts = json.loads(os.environ.get("BL21_TABLE_COUNTS", ""))
    except json.JSONDecodeError as exc:
        raise ManifestWriteError("snapshot table counts are invalid") from exc
    if not POSTGRES_VERSION.fullmatch(postgres_version) or not ALEMBIC_VERSION.fullmatch(alembic_version):
        raise ManifestWriteError("snapshot metadata is invalid")
    if not isinstance(table_counts, dict) or not table_counts or list(table_counts) != sorted(table_counts) or any(
        not isinstance(name, str) or not TABLE_NAME.fullmatch(name) or not isinstance(count, int) or isinstance(count, bool) or count < 0
        for name, count in table_counts.items()
    ):
        raise ManifestWriteError("snapshot table counts are invalid")
    return postgres_version, alembic_version, table_counts


def write_manifest(arguments: list[str]) -> None:
    generation = _generation(arguments)
    dump = _read_dump(generation / "snapshot.pgdump")
    postgres_version, alembic_version, table_counts = _metadata()
    manifest = {"schema_version": 1, "content_free": True, "snapshot": {"format": "pg_dump_custom", "path": "snapshot.pgdump", "bytes": len(dump), "sha256": hashlib.sha256(dump).hexdigest()}, "target": TARGET, "postgresql": {"server_version_num": postgres_version}, "schema": {"alembic_version": alembic_version}, "table_counts": table_counts}
    descriptor, temporary_name = tempfile.mkstemp(prefix=".manifest.", dir=generation)
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as manifest_file:
            descriptor = -1
            json.dump(manifest, manifest_file, sort_keys=True, separators=(",", ":"))
            manifest_file.write("\n")
            manifest_file.flush()
            os.fsync(manifest_file.fileno())
        os.replace(temporary, generation / "manifest.json")
        directory_descriptor = os.open(generation, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    finally:
        if descriptor != -1:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


def main() -> int:
    try:
        write_manifest(sys.argv[1:])
    except ManifestWriteError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
