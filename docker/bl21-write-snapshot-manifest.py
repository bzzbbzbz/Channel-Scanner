#!/usr/bin/env python3
"""Atomically write the fixed content-free BL-21 export manifest."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
SNAPSHOT_DIR = ROOT / ".data-experiment/snapshots/bl21-local"
SNAPSHOT_DUMP = SNAPSHOT_DIR / "source.pgdump"
SNAPSHOT_MANIFEST = SNAPSHOT_DIR / "source-manifest.json"
TARGET = {
    "service": "db",
    "host": "127.0.0.1",
    "port": 5432,
    "database": "telegram_bot_bl21_experiment",
    "user": "bot",
    "marker": "bl21",
}
POSTGRES_VERSION = re.compile(r"^[0-9]{6}$")
ALEMBIC_VERSION = re.compile(r"^[a-z0-9][a-z0-9_]{0,127}$")
TABLE_NAME = re.compile(r"^[a-z][a-z0-9_]{0,62}$")


class ManifestWriteError(ValueError):
    """The fixed manifest cannot be safely produced."""


def _require_private_directory(path: Path) -> None:
    if path.is_symlink() or not path.is_dir():
        raise ManifestWriteError("snapshot directory is unsafe")
    if stat.S_IMODE(path.stat().st_mode) != 0o700:
        raise ManifestWriteError("snapshot directory permissions are unsafe")


def _read_dump() -> bytes:
    try:
        descriptor = os.open(SNAPSHOT_DUMP, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
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
    raw_counts = os.environ.get("BL21_TABLE_COUNTS", "")
    if not POSTGRES_VERSION.fullmatch(postgres_version):
        raise ManifestWriteError("PostgreSQL version is invalid")
    if not ALEMBIC_VERSION.fullmatch(alembic_version):
        raise ManifestWriteError("Alembic version is invalid")
    try:
        table_counts = json.loads(raw_counts)
    except json.JSONDecodeError as exc:
        raise ManifestWriteError("snapshot table counts are invalid") from exc
    if (
        not isinstance(table_counts, dict)
        or not table_counts
        or any(
            not isinstance(name, str)
            or not TABLE_NAME.fullmatch(name)
            or not isinstance(count, int)
            or isinstance(count, bool)
            or count < 0
            for name, count in table_counts.items()
        )
    ):
        raise ManifestWriteError("snapshot table counts are invalid")
    if list(table_counts) != sorted(table_counts):
        raise ManifestWriteError("snapshot table counts are not ordered")
    return postgres_version, alembic_version, table_counts


def write_manifest() -> None:
    """Write the exact export manifest without accepting caller paths or targets."""
    for directory in (ROOT / ".data-experiment", ROOT / ".data-experiment/snapshots", SNAPSHOT_DIR):
        _require_private_directory(directory)
    dump = _read_dump()
    postgres_version, alembic_version, table_counts = _metadata()
    manifest = {
        "schema_version": 1,
        "content_free": True,
        "snapshot": {
            "format": "pg_dump_custom",
            "path": "source.pgdump",
            "bytes": len(dump),
            "sha256": hashlib.sha256(dump).hexdigest(),
        },
        "target": TARGET,
        "postgresql": {"server_version_num": postgres_version},
        "schema": {"alembic_version": alembic_version},
        "table_counts": table_counts,
    }
    descriptor, temporary_path = tempfile.mkstemp(prefix=".source-manifest.", dir=SNAPSHOT_DIR)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as manifest_file:
            descriptor = -1
            json.dump(manifest, manifest_file, sort_keys=True, separators=(",", ":"))
            manifest_file.write("\n")
            manifest_file.flush()
            os.fsync(manifest_file.fileno())
        os.replace(temporary_path, SNAPSHOT_MANIFEST)
        os.chmod(SNAPSHOT_MANIFEST, 0o600)
    finally:
        if descriptor != -1:
            os.close(descriptor)
        Path(temporary_path).unlink(missing_ok=True)


def main() -> int:
    try:
        write_manifest()
    except ManifestWriteError as exc:
        print(f"error: {exc}", file=os.sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
