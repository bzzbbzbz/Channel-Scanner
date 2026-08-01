#!/usr/bin/env python3
"""Atomically select a validated, clone-local BL-21 snapshot generation."""

from __future__ import annotations

import os
import re
import stat
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
SNAPSHOT_ROOT = ROOT / ".data-experiment/snapshots/bl21-local"
CURRENT = SNAPSHOT_ROOT / "current"
GENERATION_ID = re.compile(r"^g-[A-Za-z0-9]{16}$")


class PointerSwitchError(ValueError):
    """The current pointer cannot be safely replaced."""


def _private_directory(path: Path) -> None:
    if path.is_symlink() or not path.is_dir() or stat.S_IMODE(path.stat().st_mode) != 0o700:
        raise PointerSwitchError("snapshot pointer directory is unsafe")


def switch_current(generation_id: str) -> None:
    if not GENERATION_ID.fullmatch(generation_id):
        raise PointerSwitchError("snapshot generation identifier is invalid")
    _private_directory(ROOT / ".data-experiment")
    _private_directory(ROOT / ".data-experiment/snapshots")
    _private_directory(SNAPSHOT_ROOT)
    if CURRENT.is_symlink() or (CURRENT.exists() and not CURRENT.is_file()):
        raise PointerSwitchError("snapshot current pointer is unsafe")
    if CURRENT.exists() and stat.S_IMODE(CURRENT.stat().st_mode) != 0o600:
        raise PointerSwitchError("snapshot current pointer permissions are unsafe")
    descriptor, temporary_name = tempfile.mkstemp(prefix=".current.", dir=SNAPSHOT_ROOT)
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        os.write(descriptor, f"{generation_id}\n".encode("ascii"))
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        os.replace(temporary, CURRENT)
        directory_descriptor = os.open(SNAPSHOT_ROOT, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
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
        if len(sys.argv) != 3 or sys.argv[1] != "--generation":
            raise PointerSwitchError("pointer switch accepts exactly one generation identifier")
        switch_current(sys.argv[2])
    except PointerSwitchError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
