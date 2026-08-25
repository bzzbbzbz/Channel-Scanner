"""Process-bound readiness state for BL-22 worker roles."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

_ROLE_ENV = "RELIABILITY_ROLE_NAME"
_FILE_ENV = "RELIABILITY_ROLE_HEALTH_FILE"


def _process_start_ticks(pid: int) -> str:
    fields = Path(f"/proc/{pid}/stat").read_text(encoding="ascii").split()
    return fields[21]


class RoleReadiness:
    """Publish readiness tied to one concrete process generation."""

    def __init__(self, role: str, path: Path | None = None) -> None:
        self.role = role
        configured = path or (Path(os.environ[_FILE_ENV]) if os.environ.get(_FILE_ENV) else None)
        self.path = configured
        self.clear()

    def mark_ready(self) -> None:
        if self.path is None:
            return
        value = f"{self.role}:{os.getpid()}:{_process_start_ticks(os.getpid())}\n"
        self.path.write_text(value, encoding="ascii")

    def clear(self) -> None:
        if self.path is not None:
            self.path.unlink(missing_ok=True)


def role_is_ready(role: str, path: Path | None = None, pid: int | None = None) -> bool:
    pid = pid or os.getpid()
    configured = path or (Path(os.environ[_FILE_ENV]) if os.environ.get(_FILE_ENV) else None)
    if configured is None:
        return False
    try:
        expected = f"{role}:{pid}:{_process_start_ticks(pid)}\n"
        return configured.read_text(encoding="ascii") == expected
    except (FileNotFoundError, IndexError, OSError):
        return False


def main() -> None:
    parser = argparse.ArgumentParser(description="Check BL-22 role semantic readiness")
    parser.add_argument("command", choices=("check",))
    args = parser.parse_args()
    if args.command == "check" and not role_is_ready(os.environ.get(_ROLE_ENV, ""), pid=1):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
