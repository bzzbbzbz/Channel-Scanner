"""Host-side safety utilities for the opt-in BL-22 stage-7 acceptance run."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Sequence

_TOKEN_RE = re.compile(r"^[0-9]{5,20}:[A-Za-z0-9_-]{20,}$")
_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
STAGE7_ACCEPTANCE_FILES = (
    "docker-compose.yml",
    ".dockerignore",
    "scripts/run-bl22-stage7-e2e.sh",
    "tests/e2e/test_bl22_stage7_real_telegram.py",
    "tests/e2e/docker-compose.bl22-stage7.yml",
    "tests/e2e/isolated-stage7.guard",
    "src/reliability/stage7_harness.py",
    "src/reliability/stage7_control.py",
    "src/config/settings.py",
    "tests/unit/test_stage7_harness.py",
    "tests/unit/test_bot_token_file.py",
)


@dataclass(frozen=True, slots=True, repr=False)
class Stage7Secrets:
    product_token: str
    tester_token: str
    chat_id: int


def read_env_file(path: Path) -> dict[str, str]:
    """Read the small dotenv subset used by bot credentials without shell evaluation."""
    if not path.is_file():
        raise ValueError(".env file is missing")
    values: dict[str, str] = {}
    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        if "=" not in line:
            raise ValueError(f"invalid .env assignment at line {line_number}")
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not _KEY_RE.fullmatch(key):
            raise ValueError(f"invalid .env key at line {line_number}")
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        elif " #" in value:
            value = value.split(" #", 1)[0].rstrip()
        values[key] = value
    return values


def load_stage7_secrets(path: Path) -> Stage7Secrets:
    values = read_env_file(path)
    product_token = values.get("BOT_TOKEN", "").strip() or values.get("TELEGRAM_TOKEN", "").strip()
    tester_token = values.get("E2E_TELEGRAM_TOKEN", "").strip()
    chat_id_raw = values.get("E2E_CHAT_ID", "").strip()
    if not product_token:
        raise ValueError("BOT_TOKEN or TELEGRAM_TOKEN is required in .env")
    if not tester_token:
        raise ValueError("E2E_TELEGRAM_TOKEN is required in .env")
    if not chat_id_raw:
        raise ValueError("E2E_CHAT_ID is required in .env")
    if not _TOKEN_RE.fullmatch(product_token) or not _TOKEN_RE.fullmatch(tester_token):
        raise ValueError("Telegram token format is invalid")
    if product_token == tester_token:
        raise ValueError("product and tester Telegram tokens must be distinct")
    try:
        chat_id = int(chat_id_raw)
    except ValueError as exc:
        raise ValueError("E2E_CHAT_ID must be an integer") from exc
    if chat_id >= 0 or chat_id < -999_999_999_999_999:
        raise ValueError("E2E_CHAT_ID must be a valid negative group or supergroup id")
    return Stage7Secrets(product_token, tester_token, chat_id)


def cleanup_stage7(
    run: Callable[..., object],
    *,
    project: str,
    compose_file: Path,
    image: str,
    token_file: Path | None,
    stage_env: dict[str, str],
) -> list[str]:
    """Remove only stage-7 resources, image, and temporary credential file."""
    errors: list[str] = []

    def invoke(command: Sequence[str], *, timeout: int = 120, env: dict[str, str] | None = None):
        try:
            return run(list(command), timeout=timeout, check=False, capture=True, env=env)
        except Exception:  # final resource checks decide whether fallback cleanup succeeded
            return None

    invoke(
        [
            "docker", "compose", "-p", project, "-f", str(compose_file),
            "down", "--volumes", "--remove-orphans", "--timeout", "10",
        ],
        env=stage_env,
    )

    label = f"label=com.docker.compose.project={project}"

    def ids(command: Sequence[str]) -> list[str] | None:
        result = invoke(command, timeout=30)
        if result is None or getattr(result, "returncode", 1) != 0:
            return None
        return [item for item in getattr(result, "stdout", "").splitlines() if item]

    container_command = ["docker", "ps", "-aq", "--filter", label]
    containers = ids(container_command)
    if containers:
        invoke(["docker", "rm", "-f", *containers], timeout=120)
    remaining_containers = ids(container_command)
    containers_gone = remaining_containers == []

    volume_command = ["docker", "volume", "ls", "-q", "--filter", label]
    network_command = ["docker", "network", "ls", "-q", "--filter", label]
    if containers_gone:
        volumes = ids(volume_command)
        if volumes:
            invoke(["docker", "volume", "rm", "-f", *volumes], timeout=120)
        networks = ids(network_command)
        if networks:
            invoke(["docker", "network", "rm", *networks], timeout=120)

    invoke(["docker", "image", "rm", "-f", image])

    final_resources = {
        "containers": remaining_containers,
        "volumes": ids(volume_command),
        "networks": ids(network_command),
    }
    for kind, remaining in final_resources.items():
        if remaining is None:
            errors.append(f"{kind} inspection failed")
        elif remaining:
            errors.append(f"isolated {kind} remain")

    image_inspect = invoke(["docker", "image", "inspect", image], timeout=30)
    if image_inspect is None:
        errors.append("image absence inspection failed")
    elif getattr(image_inspect, "returncode", 1) == 0:
        errors.append("isolated image remains")
    elif "No such image" not in getattr(image_inspect, "stderr", ""):
        errors.append(f"image absence was not confirmed: inspect exited {image_inspect.returncode}")

    if token_file is not None and containers_gone:
        try:
            token_file.unlink(missing_ok=True)
        except Exception as exc:
            errors.append(f"token file removal raised {type(exc).__name__}")
        if token_file.exists():
            errors.append("temporary token file remains")
    elif token_file is not None:
        errors.append("temporary token file retained because project containers may still hold it")
    return errors


def source_tree_sha256(root: Path) -> str:
    """Hash exactly the source inputs copied by Dockerfile, including untracked files."""
    files: list[Path] = []
    for relative in ("Dockerfile", ".dockerignore", "pyproject.toml", "config.toml", "alembic.ini"):
        path = root / relative
        if path.is_file():
            files.append(path)
    for relative in ("src", "alembic", "docker"):
        base = root / relative
        files.extend(
            path
            for path in base.rglob("*")
            if path.is_file()
            and "__pycache__" not in path.parts
            and path.suffix not in {".pyc", ".pyo"}
        )
    digest = hashlib.sha256()
    for path in sorted(files, key=lambda item: item.relative_to(root).as_posix()):
        relative = path.relative_to(root).as_posix().encode("utf-8")
        content = path.read_bytes()
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    return f"sha256:{digest.hexdigest()}"


def acceptance_harness_sha256(root: Path) -> str:
    """Hash the dirty/untracked stage-7 orchestration that is outside the image inputs."""
    digest = hashlib.sha256()
    for relative_name in STAGE7_ACCEPTANCE_FILES:
        path = root / relative_name
        if not path.is_file():
            raise ValueError(f"stage-7 acceptance input is missing: {relative_name}")
        relative = relative_name.encode("utf-8")
        content = path.read_bytes()
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    return f"sha256:{digest.hexdigest()}"


def create_token_file(directory: Path, token: str, run_id: str) -> Path:
    """Create an exclusive host credential file without exposing the token in argv/env."""
    if not directory.is_dir():
        raise ValueError("token file directory must already exist")
    path = directory / f"bl22-stage7-{run_id}.token"
    descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="ascii") as stream:
            stream.write(token)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
    except BaseException:
        path.unlink(missing_ok=True)
        raise
    if path.stat().st_mode & 0o077:
        path.unlink(missing_ok=True)
        raise RuntimeError("temporary token file permissions are not private")
    return path


def build_success_report(
    *,
    git_head: str,
    source_sha256: str,
    acceptance_harness_sha256: str,
    image_id: str,
    correlation_id: str,
    event_id: str,
    run_id: str,
    accepted_send_count: int,
    states: dict[str, str | int | bool],
    durations_seconds: dict[str, float],
    scenarios: list[str],
) -> dict:
    if accepted_send_count != 1:
        raise ValueError("a successful stage-7 report requires exactly one accepted send")
    return {
        "timestamp_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "code_refs": {
            "git_head": git_head,
            "source_sha256": source_sha256,
            "acceptance_harness_sha256": acceptance_harness_sha256,
        },
        "image_id": image_id,
        "correlation_id": correlation_id,
        "event_id": event_id,
        "run_id": run_id,
        "accepted_send_count": accepted_send_count,
        "states": states,
        "durations_seconds": {key: round(value, 3) for key, value in durations_seconds.items()},
        "scenarios": scenarios,
    }


def write_success_report(directory: Path, report: dict) -> Path:
    """Atomically save the fixed content-free report schema."""
    allowed = {
        "timestamp_utc", "code_refs", "image_id", "correlation_id", "event_id", "run_id",
        "accepted_send_count", "states", "durations_seconds", "scenarios",
    }
    if set(report) != allowed:
        raise ValueError("stage-7 report has an unexpected schema")
    directory.mkdir(parents=True, exist_ok=True)
    stamp = str(report["timestamp_utc"]).replace(":", "").replace("-", "")
    destination = directory / f"bl22-stage7-{stamp}.json"
    temporary = destination.with_suffix(".tmp")
    temporary.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(destination)
    return destination


def main() -> None:
    parser = argparse.ArgumentParser(description="BL-22 stage-7 host safety utilities")
    parser.add_argument("command", choices=("preflight",))
    parser.add_argument("env_file", type=Path)
    args = parser.parse_args()
    if args.command == "preflight":
        load_stage7_secrets(args.env_file)
        print("BL-22 stage 7 preflight: configuration valid")


if __name__ == "__main__":
    main()
