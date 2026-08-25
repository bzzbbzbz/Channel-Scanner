from __future__ import annotations

import json
from pathlib import Path
from subprocess import CompletedProcess

import pytest

from src.reliability.stage7_harness import (
    STAGE7_ACCEPTANCE_FILES,
    acceptance_harness_sha256,
    build_success_report,
    cleanup_stage7,
    create_token_file,
    load_stage7_secrets,
    source_tree_sha256,
    write_success_report,
)


def _write_env(path: Path, **overrides: str) -> None:
    values = {
        "BOT_TOKEN": "",
        "TELEGRAM_TOKEN": "123456:abcdefghijklmnopqrstuvwxyz_ABCDE",
        "E2E_TELEGRAM_TOKEN": "654321:abcdefghijklmnopqrstuvwxyz_FGHIJ",
        "E2E_CHAT_ID": "-1001234567890",
        **overrides,
    }
    path.write_text("\n".join(f"{key}={value}" for key, value in values.items()) + "\n", encoding="utf-8")


def test_stage7_preflight_loads_fallback_and_never_exposes_secrets_in_repr(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    _write_env(env_file)
    secrets = load_stage7_secrets(env_file)
    assert secrets.chat_id == -1001234567890
    assert "123456" not in repr(secrets)


@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"TELEGRAM_TOKEN": ""}, "required"),
        ({"E2E_TELEGRAM_TOKEN": ""}, "required"),
        ({"E2E_CHAT_ID": "private"}, "integer"),
        ({"E2E_CHAT_ID": "123"}, "negative"),
        ({"E2E_TELEGRAM_TOKEN": "123456:abcdefghijklmnopqrstuvwxyz_ABCDE"}, "distinct"),
    ],
)
def test_stage7_preflight_fails_hard_on_invalid_configuration(
    tmp_path: Path,
    overrides: dict[str, str],
    message: str,
) -> None:
    env_file = tmp_path / ".env"
    _write_env(env_file, **overrides)
    with pytest.raises(ValueError, match=message):
        load_stage7_secrets(env_file)


def test_cleanup_passes_full_env_and_falls_back_to_exact_labeled_resources(tmp_path: Path) -> None:
    calls: list[list[str]] = []
    environments: list[dict | None] = []
    token_file = tmp_path / "product.token"
    token_file.write_text("secret\n", encoding="ascii")
    list_counts = {"containers": 0, "volumes": 0, "networks": 0}

    def run(command, **kwargs):
        calls.append(command)
        environments.append(kwargs.get("env"))
        if "down" in command:
            return CompletedProcess(command, 2, "", "down interpolation failed")
        if command[:3] == ["docker", "ps", "-aq"]:
            list_counts["containers"] += 1
            return CompletedProcess(command, 0, "container-id\n" if list_counts["containers"] == 1 else "", "")
        if command[:4] == ["docker", "volume", "ls", "-q"]:
            list_counts["volumes"] += 1
            return CompletedProcess(command, 0, "volume-id\n" if list_counts["volumes"] == 1 else "", "")
        if command[:4] == ["docker", "network", "ls", "-q"]:
            list_counts["networks"] += 1
            return CompletedProcess(command, 0, "network-id\n" if list_counts["networks"] == 1 else "", "")
        if command[:3] == ["docker", "image", "inspect"]:
            return CompletedProcess(command, 1, "", "No such image")
        return CompletedProcess(command, 0, "", "")

    stage_env = {
        "BL22_STAGE7_RUN_ID": "run",
        "BL22_STAGE7_IMAGE": "isolated:image",
        "BL22_STAGE7_TOKEN_FILE": str(token_file),
        "BL22_STAGE7_TESTER_ID": "1",
        "BL22_STAGE7_CHAT_TYPE": "group",
    }
    errors = cleanup_stage7(
        run,
        project="isolated-project",
        compose_file=tmp_path / "compose.yml",
        image="isolated:image",
        token_file=token_file,
        stage_env=stage_env,
    )
    assert errors == []
    down_index = next(index for index, command in enumerate(calls) if "down" in command)
    assert environments[down_index] == stage_env
    container_rm = calls.index(["docker", "rm", "-f", "container-id"])
    volume_rm = calls.index(["docker", "volume", "rm", "-f", "volume-id"])
    network_rm = calls.index(["docker", "network", "rm", "network-id"])
    assert container_rm < volume_rm < network_rm
    assert not token_file.exists()
    assert all("start" not in call and "stop" not in call for call in calls)


def test_cleanup_retains_token_and_fails_if_credential_holding_container_remains(tmp_path: Path) -> None:
    token_file = tmp_path / "product.token"
    token_file.write_text("secret\n", encoding="ascii")

    def run(command, **kwargs):
        del kwargs
        if command[:3] == ["docker", "ps", "-aq"]:
            return CompletedProcess(command, 0, "credential-holder\n", "")
        if command[:3] == ["docker", "image", "inspect"]:
            return CompletedProcess(command, 1, "", "No such image")
        if command[:4] in (["docker", "volume", "ls", "-q"], ["docker", "network", "ls", "-q"]):
            return CompletedProcess(command, 0, "", "")
        return CompletedProcess(command, 1 if command[:3] == ["docker", "rm", "-f"] else 0, "", "")

    errors = cleanup_stage7(
        run,
        project="isolated-project",
        compose_file=tmp_path / "compose.yml",
        image="isolated:image",
        token_file=token_file,
        stage_env={"BL22_STAGE7_TOKEN_FILE": str(token_file)},
    )
    assert "isolated containers remain" in errors
    assert any("token file retained" in error for error in errors)
    assert token_file.exists()


@pytest.mark.parametrize("inspect_mode", ("exception", "daemon_error"))
def test_cleanup_requires_confirmed_image_absence(tmp_path: Path, inspect_mode: str) -> None:
    def run(command, **kwargs):
        del kwargs
        if command[:3] == ["docker", "image", "inspect"]:
            if inspect_mode == "exception":
                raise RuntimeError("daemon unavailable")
            return CompletedProcess(command, 1, "", "permission denied")
        if command[:3] == ["docker", "ps", "-aq"]:
            return CompletedProcess(command, 0, "", "")
        if command[:4] in (["docker", "volume", "ls", "-q"], ["docker", "network", "ls", "-q"]):
            return CompletedProcess(command, 0, "", "")
        return CompletedProcess(command, 0, "", "")

    errors = cleanup_stage7(
        run,
        project="isolated-project",
        compose_file=tmp_path / "compose.yml",
        image="isolated:image",
        token_file=None,
        stage_env={"BL22_STAGE7_IMAGE": "isolated:image"},
    )
    assert any("image absence" in error for error in errors)


def test_success_report_has_fixed_content_free_schema_and_atomic_write(tmp_path: Path) -> None:
    report = build_success_report(
        git_head="commit-ref",
        source_sha256="sha256:source",
        acceptance_harness_sha256="sha256:harness",
        image_id="sha256:image",
        correlation_id="correlation",
        event_id="event",
        run_id="run",
        accepted_send_count=1,
        states={"run": "completed", "duplicate_suppressed": True},
        durations_seconds={"total": 1.23456},
        scenarios=["real_delivery"],
    )
    path = write_success_report(tmp_path, report)
    saved = json.loads(path.read_text(encoding="utf-8"))
    assert saved == report
    assert saved["durations_seconds"] == {"total": 1.235}
    assert saved["code_refs"] == {
        "git_head": "commit-ref",
        "source_sha256": "sha256:source",
        "acceptance_harness_sha256": "sha256:harness",
    }
    assert saved["image_id"] == "sha256:image"
    assert not path.with_suffix(".tmp").exists()
    forbidden = {"token", "chat_id", "post", "message_id", "text", "content_hash"}
    assert forbidden.isdisjoint(saved)


def test_success_report_rejects_non_single_send() -> None:
    with pytest.raises(ValueError, match="exactly one"):
        build_success_report(
            git_head="code",
            source_sha256="sha256:source",
            acceptance_harness_sha256="sha256:harness",
            image_id="sha256:image",
            correlation_id="correlation",
            event_id="event",
            run_id="run",
            accepted_send_count=2,
            states={},
            durations_seconds={},
            scenarios=[],
        )


def test_stage7_compose_has_no_poller_and_scopes_token_file_to_delivery() -> None:
    compose = (Path(__file__).parents[1] / "e2e/docker-compose.bl22-stage7.yml").read_text(encoding="utf-8")
    assert "http://fake-telegram" not in compose
    assert "  app:" not in compose
    assert compose.count("BOT_API_BASE_URL: https://api.telegram.org") == 1
    assert 'RELIABLE_DIGEST_ALL_SUBSCRIPTIONS: "0"' in compose
    assert 'RELIABLE_DIGEST_SUBSCRIPTION_IDS: "[${BL22_STAGE7_SUBSCRIPTION_ID:-}]"' in compose
    assert "BOT_E2E_EXCLUSIVE_CHAT" not in compose
    assert "BL22_STAGE7_PRODUCT_TOKEN" not in compose
    assert "BOT_TOKEN:" not in compose
    assert compose.count("BOT_TOKEN_FILE: /run/secrets/bl22_stage7_product_bot_token") == 1
    assert "BL22_STAGE7_TESTER_ID" in compose
    assert "BL22_STAGE7_CHAT_TYPE" in compose
    assert 'OPENROUTER_API_KEY: ""' in compose

    orchestration = (Path(__file__).parents[1] / "e2e/test_bl22_stage7_real_telegram.py").read_text(encoding="utf-8")
    assert '["docker", "stop"' not in orchestration
    assert '["docker", "start"' not in orchestration
    assert "get_updates(" not in orchestration
    assert '"BL22_STAGE7_PRODUCT_TOKEN": secrets.product_token' not in orchestration
    assert "StartedAt" in orchestration
    assert "production_app_continuous" in orchestration


def test_create_token_file_is_private_and_source_hash_covers_untracked_build_inputs(tmp_path: Path) -> None:
    token_file = create_token_file(tmp_path, "123:secret", "run")
    assert token_file.read_text(encoding="ascii") == "123:secret\n"
    assert token_file.stat().st_mode & 0o077 == 0

    (tmp_path / "Dockerfile").write_text("FROM scratch\n", encoding="ascii")
    dockerignore = tmp_path / ".dockerignore"
    dockerignore.write_text(".git/\n", encoding="ascii")
    source = tmp_path / "src"
    source.mkdir()
    untracked = source / "new.py"
    untracked.write_text("VALUE = 1\n", encoding="ascii")
    first = source_tree_sha256(tmp_path)
    untracked.write_text("VALUE = 2\n", encoding="ascii")
    assert source_tree_sha256(tmp_path) != first
    second = source_tree_sha256(tmp_path)
    outside = tmp_path / ".planning"
    outside.mkdir()
    (outside / "report.json").write_text("{}", encoding="ascii")
    assert source_tree_sha256(tmp_path) == second
    dockerignore.write_text(".git/\n.data/\n", encoding="ascii")
    assert source_tree_sha256(tmp_path) != second


def test_acceptance_harness_hash_covers_dirty_untracked_orchestrator(tmp_path: Path) -> None:
    for relative_name in STAGE7_ACCEPTANCE_FILES:
        path = tmp_path / relative_name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"{relative_name}\n", encoding="utf-8")
    first = acceptance_harness_sha256(tmp_path)
    root_compose = tmp_path / "docker-compose.yml"
    root_compose.write_text("changed production selector\n", encoding="utf-8")
    assert acceptance_harness_sha256(tmp_path) != first
    root_compose.write_text("docker-compose.yml\n", encoding="utf-8")
    dockerignore = tmp_path / ".dockerignore"
    dockerignore.write_text("changed build context\n", encoding="utf-8")
    assert acceptance_harness_sha256(tmp_path) != first
