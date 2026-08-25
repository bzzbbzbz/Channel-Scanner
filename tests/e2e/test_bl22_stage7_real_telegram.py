"""Opt-in real Telegram acceptance for BL-22 stage 7."""

from __future__ import annotations

import asyncio
import json
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest
from aiogram import Bot

from src.reliability.stage7_harness import (
    acceptance_harness_sha256,
    build_success_report,
    cleanup_stage7,
    create_token_file,
    load_stage7_secrets,
    source_tree_sha256,
    write_success_report,
)

ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = ROOT / "tests/e2e/docker-compose.bl22-stage7.yml"
CURRENT_COMPOSE_FILE = ROOT / "docker-compose.yml"
REPORT_DIR = ROOT / ".planning/evaluations/bl22-stage7"
TOKEN_DIRECTORY = Path("/tmp/opencode")
ROOT_TOPIC = "tpb.digest.run.requested.v1"
ROLE_SERVICES = ("scheduler", "outbox-relay", "digest-worker", "telegram-delivery-worker")
_SECRET_ENV_NAMES = {"BOT_TOKEN", "TELEGRAM_TOKEN", "E2E_TELEGRAM_TOKEN", "BL22_STAGE7_PRODUCT_TOKEN"}


@dataclass(frozen=True, slots=True)
class ProductionAppIdentity:
    container_id: str
    started_at: str


@dataclass(frozen=True, slots=True)
class ProductionUserRouting:
    tester_id: int
    chat_id: int
    chat_type: str
    updated_at: datetime


def _run(
    command: list[str],
    *,
    env: dict[str, str] | None = None,
    timeout: int = 180,
    capture: bool = False,
    check: bool = True,
):
    child_env = os.environ.copy()
    for name in _SECRET_ENV_NAMES:
        child_env.pop(name, None)
    child_env["COMPOSE_DISABLE_ENV_FILE"] = "1"
    child_env.update(env or {})
    return subprocess.run(
        command,
        cwd=ROOT,
        env=child_env,
        check=check,
        text=True,
        timeout=timeout,
        capture_output=capture,
    )


def _compose(
    project: str,
    *args: str,
    env: dict[str, str],
    timeout: int = 180,
    capture: bool = False,
    check: bool = True,
):
    return _run(
        ["docker", "compose", "-p", project, "-f", str(COMPOSE_FILE), *args],
        env=env,
        timeout=timeout,
        capture=capture,
        check=check,
    )


def _control(project: str, env: dict[str, str], *args: str, timeout: int = 90) -> dict:
    result = _compose(
        project,
        "run",
        "--rm",
        "--no-deps",
        "control",
        "python",
        "-m",
        "src.reliability.stage7_control",
        *args,
        env=env,
        timeout=timeout,
        capture=True,
    )
    for line in reversed(result.stdout.splitlines()):
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            continue
    raise AssertionError("stage-7 control returned no content-free JSON result")


def _container_id(project: str, service: str, env: dict[str, str]) -> str:
    return _compose(project, "ps", "-aq", service, env=env, capture=True).stdout.strip()


def _inspect(container_id: str) -> dict:
    if not container_id:
        return {}
    result = _run(["docker", "inspect", container_id], timeout=30, capture=True)
    return json.loads(result.stdout)[0]


def _wait_service_health(project: str, service: str, env: dict[str, str], timeout: float = 90) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        details = _inspect(_container_id(project, service, env))
        state = details.get("State", {})
        health = state.get("Health", {}).get("Status")
        if state.get("Running") and health in {None, "healthy"}:
            return
        time.sleep(0.25)
    raise AssertionError(f"isolated service {service} did not become healthy")


def _wait_log(project: str, service: str, marker: str, env: dict[str, str], timeout: float = 90) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = _compose(project, "logs", "--no-color", service, env=env, capture=True)
        if marker in result.stdout + result.stderr:
            return
        time.sleep(0.25)
    raise AssertionError(f"isolated service {service} did not report its readiness marker")


def _current_project_service(service: str) -> str:
    result = _run(
        ["docker", "compose", "-f", str(CURRENT_COMPOSE_FILE), "ps", "-q", service],
        timeout=30,
        capture=True,
    )
    ids = [item for item in result.stdout.splitlines() if item]
    if len(ids) != 1:
        raise AssertionError(f"current project must have exactly one running {service} container")
    details = _inspect(ids[0])
    labels = details.get("Config", {}).get("Labels", {})
    if labels.get("com.docker.compose.service") != service or not details.get("State", {}).get("Running"):
        raise AssertionError(f"current project {service} container must already be running")
    return ids[0]


def _capture_production_app(product_token: str) -> ProductionAppIdentity:
    container_id = _current_project_service("app")
    details = _inspect(container_id)
    environment = {}
    for entry in details.get("Config", {}).get("Env", []):
        key, separator, value = entry.partition("=")
        if separator:
            environment[key] = value
    effective_token = environment.get("BOT_TOKEN", "").strip() or environment.get("TELEGRAM_TOKEN", "").strip()
    if not effective_token or effective_token != product_token:
        raise AssertionError("current production app credential does not match the stage-7 product credential")
    started_at = details.get("State", {}).get("StartedAt")
    if not started_at:
        raise AssertionError("current production app has no stable StartedAt identity")
    identity = ProductionAppIdentity(container_id, started_at)
    logs = _production_logs(identity, started_at)
    if "Telegram bot polling started" not in logs or "Run polling for bot" not in logs:
        raise AssertionError("current production app generation has no polling startup evidence")
    return identity


def _assert_production_app_continuity(identity: ProductionAppIdentity) -> None:
    if _current_project_service("app") != identity.container_id:
        raise AssertionError("current production app container identity changed during stage 7")
    details = _inspect(identity.container_id)
    state = details.get("State", {})
    if not state.get("Running") or state.get("StartedAt") != identity.started_at:
        raise AssertionError("current production app restarted or stopped during stage 7")


def _assert_no_polling_conflict(identity: ProductionAppIdentity, since: datetime) -> None:
    logs = _production_logs(identity, since.isoformat())
    if "TelegramConflictError" in logs or "terminated by other getUpdates request" in logs:
        raise AssertionError("a polling conflict was observed during stage 7")


def _production_logs(identity: ProductionAppIdentity, since: str) -> str:
    result = _run(
        ["docker", "logs", "--since", since, identity.container_id],
        timeout=30,
        capture=True,
        check=False,
    )
    if result.returncode != 0:
        raise AssertionError("current production app logs could not be inspected")
    return result.stdout + result.stderr


def _wait_handled_update_log(identity: ProductionAppIdentity, sent_at: datetime, timeout: float = 45) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        logs = _production_logs(identity, sent_at.isoformat())
        if re.search(r"Update id=\d+ is handled", logs):
            return
        time.sleep(0.25)
    raise AssertionError("current production app did not log a handled update after tester /start")


async def _telegram_preflight(product_token: str, tester_token: str, chat_id: int) -> tuple[int, str]:
    product = Bot(product_token)
    tester = Bot(tester_token)
    try:
        product_identity, tester_identity = await asyncio.gather(product.get_me(), tester.get_me())
        if product_identity.id == tester_identity.id:
            raise AssertionError("product and tester credentials resolved to the same bot")
        product_chat, tester_chat = await asyncio.gather(product.get_chat(chat_id), tester.get_chat(chat_id))
        product_type = getattr(product_chat.type, "value", product_chat.type)
        tester_type = getattr(tester_chat.type, "value", tester_chat.type)
        if product_type not in {"group", "supergroup"} or tester_type not in {"group", "supergroup"}:
            raise AssertionError("E2E_CHAT_ID is not a group visible to both bots")
        if product_type != tester_type:
            raise AssertionError("product and tester bots observe different E2E chat types")
        return tester_identity.id, tester_type
    except AssertionError:
        raise
    except Exception as exc:
        raise RuntimeError(f"real Telegram preflight failed with {type(exc).__name__}") from None
    finally:
        await product.session.close()
        await tester.session.close()


async def _send_tester_start(tester_token: str, chat_id: int, marker: str) -> tuple[int, datetime]:
    tester = Bot(tester_token)
    sent_after = datetime.now(timezone.utc)
    try:
        message = await tester.send_message(chat_id, f"/start {marker}")
        return message.message_id, sent_after
    except Exception as exc:
        raise RuntimeError(f"tester /start failed with {type(exc).__name__}") from None
    finally:
        await tester.session.close()


async def _delete_tester_message(tester_token: str, chat_id: int, message_id: int) -> bool:
    tester = Bot(tester_token)
    try:
        await tester.delete_message(chat_id, message_id)
        return True
    except Exception:
        return False
    finally:
        await tester.session.close()


def _read_production_user(db_container_id: str, tester_id: int) -> ProductionUserRouting | None:
    query = (
        "select telegram_user_id, chat_id, chat_type, extract(epoch from updated_at) "
        f"from users where telegram_user_id = {tester_id};"
    )
    result = _run(
        ["docker", "exec", db_container_id, "psql", "-U", "bot", "-d", "telegram_bot", "-At", "-F", "|", "-c", query],
        timeout=30,
        capture=True,
        check=False,
    )
    if result.returncode != 0:
        raise AssertionError("current production tester row could not be queried")
    if not result.stdout.strip():
        return None
    fields = result.stdout.strip().split("|")
    if len(fields) != 4:
        raise AssertionError("current production tester row query was ambiguous")
    return ProductionUserRouting(
        tester_id=int(fields[0]),
        chat_id=int(fields[1]),
        chat_type=fields[2],
        updated_at=datetime.fromtimestamp(float(fields[3]), tz=timezone.utc),
    )


def _wait_production_onboarding(
    db_container_id: str,
    *,
    tester_id: int,
    chat_id: int,
    chat_type: str,
    baseline: ProductionUserRouting | None,
    sent_after: datetime,
    timeout: float = 45,
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        observed_at = datetime.now(timezone.utc)
        row = _read_production_user(db_container_id, tester_id)
        baseline_changed = baseline is None or (row is not None and row.updated_at > baseline.updated_at)
        if (
            row is not None
            and baseline_changed
            and row.tester_id == tester_id
            and row.chat_id == chat_id
            and row.chat_type == chat_type
            and sent_after <= row.updated_at <= observed_at
        ):
            return
        time.sleep(0.25)
    raise AssertionError("current production database did not confirm exact tester onboarding after /start")


def _assert_delivery_secret_scope(project: str, env: dict[str, str], token_file: Path) -> None:
    destination = "/run/secrets/bl22_stage7_product_bot_token"
    for service in ("migrate", "kafka-init", "control", *ROLE_SERVICES):
        container_id = _container_id(project, service, env)
        if not container_id:
            continue
        details = _inspect(container_id)
        entries = details.get("Config", {}).get("Env", [])
        if any(entry.startswith(("BOT_TOKEN=", "TELEGRAM_TOKEN=")) for entry in entries):
            raise AssertionError("an isolated stage-7 container received a Telegram token environment value")
        token_file_entries = [entry for entry in entries if entry.startswith("BOT_TOKEN_FILE=")]
        mounted_destinations = {mount.get("Destination") for mount in details.get("Mounts", [])}
        if service == "telegram-delivery-worker":
            if token_file_entries != [f"BOT_TOKEN_FILE={destination}"] or destination not in mounted_destinations:
                raise AssertionError("delivery worker did not receive the scoped token-file secret")
            matching_sources = [
                mount.get("Source") for mount in details.get("Mounts", []) if mount.get("Destination") == destination
            ]
            if matching_sources != [str(token_file)]:
                raise AssertionError("delivery worker token secret source is not the expected temporary file")
        elif token_file_entries or destination in mounted_destinations:
            raise AssertionError("token-file secret escaped the Telegram delivery worker")


def _consumer_group_offset(project: str, env: dict[str, str]) -> int | None:
    result = _compose(
        project,
        "exec",
        "-T",
        "kafka",
        "/opt/kafka/bin/kafka-consumer-groups.sh",
        "--bootstrap-server",
        "kafka:9092",
        "--group",
        "digest-renderer-v1",
        "--describe",
        env=env,
        capture=True,
    )
    for line in (result.stdout + result.stderr).splitlines():
        fields = line.split()
        if len(fields) >= 4 and fields[1] == ROOT_TOPIC and fields[2] == "0" and fields[3].isdigit():
            return int(fields[3])
    return None


def _wait_consumer_offset(project: str, env: dict[str, str], minimum: int, timeout: float = 30) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        offset = _consumer_group_offset(project, env)
        if offset is not None and offset >= minimum:
            return
        time.sleep(0.25)
    raise AssertionError("digest consumer offset did not advance past the duplicate event")


def _assert_final_snapshot(snapshot: dict) -> None:
    assert snapshot["routing_identity_matches"] is True
    assert snapshot["isolated_user_count"] == 1
    assert snapshot["run_count"] == 1
    assert snapshot["run_state"] == "completed"
    assert snapshot["root_event_count"] == 1
    assert snapshot["root_state"] == "published"
    assert snapshot["root_inbox_count"] == 1
    assert snapshot["root_inbox_state"] == "completed"
    assert snapshot["root_inbox_attempts"] == 1
    assert snapshot["message_count"] == 1
    assert snapshot["message_states"] == ["sent"]
    assert snapshot["message_attempts"] == [1]
    assert snapshot["telegram_message_id_set"] is True
    assert snapshot["distinct_telegram_message_ids"] == 1
    assert snapshot["marker_in_persisted_part"] is True
    assert snapshot["delivery_event_count"] == 1
    assert snapshot["delivery_event_state"] == "published"
    assert snapshot["delivery_inbox_count"] == 1
    assert snapshot["delivery_inbox_state"] == "completed"
    assert snapshot["post_delivery_count"] == 1
    assert snapshot["post_delivery_states"] == ["delivered"]
    assert snapshot["processing_log_count"] == 1
    assert snapshot["processing_totals"] == [[1, 0, 1]]
    assert snapshot["digest_chat_record_count"] == 1
    assert snapshot["last_digest_at_set"] is True


@pytest.mark.e2e
@pytest.mark.stage7
def test_bl22_stage7_real_telegram_delivery_and_duplicate_suppression() -> None:
    if os.environ.get("BL22_STAGE7_E2E") != "1":
        pytest.skip("run only through scripts/run-bl22-stage7-e2e.sh")

    secrets = load_stage7_secrets(ROOT / ".env")
    harness_run_id = uuid4()
    project = f"tpb-bl22-stage7-{harness_run_id.hex}"
    image = f"telegram-parser-bot:bl22-stage7-e2e-{harness_run_id.hex}"
    token_file: Path | None = None
    tester_message_id: int | None = None
    tester_message_deleted = False
    production_identity: ProductionAppIdentity | None = None
    body_error: BaseException | None = None
    body_traceback = None
    report_values: dict | None = None
    started_at = time.monotonic()
    continuity_since = datetime.now(timezone.utc)
    durations: dict[str, float] = {}
    stage_env: dict[str, str] = {}

    try:
        production_identity = _capture_production_app(secrets.product_token)
        production_db_id = _current_project_service("db")
        tester_id, chat_type = asyncio.run(
            _telegram_preflight(secrets.product_token, secrets.tester_token, secrets.chat_id)
        )
        tester_baseline = _read_production_user(production_db_id, tester_id)
        source_sha256 = source_tree_sha256(ROOT)
        harness_sha256 = acceptance_harness_sha256(ROOT)
        git_head = _run(["git", "rev-parse", "HEAD"], capture=True, timeout=30).stdout.strip()
        token_file = create_token_file(TOKEN_DIRECTORY, secrets.product_token, harness_run_id.hex)
        stage_env = {
            "BL22_STAGE7_E2E": "1",
            "BL22_STAGE7_RUN_ID": str(harness_run_id),
            "BL22_STAGE7_IMAGE": image,
            "BL22_STAGE7_CHAT_ID": str(secrets.chat_id),
            "BL22_STAGE7_TESTER_ID": str(tester_id),
            "BL22_STAGE7_CHAT_TYPE": chat_type,
            "BL22_STAGE7_TOKEN_FILE": str(token_file),
        }

        setup_started = time.monotonic()
        _compose(project, "build", "migrate", env=stage_env, timeout=300)
        image_id = json.loads(_run(["docker", "image", "inspect", image], capture=True, timeout=30).stdout)[0]["Id"]
        if source_tree_sha256(ROOT) != source_sha256:
            raise AssertionError("Docker build-context source changed while the stage-7 image was built")
        if acceptance_harness_sha256(ROOT) != harness_sha256:
            raise AssertionError("stage-7 acceptance harness changed while its image was built")
        _compose(project, "up", "-d", "postgres", "kafka", env=stage_env, timeout=180)
        _wait_service_health(project, "postgres", stage_env)
        _wait_service_health(project, "kafka", stage_env, timeout=180)
        _compose(project, "run", "--rm", "--no-deps", "migrate", env=stage_env, timeout=180)
        _compose(project, "run", "--rm", "--no-deps", "kafka-init", env=stage_env, timeout=120)
        assert _control(project, stage_env, "migration") == {"migration_head": "0024_reliable_delete_cascades"}
        durations["isolated_setup"] = time.monotonic() - setup_started
        _assert_production_app_continuity(production_identity)

        onboarding_started = time.monotonic()
        tester_message_id, sent_after = asyncio.run(
            _send_tester_start(secrets.tester_token, secrets.chat_id, f"BL22S7_{harness_run_id.hex}")
        )
        _wait_production_onboarding(
            production_db_id,
            tester_id=tester_id,
            chat_id=secrets.chat_id,
            chat_type=chat_type,
            baseline=tester_baseline,
            sent_after=sent_after,
        )
        _wait_handled_update_log(production_identity, sent_after)
        tester_message_deleted = asyncio.run(
            _delete_tester_message(secrets.tester_token, secrets.chat_id, tester_message_id)
        )
        if tester_message_deleted:
            tester_message_id = None
        _assert_production_app_continuity(production_identity)
        _assert_no_polling_conflict(production_identity, continuity_since)
        durations["production_onboarding"] = time.monotonic() - onboarding_started

        seeded = _control(project, stage_env, "seed")
        subscription_id = int(seeded["subscription_id"])
        assert seeded["seeded_posts"] == 1
        assert seeded["routing_identity_mirrored"] is True
        stage_env["BL22_STAGE7_SUBSCRIPTION_ID"] = str(subscription_id)

        delivery_started = time.monotonic()
        _compose(project, "up", "-d", "scheduler", env=stage_env)
        _wait_log(project, "scheduler", "reliable scheduler active", stage_env)
        _compose(project, "up", "-d", "outbox-relay", env=stage_env)
        _wait_log(project, "outbox-relay", "outbox relay ready", stage_env)
        _compose(project, "up", "-d", "digest-worker", env=stage_env)
        _wait_log(project, "digest-worker", "digest worker active", stage_env)
        _compose(project, "up", "-d", "telegram-delivery-worker", env=stage_env)
        _wait_log(project, "telegram-delivery-worker", "Telegram delivery worker active", stage_env)
        _assert_delivery_secret_scope(project, stage_env, token_file)

        _control(project, stage_env, "wait-completed", str(subscription_id), "--timeout", "120", timeout=140)
        before_duplicate = _control(project, stage_env, "snapshot", str(subscription_id))
        _assert_final_snapshot(before_duplicate)
        durations["first_delivery"] = time.monotonic() - delivery_started
        _assert_production_app_continuity(production_identity)

        duplicate_started = time.monotonic()
        duplicate = _control(project, stage_env, "republish-root", str(subscription_id))
        assert duplicate["event_id"] == before_duplicate["root_event_id"]
        audit = _control(
            project,
            stage_env,
            "audit-root",
            str(subscription_id),
            "--timeout",
            "5",
            timeout=20,
        )
        assert audit["event_id"] == before_duplicate["root_event_id"]
        assert audit["count"] == 2
        assert audit["key_matches"] is True
        assert audit["bytes_identical"] is True
        assert audit["envelopes_match_db"] is True
        assert duplicate["offset"] in audit["offsets"]
        _wait_consumer_offset(project, stage_env, int(duplicate["offset"]) + 1)
        time.sleep(5)
        after_duplicate = _control(project, stage_env, "snapshot", str(subscription_id))
        _assert_final_snapshot(after_duplicate)
        assert after_duplicate == before_duplicate
        durations["duplicate_observation"] = time.monotonic() - duplicate_started

        role_logs_result = _compose(project, "logs", "--no-color", *ROLE_SERVICES, env=stage_env, capture=True)
        role_logs = role_logs_result.stdout + role_logs_result.stderr
        if secrets.product_token in role_logs or secrets.tester_token in role_logs or str(secrets.chat_id) in role_logs:
            raise AssertionError("stage-7 logs contain a credential or chat id")
        if "BL22S7-" in role_logs:
            raise AssertionError("stage-7 transition logs contain the private marker")
        sent_lines = [
            line for line in role_logs.splitlines()
            if f"run_id={before_duplicate['run_id']}" in line and "state=sent" in line
        ]
        assert len(sent_lines) == 1
        assert re.search(
            rf"event_id=[0-9a-f-]+ correlation_id={re.escape(before_duplicate['correlation_id'])} "
            rf"run_id={re.escape(before_duplicate['run_id'])} message_id=[0-9a-f-]+ attempt=1 state=sent",
            sent_lines[0],
        )
        _assert_production_app_continuity(production_identity)
        _assert_no_polling_conflict(production_identity, continuity_since)
        if source_tree_sha256(ROOT) != source_sha256 or acceptance_harness_sha256(ROOT) != harness_sha256:
            raise AssertionError("stage-7 source or acceptance harness changed during execution")

        report_values = {
            "git_head": git_head,
            "source_sha256": source_sha256,
            "acceptance_harness_sha256": harness_sha256,
            "image_id": image_id,
            "correlation_id": before_duplicate["correlation_id"],
            "event_id": before_duplicate["root_event_id"],
            "run_id": before_duplicate["run_id"],
            "accepted_send_count": 1,
            "states": {
                "run": after_duplicate["run_state"],
                "part": after_duplicate["message_states"][0],
                "root_inbox": after_duplicate["root_inbox_state"],
                "delivery_inbox": after_duplicate["delivery_inbox_state"],
                "duplicate_suppressed": True,
                "production_onboarding_confirmed": True,
                "isolated_routing_identity_mirrored": True,
                "production_app_continuous": True,
                "tester_start_deleted": tester_message_deleted,
            },
            "durations_seconds": durations,
            "scenarios": [
                "continuous_production_app_onboarding",
                "reliable_real_telegram_delivery",
                "exact_event_duplicate_suppression",
                "fail_hard_isolated_cleanup",
            ],
        }
    except BaseException as exc:
        body_error = exc
        body_traceback = sys.exc_info()[2]

    if tester_message_id is not None:
        tester_message_deleted = asyncio.run(
            _delete_tester_message(secrets.tester_token, secrets.chat_id, tester_message_id)
        )
    cleanup_started = time.monotonic()
    cleanup_errors = cleanup_stage7(
        _run,
        project=project,
        compose_file=COMPOSE_FILE,
        image=image,
        token_file=token_file,
        stage_env=stage_env,
    )
    if production_identity is not None:
        try:
            _assert_production_app_continuity(production_identity)
            _assert_no_polling_conflict(production_identity, continuity_since)
        except Exception as exc:
            cleanup_errors.append(f"production continuity verification raised {type(exc).__name__}")
    durations["cleanup"] = time.monotonic() - cleanup_started
    durations["total"] = time.monotonic() - started_at
    if cleanup_errors:
        message = "BL-22 stage-7 cleanup failed: " + "; ".join(cleanup_errors)
        if body_error is not None:
            body_error.add_note(message)
        else:
            raise AssertionError(message)
    if body_error is not None:
        raise body_error.with_traceback(body_traceback)
    if report_values is None:
        raise AssertionError("stage-7 succeeded without report evidence")
    report_values["states"]["tester_start_deleted"] = tester_message_deleted
    report_values["durations_seconds"] = durations
    report = build_success_report(**report_values)
    report_path = write_success_report(REPORT_DIR, report)
    print(json.dumps({"bl22_stage7": "passed", "report": str(report_path.relative_to(ROOT))}, sort_keys=True))
