"""Reproducible process E2E for BL-22 stage 6 on isolated Docker services."""

from __future__ import annotations

import json
import os
import re
import subprocess
import time
from pathlib import Path
from uuid import uuid4

import pytest

ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = ROOT / "tests/e2e/docker-compose.bl22-stage6.yml"
ROOT_TOPIC = "tpb.digest.run.requested.v1"
ROLE_SERVICES = ("scheduler", "outbox-relay", "digest-worker", "telegram-delivery-worker", "app")
CRITICAL_ROLE_SERVICES = ("scheduler", "outbox-relay", "digest-worker", "telegram-delivery-worker")


def _run(
    command: list[str],
    *,
    env: dict[str, str] | None = None,
    timeout: int = 180,
    capture: bool = False,
    check: bool = True,
):
    return subprocess.run(
        command,
        cwd=ROOT,
        env={**os.environ, **(env or {})},
        check=check,
        text=True,
        timeout=timeout,
        capture_output=capture,
    )


def _compose(project: str, *args: str, timeout: int = 180, capture: bool = False, check: bool = True):
    return _run(
        ["docker", "compose", "-p", project, "-f", str(COMPOSE_FILE), *args],
        timeout=timeout,
        capture=capture,
        check=check,
    )


def _control(project: str, *args: str, timeout: int = 60) -> dict:
    result = _compose(
        project,
        "exec",
        "-T",
        "app",
        "python",
        "-m",
        "src.reliability.e2e_control",
        *args,
        timeout=timeout,
        capture=True,
    )
    for line in reversed(result.stdout.splitlines()):
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            continue
    raise AssertionError(f"control command returned no JSON: {result.stdout}")


def _container_id(project: str, service: str) -> str:
    return _compose(project, "ps", "-aq", service, capture=True).stdout.strip()


def _inspect_container(project: str, service: str) -> dict:
    container_id = _container_id(project, service)
    if not container_id:
        return {}
    return json.loads(_run(["docker", "inspect", container_id], capture=True).stdout)[0]


def _wait_service_health(project: str, service: str, timeout: float = 90) -> None:
    deadline = time.monotonic() + timeout
    last = {}
    while time.monotonic() < deadline:
        last = _inspect_container(project, service)
        state = last.get("State", {})
        health = state.get("Health", {}).get("Status")
        if state.get("Status") == "running" and health in {None, "healthy"}:
            return
        time.sleep(0.2)
    raise AssertionError(f"{service} did not become healthy: {last.get('State')}")


def _wait_container_exit(project: str, service: str, exit_code: int, timeout: float = 45) -> None:
    deadline = time.monotonic() + timeout
    last = {}
    while time.monotonic() < deadline:
        last = _inspect_container(project, service).get("State", {})
        if last.get("Status") == "exited" and last.get("ExitCode") == exit_code:
            return
        time.sleep(0.2)
    raise AssertionError(f"{service} did not exit with {exit_code}: {last}")


def _assert_service_not_ready(project: str, service: str) -> None:
    state = _inspect_container(project, service).get("State", {})
    assert state.get("Status") == "exited", f"{service} did not exit: {state}"
    assert state.get("Running") is False, f"{service} still reports running: {state}"


def _assert_role_readiness_matrix(project: str) -> None:
    for service in CRITICAL_ROLE_SERVICES:
        _wait_service_health(project, service)
        _compose(project, "stop", service)
        _assert_service_not_ready(project, service)
        _compose(project, "start", service)
        _wait_service_health(project, service)


def _wait_role_log(project: str, service: str, marker: str, timeout: float = 90) -> None:
    deadline = time.monotonic() + timeout
    output = ""
    while time.monotonic() < deadline:
        result = _compose(project, "logs", "--no-color", service, capture=True)
        output = result.stdout + result.stderr
        if marker in output:
            _wait_service_health(project, service)
            return
        time.sleep(0.2)
    raise AssertionError(f"{service} did not report readiness marker {marker!r}: {output[-4000:]}")


def _consumer_group_offset(project: str, group: str, topic: str) -> int | None:
    result = _compose(
        project,
        "exec",
        "-T",
        "kafka",
        "/opt/kafka/bin/kafka-consumer-groups.sh",
        "--bootstrap-server",
        "kafka:9092",
        "--group",
        group,
        "--describe",
        capture=True,
    )
    for line in (result.stdout + result.stderr).splitlines():
        fields = line.split()
        if len(fields) >= 4 and fields[1] == topic and fields[2] == "0" and fields[3].isdigit():
            return int(fields[3])
    return None


def _wait_consumer_offset(project: str, group: str, topic: str, minimum: int, timeout: float = 30) -> None:
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = _consumer_group_offset(project, group, topic)
        if last is not None and last >= minimum:
            return
        time.sleep(0.2)
    raise AssertionError(f"{group} offset did not reach {minimum}: {last}")


def _assert_snapshot(snapshot: dict, *, posts: int, parts: int, attempts: list[int] | None = None) -> None:
    assert snapshot["run_state"] == "completed"
    assert snapshot["run_count"] == 1
    assert snapshot["root_event_count"] == 1
    assert snapshot["message_count"] == parts
    assert snapshot["message_states"] == ["sent"] * parts
    assert snapshot["delivery_count"] == posts
    assert snapshot["legacy_delivery_count"] == 0
    assert snapshot["processing_log_count"] == 1
    assert snapshot["last_digest_at_set"] is True
    if attempts is not None:
        assert snapshot["message_attempts"] == attempts


def _assert_token_isolation(project: str, token: str) -> None:
    for service in ROLE_SERVICES:
        environment = _inspect_container(project, service)["Config"]["Env"]
        token_entries = [item for item in environment if item.startswith("BOT_TOKEN=")]
        if service == "telegram-delivery-worker":
            assert token_entries == [f"BOT_TOKEN={token}"]
        else:
            assert token_entries == []
            assert all(token not in item for item in environment)


def _role_logs(project: str) -> str:
    services = (*ROLE_SERVICES, "fake-telegram", "kafka-init")
    result = _compose(project, "logs", "--no-color", *services, capture=True)
    return result.stdout + result.stderr


def _assert_content_free_logs(project: str, token: str) -> None:
    logs = _role_logs(project)
    assert "BL22_STAGE6_PRIVATE_CONTENT" not in logs
    assert token not in logs
    assert re.search(r"x{80,}", logs) is None


def _assert_transition_logs(project: str, snapshot: dict) -> None:
    logs = _role_logs(project)
    root_event_id = re.escape(snapshot["root_event_id"])
    correlation_id = re.escape(snapshot["correlation_id"])
    run_id = re.escape(snapshot["run_id"])
    assert re.search(
        rf"event_id={root_event_id} correlation_id={correlation_id} attempt=\d+ state=publishing",
        logs,
    )
    assert re.search(
        rf"event_id={root_event_id} correlation_id={correlation_id} attempt=\d+ state=published",
        logs,
    )
    assert re.search(
        rf"event_id={root_event_id} correlation_id={correlation_id} run_id={run_id} attempt=1 state=delivering",
        logs,
    )
    for message_id in snapshot["message_ids"]:
        assert re.search(
            rf"correlation_id={correlation_id} run_id={run_id} message_id={re.escape(message_id)} attempt=1 state=sent",
            logs,
        )
    assert "state=retry_wait" in logs
    assert "state=dead_letter" in logs


def _cleanup_isolated(project: str, image: str) -> list[str]:
    errors: list[str] = []
    try:
        down = _compose(
            project,
            "down",
            "--volumes",
            "--remove-orphans",
            "--timeout",
            "10",
            timeout=120,
            check=False,
            capture=True,
        )
        if down.returncode != 0:
            errors.append(f"compose down exited {down.returncode}")
    except Exception as exc:
        errors.append(f"compose down raised {type(exc).__name__}")
    try:
        image_rm = _run(["docker", "image", "rm", "-f", image], timeout=120, check=False, capture=True)
    except Exception as exc:
        errors.append(f"image removal raised {type(exc).__name__}")
        image_rm = None

    resource_commands = {
        "containers": ["docker", "ps", "-aq", "--filter", f"label=com.docker.compose.project={project}"],
        "volumes": ["docker", "volume", "ls", "-q", "--filter", f"label=com.docker.compose.project={project}"],
        "networks": ["docker", "network", "ls", "-q", "--filter", f"label=com.docker.compose.project={project}"],
    }
    for kind, command in resource_commands.items():
        try:
            result = _run(command, timeout=30, check=False, capture=True)
            if result.returncode != 0:
                errors.append(f"{kind} inspection exited {result.returncode}")
            elif result.stdout.strip():
                errors.append(f"isolated {kind} remain")
        except Exception as exc:
            errors.append(f"{kind} inspection raised {type(exc).__name__}")

    try:
        image_inspect = _run(["docker", "image", "inspect", image], timeout=30, check=False, capture=True)
        if image_inspect.returncode == 0:
            errors.append("isolated image remains")
        elif image_rm is not None and image_rm.returncode != 0 and "No such image" not in image_rm.stderr:
            errors.append(f"image removal exited {image_rm.returncode}")
    except Exception as exc:
        errors.append(f"image inspection raised {type(exc).__name__}")
    return errors


@pytest.mark.e2e
@pytest.mark.stage6
def test_bl22_stage6_isolated_postgres_kafka_process_recovery(monkeypatch) -> None:
    if os.environ.get("BL22_STAGE6_E2E") != "1":
        pytest.skip("set BL22_STAGE6_E2E=1 to run the isolated Docker process E2E")

    run_id = uuid4()
    project = f"tpb-bl22-stage6-{uuid4().hex}"
    image = f"telegram-parser-bot:bl22-stage6-e2e-{run_id.hex}"
    token = f"123456:{uuid4().hex}"
    monkeypatch.setenv("BL22_STAGE6_RUN_ID", str(run_id))
    monkeypatch.setenv("BL22_STAGE6_IMAGE", image)
    monkeypatch.setenv("BL22_STAGE6_BOT_TOKEN", token)
    evidence: list[str] = []
    body_error: BaseException | None = None
    try:
        _compose(project, "build", "migrate", timeout=300)
        _compose(project, "up", "-d", "postgres", "kafka", "fake-telegram", timeout=180)
        _wait_service_health(project, "postgres")
        _wait_service_health(project, "kafka")
        _wait_service_health(project, "fake-telegram")

        # The first database operation on the new volume is the full migration chain.
        _compose(project, "run", "--rm", "--no-deps", "migrate", timeout=180)
        _compose(project, "run", "--rm", "--no-deps", "kafka-init", timeout=120)
        _compose(project, "up", "-d", "scheduler")
        _wait_role_log(project, "scheduler", "reliable scheduler active")
        _compose(project, "up", "-d", "outbox-relay")
        _wait_role_log(project, "outbox-relay", "outbox relay ready")
        _compose(project, "up", "-d", "digest-worker")
        _wait_role_log(project, "digest-worker", "digest worker active")
        _compose(project, "up", "-d", "telegram-delivery-worker")
        _wait_role_log(project, "telegram-delivery-worker", "Telegram delivery worker active")
        _compose(project, "up", "-d", "app")
        _wait_role_log(project, "app", "Digest delivery job not scheduled because BOT_TOKEN is empty")
        _wait_role_log(project, "app", "Scheduler started")
        _assert_token_isolation(project, token)
        evidence.append("role_token_isolation")
        _assert_role_readiness_matrix(project)
        evidence.append("semantic_role_readiness_stop_start_matrix")

        # Kafka outage leaves durable work. Recovery reaches broker ACK, then the
        # E2E-only relay hook exits before PostgreSQL can mark the row published.
        _compose(project, "stop", "kafka")
        normal = _control(project, "seed", "kafka-outage", "--posts", "40")
        normal_id = normal["subscription_id"]
        assert normal["pending"] == 40
        _control(project, "wait", str(normal_id), "outbox-failed", "--timeout", "25", timeout=40)
        # Freeze the observed failed state and drop the outage-era producer so
        # cancelled ambiguous sends cannot flush when the broker reconnects.
        _compose(project, "stop", "outbox-relay")
        outage_metrics = _control(project, "metrics")
        assert outage_metrics["unpublished_outbox"]["count"] >= 1
        assert outage_metrics["unpublished_outbox"]["oldest_age_seconds"] >= 0
        assert outage_metrics["pending_retries"]["outbox"] >= 1
        legacy = _control(project, "legacy-cycle", str(normal_id))
        assert legacy == {
            "subscription_id": normal_id,
            "due": True,
            "pending": 40,
            "legacy_delivered": 0,
            "legacy_sender_calls": 0,
            "legacy_delivery_count": 0,
            "reliable_run_count": 1,
        }
        evidence.append("actual_legacy_scheduler_cycle_excludes_reliable_owner")
        evidence.append("kafka_outage_persists_durable_root_retry")
        _control(project, "expedite-root-retry", str(normal_id))
        _compose(project, "start", "kafka")
        _wait_service_health(project, "kafka", timeout=180)
        _compose(project, "start", "outbox-relay")
        _wait_container_exit(project, "outbox-relay", 86)
        _assert_service_not_ready(project, "outbox-relay")
        first_publication = _control(
            project, "audit-root", str(normal_id), "--expected-count", "1", "--timeout", "20", timeout=45
        )
        crashed_relay = _control(project, "snapshot", str(normal_id))
        assert crashed_relay["root_state"] == "publishing"
        assert crashed_relay["root_offset"] is None
        assert crashed_relay["root_sha256"] == first_publication["publications"][0]["sha256"]

        # The digest handler commits all parts/inbox, then exits before offset commit.
        _wait_container_exit(project, "digest-worker", 87)
        _assert_service_not_ready(project, "digest-worker")
        post_db = _control(project, "snapshot", str(normal_id))
        assert post_db["run_state"] == "delivering"
        assert post_db["message_count"] == 3
        assert post_db["inbox_processing_attempts"] == 1

        # Restarting the same containers preserves one-shot markers. Lease expiry
        # republishes identical bytes, and completed inbox state commits both copies.
        _compose(project, "start", "outbox-relay")
        _wait_service_health(project, "outbox-relay")
        _control(project, "wait", str(normal_id), "root-published", "--timeout", "45", timeout=55)
        publications = _control(
            project, "audit-root", str(normal_id), "--expected-count", "2", "--timeout", "20", timeout=45
        )
        assert publications["publications"][0]["sha256"] == publications["publications"][1]["sha256"]
        _compose(project, "start", "digest-worker")
        _wait_service_health(project, "digest-worker")
        _control(project, "wait", str(normal_id), "completed", "--timeout", "45", timeout=55)
        _wait_consumer_offset(
            project,
            "digest-renderer-v1",
            ROOT_TOPIC,
            max(item["offset"] for item in publications["publications"]) + 1,
        )
        normal_snapshot = _control(project, "snapshot", str(normal_id))
        _assert_snapshot(normal_snapshot, posts=40, parts=3, attempts=[1, 1, 1])
        assert normal_snapshot["inbox_processing_attempts"] == 1
        assert len(_control(project, "fake-state")["calls"]) == 3
        evidence.extend(("kafka_ack_pre_db_crash_recovery", "digest_db_pre_offset_crash_dedup"))

        # A separate process recovers deliberately expired inbox and render leases.
        _control(project, "fake-plan", "200")
        _compose(project, "stop", "digest-worker")
        lease = _control(project, "seed", "lease-expiry", "--posts", "1")
        lease_id = lease["subscription_id"]
        _control(project, "wait", str(lease_id), "root-published", "--timeout", "20", timeout=45)
        _control(project, "inject-expired-render-lease", str(lease_id))
        lease_metrics = _control(project, "metrics")
        assert lease_metrics["expired_leases"]["runs"] >= 1
        assert lease_metrics["expired_leases"]["inbox"] >= 1
        assert lease_metrics["expired_leases"]["count"] >= 2
        _compose(project, "start", "digest-worker")
        _wait_service_health(project, "digest-worker")
        _control(project, "wait", str(lease_id), "lease-recovered", "--timeout", "30", timeout=40)
        lease_snapshot = _control(project, "snapshot", str(lease_id))
        _assert_snapshot(lease_snapshot, posts=1, parts=1)
        assert lease_snapshot["render_attempts"] == 2
        assert lease_snapshot["inbox_processing_attempts"] == 2
        assert len(_control(project, "fake-state")["calls"]) == 1
        evidence.append("expired_inbox_and_render_lease_recovery")

        # Fake Telegram accepts a message and assigns an ID but withholds the
        # response past sender timeout. The retry is a second accepted visible send.
        _control(project, "fake-plan", "accept_timeout", "200")
        ambiguous = _control(project, "seed", "ambiguous-send", "--posts", "1")
        ambiguous_id = ambiguous["subscription_id"]
        _control(project, "wait", str(ambiguous_id), "ambiguous-retry", "--timeout", "25", timeout=35)
        _compose(project, "stop", "telegram-delivery-worker")
        ambiguous_first = _control(project, "fake-state")["calls"]
        assert len(ambiguous_first) == 1
        assert ambiguous_first[0]["status"] == "accept_timeout"
        assert ambiguous_first[0]["accepted"] is True
        retry_snapshot = _control(project, "snapshot", str(ambiguous_id))
        assert retry_snapshot["message_ambiguous"] == [True]
        assert retry_snapshot["message_attempts"] == [1]
        retry_metrics = _control(project, "metrics")
        assert retry_metrics["pending_retries"]["messages"] >= 1
        assert retry_metrics["pending_retries"]["oldest_age_seconds"] >= 0
        _control(project, "expedite-retry", str(ambiguous_id))
        _compose(project, "start", "telegram-delivery-worker")
        _wait_service_health(project, "telegram-delivery-worker")
        _control(project, "wait", str(ambiguous_id), "completed", "--timeout", "30", timeout=40)
        ambiguous_calls = _control(project, "fake-state")["calls"]
        assert len(ambiguous_calls) == 2
        assert ambiguous_calls[0]["text_sha256"] == ambiguous_calls[1]["text_sha256"]
        assert ambiguous_calls[0]["message_id"] != ambiguous_calls[1]["message_id"]
        ambiguous_snapshot = _control(project, "snapshot", str(ambiguous_id))
        _assert_snapshot(ambiguous_snapshot, posts=1, parts=1, attempts=[2])
        assert ambiguous_snapshot["message_ambiguous"] == [True]
        assert ambiguous_snapshot["telegram_message_ids"] == [ambiguous_calls[1]["message_id"]]
        evidence.append("accepted_no_response_ambiguous_visible_duplicate")

        # Three persisted parts: two succeed, one receives durable 429; restart
        # sends only the remaining fingerprint.
        _control(project, "fake-plan", "200", "200", "429", "200")
        partial = _control(project, "seed", "partial-restart", "--posts", "40")
        partial_id = partial["subscription_id"]
        _control(project, "wait", str(partial_id), "partial", "--timeout", "35", timeout=45)
        _compose(project, "stop", "telegram-delivery-worker")
        before_restart = _control(project, "fake-state")["calls"]
        assert [call["status"] for call in before_restart] == [200, 200, 429]
        _control(project, "expedite-retry", str(partial_id))
        _compose(project, "start", "telegram-delivery-worker")
        _wait_service_health(project, "telegram-delivery-worker")
        _control(project, "wait", str(partial_id), "completed", "--timeout", "30", timeout=40)
        after_restart = _control(project, "fake-state")["calls"]
        assert len(after_restart) == 4
        assert after_restart[2]["text_sha256"] == after_restart[3]["text_sha256"]
        assert sum(call["text_sha256"] == after_restart[0]["text_sha256"] for call in after_restart) == 1
        assert sum(call["text_sha256"] == after_restart[1]["text_sha256"] for call in after_restart) == 1
        partial_snapshot = _control(project, "snapshot", str(partial_id))
        _assert_snapshot(partial_snapshot, posts=40, parts=3)
        assert sorted(partial_snapshot["message_attempts"]) == [1, 1, 2]
        evidence.append("three_part_restart_only_remaining_part")

        # Two transient failures exhaust the durable budget and publish DLQ.
        _control(project, "fake-plan", "500", "500")
        terminal = _control(project, "seed", "terminal-dlq", "--posts", "1")
        terminal_id = terminal["subscription_id"]
        _control(project, "wait", str(terminal_id), "retrying", "--timeout", "25", timeout=35)
        _compose(project, "stop", "telegram-delivery-worker")
        _control(project, "expedite-retry", str(terminal_id))
        _compose(project, "start", "telegram-delivery-worker")
        _wait_service_health(project, "telegram-delivery-worker")
        terminal_wait = _control(project, "wait", str(terminal_id), "terminal", "--timeout", "40", timeout=50)
        dead_letter_id = terminal_wait["dead_letter_id"]
        assert [call["status"] for call in _control(project, "fake-state")["calls"]] == [500, 500]
        audit = _control(project, "audit-kafka", dead_letter_id, "--timeout", "20", timeout=45)
        assert audit["dlq"]["topic"] == "tpb.telegram.delivery.requested.dlq.v1"
        dlq_metrics = _control(project, "metrics")
        assert dlq_metrics["open_dead_letters"]["count"] >= 1
        assert dlq_metrics["open_dead_letters"]["oldest_age_seconds"] >= 0
        evidence.append("retry_exhaustion_db_and_kafka_dlq_content_free")

        # Chromium drives list, detail and replay controls. A duplicate request with
        # the captured UI idempotency key returns the same audit/outbox result.
        _control(project, "fake-plan", "200")
        app_port = _compose(project, "port", "app", "8080", capture=True).stdout.strip()
        _run(
            ["npx", "playwright", "test", "--config", "playwright.bl22-stage6.config.js"],
            env={"BL22_ADMIN_URL": f"http://{app_port}", "BL22_DEAD_LETTER_ID": dead_letter_id},
            timeout=90,
        )
        _control(project, "wait", str(terminal_id), "completed", "--timeout", "30", timeout=40)
        replay = _control(project, "assert-replay", dead_letter_id)
        assert replay["generation"] == 2
        terminal_snapshot = _control(project, "snapshot", str(terminal_id))
        _assert_snapshot(terminal_snapshot, posts=1, parts=1, attempts=[1])
        assert len(_control(project, "fake-state")["calls"]) == 1
        evidence.append("browser_controls_single_replay_idempotency")

        evidence.append("reliability_metrics_at_failure_states")
        _assert_transition_logs(project, normal_snapshot)
        _assert_content_free_logs(project, token)
        evidence.append("structured_content_free_transition_logs")
        assert len(evidence) == 13
        print(json.dumps({"bl22_stage6": "passed", "scenarios": evidence}, sort_keys=True))
    except BaseException as exc:
        body_error = exc
        raise
    finally:
        try:
            cleanup_errors = _cleanup_isolated(project, image)
        except Exception as cleanup_exc:
            cleanup_errors = [f"cleanup orchestration raised {type(cleanup_exc).__name__}"]
        if cleanup_errors:
            message = "BL-22 isolated cleanup failed: " + "; ".join(cleanup_errors)
            if body_error is not None:
                body_error.add_note(message)
            else:
                raise AssertionError(message)
