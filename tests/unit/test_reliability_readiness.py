from pathlib import Path

from src.reliability.readiness import RoleReadiness, role_is_ready


def test_role_readiness_is_bound_to_current_process_generation(tmp_path: Path) -> None:
    path = tmp_path / "role.ready"
    readiness = RoleReadiness("digest-worker", path)

    assert role_is_ready("digest-worker", path) is False
    readiness.mark_ready()
    assert role_is_ready("digest-worker", path) is True
    assert role_is_ready("outbox-relay", path) is False

    path.write_text("digest-worker:1:stale\n", encoding="ascii")
    assert role_is_ready("digest-worker", path) is False

    readiness.clear()
    assert path.exists() is False
