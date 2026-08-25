"""Source-level assertions for fail-closed Compose shadow configuration."""

from pathlib import Path


def test_compose_shadow_roles_use_semantic_readiness_and_app_requires_explicit_kafka_switch() -> None:
    root = Path(__file__).resolve().parents[2]
    compose = (root / "docker-compose.yml").read_text(encoding="utf-8")
    env_example = (root / ".env.example").read_text(encoding="utf-8")

    assert 'KAFKA_ENABLED: ${KAFKA_ENABLED:-0}' in compose
    assert 'KAFKA_ENABLED: "1"' in compose
    assert "RELIABLE_DIGEST_SUBSCRIPTION_IDS: ${RELIABLE_DIGEST_SUBSCRIPTION_IDS:-[]}" in compose
    assert '["CMD", "python", "-m", "src.reliability.readiness", "check"]' in compose
    assert "RELIABILITY_ROLE_HEALTH_FILE: /tmp/tpb-role.ready" in compose
    for role in ("scheduler", "outbox-relay", "digest-worker", "telegram-delivery-worker"):
        assert f"RELIABILITY_ROLE_NAME: {role}" in compose
    assert "KAFKA_ENABLED=0" in env_example
    assert "RELIABLE_DIGEST_SUBSCRIPTION_IDS=[]" in env_example
    assert "operations probe requires KAFKA_ENABLED=1" in env_example
