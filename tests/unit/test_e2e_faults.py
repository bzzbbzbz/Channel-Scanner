from pathlib import Path
from uuid import uuid4

import pytest

from src.config.settings import Settings
from src.reliability.e2e_faults import ISOLATED_BOT_API_URL, isolated_e2e_context


def _settings(*, database_url="postgresql+asyncpg://bot:bot@postgres:5432/telegram_bot", api_url=ISOLATED_BOT_API_URL):
    return Settings.model_validate(
        {
            "database": {"url": database_url},
            "bot": {"api_base_url": api_url},
            "kafka": {"enabled": True, "bootstrap_servers": "kafka:9092"},
            "reliable_delivery": {"enabled": True, "all_subscriptions": True},
            "memory": {"enabled": False},
        }
    )


def test_isolated_e2e_capability_requires_flag_for_custom_api(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("BL22_STAGE6_E2E", raising=False)
    with pytest.raises(RuntimeError, match="isolated stage-6 capability"):
        isolated_e2e_context(_settings(), sentinel=tmp_path / "missing")


def test_isolated_e2e_capability_requires_external_sentinel(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("BL22_STAGE6_E2E", "1")
    monkeypatch.setenv("BL22_STAGE6_RUN_ID", str(uuid4()))
    with pytest.raises(RuntimeError, match="sentinel"):
        isolated_e2e_context(_settings(), sentinel=tmp_path / "missing")


def test_isolated_e2e_capability_rejects_nonisolated_database(monkeypatch, tmp_path: Path) -> None:
    sentinel = tmp_path / "guard"
    sentinel.write_text("telegram-parser-bot BL-22 stage-6 isolated E2E only\n", encoding="ascii")
    monkeypatch.setenv("BL22_STAGE6_E2E", "1")
    monkeypatch.setenv("BL22_STAGE6_RUN_ID", str(uuid4()))
    with pytest.raises(RuntimeError, match="isolated PostgreSQL"):
        isolated_e2e_context(
            _settings(database_url="postgresql+asyncpg://bot:bot@production-db:5432/telegram_bot"),
            sentinel=sentinel,
        )


def test_isolated_e2e_capability_accepts_all_independent_guards(monkeypatch, tmp_path: Path) -> None:
    sentinel = tmp_path / "guard"
    sentinel.write_text("telegram-parser-bot BL-22 stage-6 isolated E2E only\n", encoding="ascii")
    run_id = uuid4()
    monkeypatch.setenv("BL22_STAGE6_E2E", "1")
    monkeypatch.setenv("BL22_STAGE6_RUN_ID", str(run_id))
    assert isolated_e2e_context(_settings(), sentinel=sentinel).run_id == run_id


def test_stage7_default_bot_api_does_not_activate_stage6_custom_api_guard(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("BL22_STAGE6_E2E", raising=False)
    monkeypatch.delenv("BL22_STAGE6_FAULTS", raising=False)
    monkeypatch.setenv("BL22_STAGE7_E2E", "1")
    settings = _settings(api_url="https://api.telegram.org")
    assert isolated_e2e_context(settings, sentinel=tmp_path / "unused") is None
