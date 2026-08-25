from __future__ import annotations

from pathlib import Path

import pytest

from src.config.settings import Settings


def _clear_tokens(monkeypatch) -> None:
    for name in ("BOT_TOKEN", "TELEGRAM_TOKEN", "BOT_TOKEN_FILE"):
        monkeypatch.delenv(name, raising=False)


def test_bot_token_file_loads_private_secret(monkeypatch, tmp_path: Path) -> None:
    _clear_tokens(monkeypatch)
    token_file = tmp_path / "bot.token"
    token_file.write_text("123456:abcdefghijklmnopqrstuvwxyz_ABCDE\n", encoding="ascii")
    token_file.chmod(0o600)
    monkeypatch.setenv("BOT_TOKEN_FILE", str(token_file))
    settings = Settings.from_toml(tmp_path / "missing.toml")
    assert settings.bot.token == "123456:abcdefghijklmnopqrstuvwxyz_ABCDE"


def test_bot_token_file_is_mutually_exclusive_with_regular_token(monkeypatch, tmp_path: Path) -> None:
    _clear_tokens(monkeypatch)
    token_file = tmp_path / "bot.token"
    token_file.write_text("file-token\n", encoding="ascii")
    token_file.chmod(0o600)
    monkeypatch.setenv("BOT_TOKEN_FILE", str(token_file))
    monkeypatch.setenv("BOT_TOKEN", "environment-token")
    with pytest.raises(ValueError, match="mutually exclusive"):
        Settings.from_toml(tmp_path / "missing.toml")


def test_bot_token_file_rejects_missing_or_public_file(monkeypatch, tmp_path: Path) -> None:
    _clear_tokens(monkeypatch)
    missing = tmp_path / "missing.token"
    monkeypatch.setenv("BOT_TOKEN_FILE", str(missing))
    with pytest.raises(ValueError, match="absolute regular file"):
        Settings.from_toml(tmp_path / "missing.toml")

    token_file = tmp_path / "public.token"
    token_file.write_text("token\n", encoding="ascii")
    token_file.chmod(0o644)
    monkeypatch.setenv("BOT_TOKEN_FILE", str(token_file))
    with pytest.raises(ValueError, match="group or other"):
        Settings.from_toml(tmp_path / "missing.toml")


def test_regular_bot_token_behavior_is_unchanged(monkeypatch, tmp_path: Path) -> None:
    _clear_tokens(monkeypatch)
    monkeypatch.setenv("BOT_TOKEN", "regular-token")
    assert Settings.from_toml(tmp_path / "missing.toml").bot.token == "regular-token"

    monkeypatch.delenv("BOT_TOKEN")
    monkeypatch.setenv("TELEGRAM_TOKEN", "fallback-token")
    assert Settings.from_toml(tmp_path / "missing.toml").bot.token == "fallback-token"

    monkeypatch.setenv("BOT_TOKEN", "")
    assert Settings.from_toml(tmp_path / "missing.toml").bot.token == "fallback-token"
