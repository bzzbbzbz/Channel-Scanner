"""Fail-closed process fault hooks available only to the isolated stage-6 harness."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse
from uuid import UUID

from src.config.settings import Settings

ISOLATED_BOT_API_URL = "http://fake-telegram:8081"
_SENTINEL = Path("/run/bl22-stage6/isolated.guard")
_SENTINEL_CONTENT = "telegram-parser-bot BL-22 stage-6 isolated E2E only\n"


@dataclass(frozen=True, slots=True)
class IsolatedE2EContext:
    run_id: UUID

    def crash_once(self, fault: str, exit_code: int) -> None:
        enabled = {item.strip() for item in os.environ.get("BL22_STAGE6_FAULTS", "").split(",") if item.strip()}
        if fault not in enabled:
            return
        marker = Path(f"/tmp/bl22-stage6-{self.run_id}-{fault}.done")
        try:
            descriptor = os.open(marker, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            return
        with os.fdopen(descriptor, "w", encoding="ascii") as stream:
            stream.write(f"{fault}\n")
            stream.flush()
            os.fsync(stream.fileno())
        os._exit(exit_code)


def isolated_e2e_context(settings: Settings, *, sentinel: Path = _SENTINEL) -> IsolatedE2EContext | None:
    """Return the capability only when every isolation boundary is present."""
    requested = os.environ.get("BL22_STAGE6_E2E") == "1"
    has_faults = bool(os.environ.get("BL22_STAGE6_FAULTS"))
    non_production_api = settings.bot.api_base_url != "https://api.telegram.org"
    if not requested:
        if has_faults or non_production_api:
            raise RuntimeError("BL-22 E2E faults and custom Bot API require the isolated stage-6 capability")
        return None

    try:
        run_id = UUID(os.environ["BL22_STAGE6_RUN_ID"])
    except (KeyError, ValueError) as exc:
        raise RuntimeError("BL22_STAGE6_RUN_ID must be a UUID") from exc
    parsed_db = urlparse(settings.database.url.replace("postgresql+asyncpg", "postgresql", 1))
    if parsed_db.hostname != "postgres" or parsed_db.path != "/telegram_bot":
        raise RuntimeError("BL-22 stage-6 capability requires the isolated PostgreSQL service")
    if settings.kafka.bootstrap_servers != "kafka:9092":
        raise RuntimeError("BL-22 stage-6 capability requires the isolated Kafka service")
    if not sentinel.is_file() or sentinel.read_text(encoding="ascii") != _SENTINEL_CONTENT:
        raise RuntimeError("BL-22 stage-6 capability sentinel is missing")
    if non_production_api and settings.bot.api_base_url != ISOLATED_BOT_API_URL:
        raise RuntimeError("Only the fixed isolated fake Bot API is permitted")
    return IsolatedE2EContext(run_id)
