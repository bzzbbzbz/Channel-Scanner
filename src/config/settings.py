"""Pydantic settings loaded from TOML config + env var overrides."""

from __future__ import annotations

import pathlib
import sys
from functools import lru_cache
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings

if sys.version_info >= (3, 11):
    import tomllib
else:
    try:
        import tomllib  # type: ignore[no-redef]
    except ImportError:
        import tomli as tomllib  # type: ignore[no-redef]

_CONFIG_PATH = pathlib.Path(__file__).resolve().parents[2] / "config.toml"


class DatabaseSettings(BaseSettings):
    """Database connection settings."""

    url: str = Field(
        default="postgresql+asyncpg://bot:bot@db:5432/telegram_bot",
        description="Async PostgreSQL connection URL",
    )
    pool_size: int = Field(default=5, description="Connection pool size")
    pool_recycle: int = Field(default=3600, description="Recycle connections after N seconds")

    model_config = {"env_prefix": ""}


class ScraperSettings(BaseSettings):
    """Scraper tuning parameters."""

    interval_seconds: int = Field(default=300, description="Seconds between scrape runs")
    rate_limit_per_sec: float = Field(default=1.0, description="Max requests per second")
    max_posts: int = Field(default=20, description="Max posts to fetch per channel per run")
    user_agent: str = Field(
        default="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        description="HTTP User-Agent header",
    )

    model_config = {"env_prefix": "SCRAPER_"}


class SchedulerSettings(BaseSettings):
    """Scheduler settings."""

    enabled: bool = Field(default=True, description="Enable periodic scraping")
    interval_minutes: int = Field(default=5, description="Minutes between scrape cycles")

    model_config = {"env_prefix": "SCHEDULER_"}


class LoggingSettings(BaseSettings):
    """Logging settings."""

    level: str = Field(default="INFO", description="Log level")
    format: str = Field(default="json", description="Log format: json or console")

    model_config = {"env_prefix": "LOG_"}


class Settings(BaseSettings):
    """Root settings — loads config.toml, then applies env var overrides."""

    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    scraper: ScraperSettings = Field(default_factory=ScraperSettings)
    scheduler: SchedulerSettings = Field(default_factory=SchedulerSettings)
    logging: LoggingSettings = Field(default_factory=LoggingSettings)

    model_config = {"env_prefix": ""}

    @classmethod
    def from_toml(cls, path: Optional[pathlib.Path] = None) -> "Settings":
        """Load settings from a TOML file, then apply env var overrides.

        Env vars take precedence: DATABASE_URL, DB_PASSWORD, etc.
        """
        config_path = path or _CONFIG_PATH
        toml_data: dict = {}

        if config_path.exists():
            with open(config_path, "rb") as f:
                toml_data = tomllib.load(f)

        # Extract sections; Pydantic will merge with defaults for missing keys
        database_raw = toml_data.get("database", {})
        # Env var override: DATABASE_URL and DB_PASSWORD
        import os

        if "DATABASE_URL" in os.environ:
            database_raw["url"] = os.environ["DATABASE_URL"]
        if "DB_PASSWORD" in os.environ:
            db_url = database_raw.get("url", "")
            # Replace password in URL
            if "@" in db_url:
                prefix, rest = db_url.rsplit("@", 1)
                # prefix is like postgresql+asyncpg://bot:OLD_PASS
                if ":" in prefix.split("://", 1)[-1]:
                    scheme_creds = prefix.rsplit(":", 1)[0]
                    database_raw["url"] = f"{scheme_creds}:{os.environ['DB_PASSWORD']}@{rest}"

        return cls(
            database=DatabaseSettings(**database_raw),
            scraper=ScraperSettings(**toml_data.get("scraper", {})),
            scheduler=SchedulerSettings(**toml_data.get("scheduler", {})),
            logging=LoggingSettings(**toml_data.get("logging", {})),
        )


@lru_cache
def get_settings() -> Settings:
    """Cached settings singleton."""
    return Settings.from_toml()
