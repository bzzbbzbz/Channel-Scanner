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

    model_config = {"env_prefix": "", "populate_by_name": True}


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


class BotSettings(BaseSettings):
    """Telegram bot runtime settings."""

    enabled: bool = Field(default=True, description="Enable Telegram bot runtime")
    polling: bool = Field(default=True, description="Use Bot API polling")
    token: str = Field(default="", description="Telegram bot token")
    set_commands_on_startup: bool = Field(
        default=True,
        description="Set Telegram bot commands during startup",
    )
    drop_pending_updates: bool = Field(
        default=False,
        description="Drop pending Bot API updates on startup",
    )
    default_timezone: str = Field(
        default="UTC",
        description="Default timezone for newly registered users",
    )
    e2e_allowed_chat_id: int | None = Field(
        default=None,
        description="Allow one non-private E2E chat in addition to private chats",
    )

    model_config = {"env_prefix": "BOT_"}


class LlmSettings(BaseSettings):
    """OpenRouter-compatible LLM settings."""

    openrouter_api_key: str = Field(
        default="",
        description="OpenRouter API key",
        validation_alias="OPENROUTER_API_KEY",
    )
    openrouter_base_url: str = Field(
        default="https://openrouter.ai/api/v1",
        description="OpenRouter-compatible base URL",
        validation_alias="OPENROUTER_BASE_URL",
    )
    timeout_seconds: float = Field(
        default=30.0,
        description="LLM request timeout in seconds",
        validation_alias="LLM_TIMEOUT_SECONDS",
    )

    model_config = {"env_prefix": "", "populate_by_name": True}


class AssistantSettings(BaseSettings):
    """Natural-language assistant runtime settings."""

    enabled: bool = Field(default=True, description="Enable free-text assistant handling")
    history_limit: int = Field(default=30, description="Recent chat messages passed to the assistant")
    max_tool_rounds: int = Field(default=5, description="Maximum tool-calling rounds per assistant turn")

    model_config = {"env_prefix": "ASSISTANT_"}


class MemorySettings(BaseSettings):
    """mem0-backed semantic memory settings."""

    enabled: bool = Field(default=True, description="Enable long-term semantic memory")
    collection_name: str = Field(default="telegram_parser_bot_memories", description="mem0 vector collection")
    qdrant_path: str = Field(default=".data/mem0/qdrant", description="Local Qdrant storage path used by mem0")
    history_db_path: str = Field(default=".data/mem0/history.db", description="mem0 history SQLite path")
    embedder_model: str = Field(default="qwen/qwen3-embedding-8b", description="OpenRouter-compatible embedding model")
    embedding_dims: int = Field(default=4096, description="Embedding vector dimensions")

    model_config = {"env_prefix": "MEMORY_"}


class Settings(BaseSettings):
    """Root settings — loads config.toml, then applies env var overrides."""

    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    scraper: ScraperSettings = Field(default_factory=ScraperSettings)
    scheduler: SchedulerSettings = Field(default_factory=SchedulerSettings)
    bot: BotSettings = Field(default_factory=BotSettings)
    llm: LlmSettings = Field(default_factory=LlmSettings)
    assistant: AssistantSettings = Field(default_factory=AssistantSettings)
    memory: MemorySettings = Field(default_factory=MemorySettings)
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

        bot_raw = toml_data.get("bot", {})
        if "BOT_TOKEN" not in os.environ and "TELEGRAM_TOKEN" in os.environ:
            bot_raw["token"] = os.environ["TELEGRAM_TOKEN"]
        if "E2E_CHAT_ID" in os.environ:
            bot_raw["e2e_allowed_chat_id"] = int(os.environ["E2E_CHAT_ID"])

        assistant_raw = toml_data.get("assistant", {})
        if "ASSISTANT_ENABLED" in os.environ:
            assistant_raw["enabled"] = os.environ["ASSISTANT_ENABLED"].lower() in {"1", "true", "yes", "on"}
        if "ASSISTANT_HISTORY_LIMIT" in os.environ:
            assistant_raw["history_limit"] = int(os.environ["ASSISTANT_HISTORY_LIMIT"])
        if "ASSISTANT_MAX_TOOL_ROUNDS" in os.environ:
            assistant_raw["max_tool_rounds"] = int(os.environ["ASSISTANT_MAX_TOOL_ROUNDS"])

        memory_raw = toml_data.get("memory", {})
        if "MEMORY_ENABLED" in os.environ:
            memory_raw["enabled"] = os.environ["MEMORY_ENABLED"].lower() in {"1", "true", "yes", "on"}
        if "MEMORY_COLLECTION_NAME" in os.environ:
            memory_raw["collection_name"] = os.environ["MEMORY_COLLECTION_NAME"]
        if "MEMORY_QDRANT_PATH" in os.environ:
            memory_raw["qdrant_path"] = os.environ["MEMORY_QDRANT_PATH"]
        if "MEMORY_HISTORY_DB_PATH" in os.environ:
            memory_raw["history_db_path"] = os.environ["MEMORY_HISTORY_DB_PATH"]
        if "MEMORY_EMBEDDER_MODEL" in os.environ:
            memory_raw["embedder_model"] = os.environ["MEMORY_EMBEDDER_MODEL"]
        if "MEMORY_EMBEDDING_DIMS" in os.environ:
            memory_raw["embedding_dims"] = int(os.environ["MEMORY_EMBEDDING_DIMS"])

        return cls(
            database=DatabaseSettings(**database_raw),
            scraper=ScraperSettings(**toml_data.get("scraper", {})),
            scheduler=SchedulerSettings(**toml_data.get("scheduler", {})),
            bot=BotSettings(**bot_raw),
            llm=LlmSettings(**toml_data.get("llm", {})),
            assistant=AssistantSettings(**assistant_raw),
            memory=MemorySettings(**memory_raw),
            logging=LoggingSettings(**toml_data.get("logging", {})),
        )


@lru_cache
def get_settings() -> Settings:
    """Cached settings singleton."""
    return Settings.from_toml()
