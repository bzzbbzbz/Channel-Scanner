"""Pydantic settings loaded from TOML config + env var overrides."""

from __future__ import annotations

import json
import pathlib
import sys
from functools import lru_cache
from typing import Literal, Optional

from pydantic import Field, model_validator
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
    api_base_url: str = Field(
        default="https://api.telegram.org",
        description="Telegram Bot API base URL; override only for an isolated compatible endpoint",
    )
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
    max_subscriptions_per_user: int = Field(default=5, description="Maximum subscriptions one user can own")
    max_channels_per_subscription: int = Field(default=10, description="Maximum channels in one subscription")

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
    max_tool_rounds: int = Field(default=10, description="Maximum tool-calling rounds per assistant turn")
    max_tool_calls: int = Field(default=10, description="Maximum product tool executions per assistant turn")

    model_config = {"env_prefix": "ASSISTANT_"}


class KnowledgeSettings(BaseSettings):
    """Shared catalog and retrieval configuration for channel RAG."""

    enabled: bool = Field(default=True, description="Enable public channel knowledge search")
    administrator_telegram_ids: list[int] = Field(default_factory=list, description="Telegram IDs allowed to moderate catalog imports")
    qdrant_path: str = Field(default=".data/knowledge/qdrant", description="Dedicated local Qdrant storage path")
    collection_name: str = Field(default="telegram_channel_knowledge", description="Knowledge vector collection")
    embedding_model: str = Field(default="qwen/qwen3-embedding-8b", description="OpenRouter embedding model")
    enrichment_model: str = Field(default="deepseek/deepseek-v4-flash", description="Fixed model for knowledge metadata enrichment")
    catalog_description_model: str = Field(default="deepseek/deepseek-v4-flash", description="Fixed model for bounded catalog descriptions")
    answer_model: str = Field(default="deepseek/deepseek-v4-flash", description="Fixed model for grounded interactive RAG answers")
    embedding_dimensions: int = Field(default=4096, description="Knowledge vector dimensions")
    import_max_bytes: int = Field(default=100_000_000, description="Maximum uploaded Telegram export size")
    parent_context_limit: int = Field(default=1600, description="Token estimate for full parent context")
    sync_interval_hours: int = Field(default=4, description="Approved catalog synchronization interval")
    max_retry_attempts: int = Field(default=3, description="Maximum automatic enrichment or indexing attempts per record")
    retry_concurrency: int = Field(default=3, description="Maximum concurrent automatic knowledge retries")
    import_version: str = Field(default="1", description="Telegram import normalization version")
    enrichment_version: str = Field(default="1", description="Fixed metadata prompt version")
    chunking_version: str = Field(default="1", description="Representation chunking version")
    embedding_version: str = Field(default="1", description="Embedding configuration version")
    index_version: int = Field(default=1, description="Active Qdrant index version")
    short_post_max_tokens: int = Field(default=700, description="Largest post stored as one full representation")
    target_chunk_tokens: int = Field(default=450, description="Preferred paragraph-aware chunk size")
    min_chunk_tokens: int = Field(default=250, description="Smallest preferred combined chunk")
    max_chunk_tokens: int = Field(default=700, description="Largest chunk before sentence splitting")
    neighbor_expansion: int = Field(default=1, description="Sibling chunks added to long-post context")
    summary_enabled: bool = Field(default=True, description="Create validated summary retrieval cards")
    full_for_short_posts: bool = Field(default=True, description="Embed original short-post text")
    chunks_for_long_posts: bool = Field(default=True, description="Embed original long-post chunks")
    rag_rollout_enabled: bool = Field(default=False, description="Enable the candidate RAG variant for allowlisted canary users only")
    rag_canary_telegram_ids: list[int] = Field(default_factory=list, description="Telegram IDs allowed to receive the candidate RAG variant")
    rag_enabled_for_all_users: bool = Field(default=False, description="Enable the candidate RAG variant for every user; the canary allowlist stays as a fallback/escape hatch")
    rag_configuration_id: str = Field(default="bl24-rerank20-v2", min_length=1, max_length=64, description="Versioned non-secret RAG configuration identifier")
    rag_code_version: str = Field(default="bl24-2", min_length=1, max_length=64, description="Application code version paired with the RAG configuration")
    rag_configuration_operator: str = Field(default="config", max_length=128, description="Operator label for the RAG configuration audit")
    rag_query_instruction: str = Field(default="Represent this question for retrieving relevant public Telegram posts.", min_length=1, max_length=1000, description="Fixed query instruction used only by the candidate vector search")
    rag_reranker_model: str = Field(default="cohere/rerank-4-pro", min_length=1, max_length=255, description="Candidate reranker model")
    rag_rerank_candidate_limit: int = Field(default=20, ge=1, le=20, description="Maximum already-authorized canonical posts sent to the reranker")
    rag_rerank_estimated_cost_usd: float = Field(default=0.0025, ge=0, description="Conservative per-request rerank cost used for the preflight cap")
    rag_rerank_max_cost_usd: float = Field(default=0.01, ge=0, description="Maximum allowed estimated or reported cost of one rerank request")
    deepseek_api_key: str = Field(default="", description="Direct DeepSeek API key; empty keeps OpenRouter for knowledge answers")
    deepseek_base_url: str = Field(default="https://api.deepseek.com", description="OpenAI-compatible DeepSeek base URL")
    answer_direct_enabled: bool = Field(default=False, description="Use the direct DeepSeek API instead of OpenRouter for grounded answers")
    judge_model: str = Field(default="deepseek-v4-flash", description="Direct DeepSeek model used for semantic claim judging")
    judge_version: str = Field(default="1", min_length=1, max_length=64, description="Versioned semantic-judge prompt/rule identity")
    # Zero disables the corresponding guard for an instrumented RAG experiment.
    # Stage durations continue to be persisted, so this does not hide latency.
    catalog_selection_timeout_seconds: float = Field(default=0, ge=0)
    vector_retrieval_timeout_seconds: float = Field(default=0, ge=0)
    rerank_timeout_seconds: float = Field(default=0, ge=0)
    answer_timeout_seconds: float = Field(default=0, ge=0)
    rag_total_timeout_seconds: float = Field(default=0, ge=0)
    rag_provider_timeout_seconds: float = Field(default=0, ge=0, description="Per-request provider timeout for RAG; zero disables it")
    catalog_description_timeout_seconds: float = Field(default=30, gt=0, le=120, description="Bounded background catalog-description generation")

    model_config = {"env_prefix": "KNOWLEDGE_"}


class AdminSettings(BaseSettings):
    """Authenticated web dashboard settings."""

    enabled: bool = Field(default=False, description="Enable the admin dashboard HTTP server")
    host: str = Field(default="0.0.0.0", description="Dashboard HTTP bind host")
    port: int = Field(default=8080, description="Dashboard HTTP bind port")
    username: str = Field(default="", description="Administrator login name")
    password_hash: str = Field(default="", description="PBKDF2 password hash for the administrator")
    session_secret: str = Field(default="", description="Secret used to sign dashboard sessions")
    secure_cookies: bool = Field(default=True, description="Require HTTPS for dashboard session cookies")

    model_config = {"env_prefix": "ADMIN_"}


class MemorySettings(BaseSettings):
    """mem0-backed semantic memory settings."""

    enabled: bool = Field(default=True, description="Enable long-term semantic memory")
    collection_name: str = Field(default="telegram_parser_bot_memories", description="mem0 vector collection")
    qdrant_path: str = Field(default=".data/mem0/qdrant", description="Local Qdrant storage path used by mem0")
    history_db_path: str = Field(default=".data/mem0/history.db", description="mem0 history SQLite path")
    embedder_model: str = Field(default="qwen/qwen3-embedding-8b", description="OpenRouter-compatible embedding model")
    embedding_dims: int = Field(default=4096, description="Embedding vector dimensions")

    model_config = {"env_prefix": "MEMORY_"}


class KafkaSettings(BaseSettings):
    """BL-22 Kafka transport and topic provisioning settings."""

    enabled: bool = Field(default=False, description="Enable BL-22 Kafka shadow roles")
    bootstrap_servers: str = Field(default="kafka:9092", min_length=1)
    security_protocol: Literal["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"] = "PLAINTEXT"
    client_id_prefix: str = Field(default="telegram-parser-bot", min_length=1, max_length=128)
    request_timeout_ms: int = Field(default=10_000, ge=1_000, le=120_000)
    startup_timeout_seconds: int = Field(default=60, ge=5, le=300)
    topic_partitions: int = Field(default=1, ge=1)
    topic_replication_factor: int = Field(default=1, ge=1)
    topic_retention_ms: int = Field(default=604_800_000, ge=60_000)
    dlq_retention_ms: int = Field(default=2_592_000_000, ge=60_000)
    topic_retention_bytes: int = Field(default=536_870_912, ge=1_048_576)
    dlq_retention_bytes: int = Field(default=268_435_456, ge=1_048_576)
    max_event_bytes: int = Field(default=65_536, ge=1_024, le=131_072)
    outbox_lease_seconds: float = Field(default=30.0, ge=1, le=3600)
    outbox_publish_timeout_seconds: float = Field(default=10.0, gt=0, le=3600)
    outbox_poll_interval_seconds: float = Field(default=1.0, gt=0, le=60)
    outbox_batch_size: int = Field(default=100, ge=1, le=1000)
    outbox_backoff_base_seconds: float = Field(default=1.0, gt=0, le=3600)
    outbox_backoff_cap_seconds: float = Field(default=60.0, gt=0, le=86_400)

    model_config = {"env_prefix": "KAFKA_"}

    @model_validator(mode="after")
    def validate_outbox_publish_timeout(self) -> "KafkaSettings":
        if self.outbox_publish_timeout_seconds >= self.outbox_lease_seconds:
            raise ValueError("outbox_publish_timeout_seconds must be less than outbox_lease_seconds")
        return self


class ReliableDeliverySettings(BaseSettings):
    """Fail-closed rollout and worker tuning for reliable scheduled digests."""

    enabled: bool = Field(default=False, description="Master switch for the BL-22 reliable digest path")
    subscription_ids: list[int] = Field(default_factory=list, description="Subscription IDs owned by the reliable path")
    all_subscriptions: bool = Field(default=False, description="Route every scheduled subscription through the reliable path")
    poll_interval_seconds: float = Field(default=1.0, gt=0, le=60)
    consumer_poll_timeout_ms: int = Field(default=1000, ge=100, le=60_000)
    inbox_lease_seconds: float = Field(default=900.0, ge=1, le=7200)
    render_lease_seconds: float = Field(default=900.0, ge=1, le=7200)
    render_max_attempts: int = Field(default=5, ge=1, le=20)
    delivery_lease_seconds: float = Field(default=60.0, ge=1, le=3600)
    delivery_send_timeout_seconds: float = Field(default=30.0, gt=0, le=3600)
    delivery_max_attempts: int = Field(default=10, ge=1, le=100)
    delivery_backoff_base_seconds: float = Field(default=2.0, gt=0, le=3600)
    delivery_backoff_cap_seconds: float = Field(default=900.0, gt=0, le=86_400)
    render_memory_enabled: bool = Field(
        default=False,
        description="Reserved; stage-3 workers intentionally render with memory_service=None and no .data mount",
    )

    model_config = {"env_prefix": "RELIABLE_DIGEST_"}

    @model_validator(mode="after")
    def validate_rollout(self) -> "ReliableDeliverySettings":
        if any(subscription_id <= 0 for subscription_id in self.subscription_ids):
            raise ValueError("subscription_ids must contain positive IDs")
        if self.render_memory_enabled:
            raise ValueError("render_memory_enabled is unsupported until reliable workers have safe shared memory storage")
        if self.inbox_lease_seconds < self.render_lease_seconds:
            raise ValueError("inbox_lease_seconds must be greater than or equal to render_lease_seconds")
        if self.delivery_send_timeout_seconds >= self.delivery_lease_seconds:
            raise ValueError("delivery_send_timeout_seconds must be less than delivery_lease_seconds")
        if self.inbox_lease_seconds < self.delivery_lease_seconds:
            raise ValueError("inbox_lease_seconds must be greater than or equal to delivery_lease_seconds")
        if self.delivery_backoff_cap_seconds < self.delivery_backoff_base_seconds:
            raise ValueError("delivery_backoff_cap_seconds must be greater than or equal to delivery_backoff_base_seconds")
        if self.enabled and not self.all_subscriptions and not self.subscription_ids:
            raise ValueError("enabled reliable delivery requires subscription_ids or all_subscriptions=true")
        return self

    def owns_subscription(self, subscription_id: int) -> bool:
        return self.enabled and (self.all_subscriptions or subscription_id in self.subscription_ids)


class Settings(BaseSettings):
    """Root settings — loads config.toml, then applies env var overrides."""

    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    scraper: ScraperSettings = Field(default_factory=ScraperSettings)
    scheduler: SchedulerSettings = Field(default_factory=SchedulerSettings)
    bot: BotSettings = Field(default_factory=BotSettings)
    llm: LlmSettings = Field(default_factory=LlmSettings)
    assistant: AssistantSettings = Field(default_factory=AssistantSettings)
    knowledge: KnowledgeSettings = Field(default_factory=KnowledgeSettings)
    admin: AdminSettings = Field(default_factory=AdminSettings)
    memory: MemorySettings = Field(default_factory=MemorySettings)
    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    reliable_delivery: ReliableDeliverySettings = Field(default_factory=ReliableDeliverySettings)
    logging: LoggingSettings = Field(default_factory=LoggingSettings)

    model_config = {"env_prefix": ""}

    @model_validator(mode="after")
    def validate_reliable_delivery(self) -> "Settings":
        if self.reliable_delivery.enabled and not self.kafka.enabled:
            raise ValueError("reliable delivery requires kafka.enabled=true")
        if self.reliable_delivery.enabled and self.memory.enabled:
            raise ValueError("reliable delivery requires memory.enabled=false until workers have safe shared memory storage")
        return self

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

        bot_raw = toml_data.get("bot", {}).copy()
        bot_token = os.environ.get("BOT_TOKEN")
        telegram_token = os.environ.get("TELEGRAM_TOKEN")
        bot_token_file = os.environ.get("BOT_TOKEN_FILE", "").strip()
        if bot_token_file:
            if (bot_token and bot_token.strip()) or (telegram_token and telegram_token.strip()):
                raise ValueError("BOT_TOKEN_FILE is mutually exclusive with BOT_TOKEN and TELEGRAM_TOKEN")
            token_path = pathlib.Path(bot_token_file)
            if not token_path.is_absolute() or token_path.is_symlink() or not token_path.is_file():
                raise ValueError("BOT_TOKEN_FILE must be an absolute regular file")
            token_stat = token_path.stat()
            if token_stat.st_mode & 0o077:
                raise ValueError("BOT_TOKEN_FILE must not be accessible by group or other users")
            if token_stat.st_size < 1 or token_stat.st_size > 513:
                raise ValueError("BOT_TOKEN_FILE must contain exactly one non-empty token")
            token_from_file = token_path.read_text(encoding="utf-8").strip()
            if not token_from_file or len(token_from_file) > 512 or any(char.isspace() for char in token_from_file):
                raise ValueError("BOT_TOKEN_FILE must contain exactly one non-empty token")
            bot_raw["token"] = token_from_file
        elif bot_token and bot_token.strip():
            bot_raw["token"] = bot_token
        elif telegram_token is not None:
            bot_raw["token"] = telegram_token
        elif bot_token is not None:
            bot_raw["token"] = bot_token
        if "E2E_CHAT_ID" in os.environ:
            bot_raw["e2e_allowed_chat_id"] = int(os.environ["E2E_CHAT_ID"])
        if "BOT_API_BASE_URL" in os.environ:
            bot_raw["api_base_url"] = os.environ["BOT_API_BASE_URL"]
        if "BOT_MAX_SUBSCRIPTIONS_PER_USER" in os.environ:
            bot_raw["max_subscriptions_per_user"] = int(os.environ["BOT_MAX_SUBSCRIPTIONS_PER_USER"])
        if "BOT_MAX_CHANNELS_PER_SUBSCRIPTION" in os.environ:
            bot_raw["max_channels_per_subscription"] = int(os.environ["BOT_MAX_CHANNELS_PER_SUBSCRIPTION"])

        assistant_raw = toml_data.get("assistant", {})
        if "ASSISTANT_ENABLED" in os.environ:
            assistant_raw["enabled"] = os.environ["ASSISTANT_ENABLED"].lower() in {"1", "true", "yes", "on"}
        if "ASSISTANT_HISTORY_LIMIT" in os.environ:
            assistant_raw["history_limit"] = int(os.environ["ASSISTANT_HISTORY_LIMIT"])
        if "ASSISTANT_MAX_TOOL_ROUNDS" in os.environ:
            assistant_raw["max_tool_rounds"] = int(os.environ["ASSISTANT_MAX_TOOL_ROUNDS"])
        if "ASSISTANT_MAX_TOOL_CALLS" in os.environ:
            assistant_raw["max_tool_calls"] = int(os.environ["ASSISTANT_MAX_TOOL_CALLS"])

        knowledge_raw = toml_data.get("knowledge", {})
        if "KNOWLEDGE_ENABLED" in os.environ:
            knowledge_raw["enabled"] = os.environ["KNOWLEDGE_ENABLED"].lower() in {"1", "true", "yes", "on"}
        if "KNOWLEDGE_ADMINISTRATOR_TELEGRAM_IDS" in os.environ:
            knowledge_raw["administrator_telegram_ids"] = [
                int(value.strip())
                for value in os.environ["KNOWLEDGE_ADMINISTRATOR_TELEGRAM_IDS"].split(",")
                if value.strip()
            ]
        if "KNOWLEDGE_RAG_ROLLOUT_ENABLED" in os.environ:
            knowledge_raw["rag_rollout_enabled"] = os.environ["KNOWLEDGE_RAG_ROLLOUT_ENABLED"].lower() in {"1", "true", "yes", "on"}
        if "KNOWLEDGE_RAG_ENABLED_FOR_ALL_USERS" in os.environ:
            knowledge_raw["rag_enabled_for_all_users"] = os.environ["KNOWLEDGE_RAG_ENABLED_FOR_ALL_USERS"].lower() in {"1", "true", "yes", "on"}
        if "KNOWLEDGE_RAG_CANARY_TELEGRAM_IDS" in os.environ:
            knowledge_raw["rag_canary_telegram_ids"] = [
                int(value.strip())
                for value in os.environ["KNOWLEDGE_RAG_CANARY_TELEGRAM_IDS"].split(",")
                if value.strip()
            ]
        if "KNOWLEDGE_ANSWER_DIRECT_ENABLED" in os.environ:
            knowledge_raw["answer_direct_enabled"] = os.environ["KNOWLEDGE_ANSWER_DIRECT_ENABLED"].lower() in {"1", "true", "yes", "on"}
        if "KNOWLEDGE_DEEPSEEK_API_KEY" not in os.environ and "DEEPSEEK_API_KEY" in os.environ:
            knowledge_raw["deepseek_api_key"] = os.environ["DEEPSEEK_API_KEY"]
        for env_name, field_name, caster in (
            ("KNOWLEDGE_RAG_CONFIGURATION_ID", "rag_configuration_id", str),
            ("KNOWLEDGE_QDRANT_PATH", "qdrant_path", str),
            ("KNOWLEDGE_CATALOG_DESCRIPTION_MODEL", "catalog_description_model", str),
            ("KNOWLEDGE_ANSWER_MODEL", "answer_model", str),
            ("KNOWLEDGE_RAG_CODE_VERSION", "rag_code_version", str),
            ("KNOWLEDGE_RAG_CONFIGURATION_OPERATOR", "rag_configuration_operator", str),
            ("KNOWLEDGE_RAG_QUERY_INSTRUCTION", "rag_query_instruction", str),
            ("KNOWLEDGE_RAG_RERANKER_MODEL", "rag_reranker_model", str),
            ("KNOWLEDGE_RAG_RERANK_CANDIDATE_LIMIT", "rag_rerank_candidate_limit", int),
            ("KNOWLEDGE_RAG_RERANK_ESTIMATED_COST_USD", "rag_rerank_estimated_cost_usd", float),
            ("KNOWLEDGE_RAG_RERANK_MAX_COST_USD", "rag_rerank_max_cost_usd", float),
            ("KNOWLEDGE_DEEPSEEK_API_KEY", "deepseek_api_key", str),
            ("KNOWLEDGE_DEEPSEEK_BASE_URL", "deepseek_base_url", str),
            ("KNOWLEDGE_JUDGE_MODEL", "judge_model", str),
            ("KNOWLEDGE_JUDGE_VERSION", "judge_version", str),
            ("KNOWLEDGE_CATALOG_SELECTION_TIMEOUT_SECONDS", "catalog_selection_timeout_seconds", float),
            ("KNOWLEDGE_VECTOR_RETRIEVAL_TIMEOUT_SECONDS", "vector_retrieval_timeout_seconds", float),
            ("KNOWLEDGE_RERANK_TIMEOUT_SECONDS", "rerank_timeout_seconds", float),
            ("KNOWLEDGE_ANSWER_TIMEOUT_SECONDS", "answer_timeout_seconds", float),
            ("KNOWLEDGE_RAG_TOTAL_TIMEOUT_SECONDS", "rag_total_timeout_seconds", float),
            ("KNOWLEDGE_RAG_PROVIDER_TIMEOUT_SECONDS", "rag_provider_timeout_seconds", float),
            ("KNOWLEDGE_CATALOG_DESCRIPTION_TIMEOUT_SECONDS", "catalog_description_timeout_seconds", float),
        ):
            if env_name in os.environ:
                knowledge_raw[field_name] = caster(os.environ[env_name])

        admin_raw = toml_data.get("admin", {})
        if "ADMIN_ENABLED" in os.environ:
            admin_raw["enabled"] = os.environ["ADMIN_ENABLED"].lower() in {"1", "true", "yes", "on"}
        if "ADMIN_HOST" in os.environ:
            admin_raw["host"] = os.environ["ADMIN_HOST"]
        if "ADMIN_PORT" in os.environ:
            admin_raw["port"] = int(os.environ["ADMIN_PORT"])
        if "ADMIN_USERNAME" in os.environ:
            admin_raw["username"] = os.environ["ADMIN_USERNAME"]
        if "ADMIN_PASSWORD_HASH" in os.environ:
            admin_raw["password_hash"] = os.environ["ADMIN_PASSWORD_HASH"]
        if "ADMIN_SESSION_SECRET" in os.environ:
            admin_raw["session_secret"] = os.environ["ADMIN_SESSION_SECRET"]
        if "ADMIN_SECURE_COOKIES" in os.environ:
            admin_raw["secure_cookies"] = os.environ["ADMIN_SECURE_COOKIES"].lower() in {"1", "true", "yes", "on"}

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

        kafka_raw = toml_data.get("kafka", {}).copy()
        if "KAFKA_ENABLED" in os.environ:
            kafka_raw["enabled"] = os.environ["KAFKA_ENABLED"].lower() in {"1", "true", "yes", "on"}
        for env_name, field_name, caster in (
            ("KAFKA_BOOTSTRAP_SERVERS", "bootstrap_servers", str),
            ("KAFKA_SECURITY_PROTOCOL", "security_protocol", str),
            ("KAFKA_CLIENT_ID_PREFIX", "client_id_prefix", str),
            ("KAFKA_REQUEST_TIMEOUT_MS", "request_timeout_ms", int),
            ("KAFKA_STARTUP_TIMEOUT_SECONDS", "startup_timeout_seconds", int),
            ("KAFKA_TOPIC_PARTITIONS", "topic_partitions", int),
            ("KAFKA_TOPIC_REPLICATION_FACTOR", "topic_replication_factor", int),
            ("KAFKA_TOPIC_RETENTION_MS", "topic_retention_ms", int),
            ("KAFKA_DLQ_RETENTION_MS", "dlq_retention_ms", int),
            ("KAFKA_TOPIC_RETENTION_BYTES", "topic_retention_bytes", int),
            ("KAFKA_DLQ_RETENTION_BYTES", "dlq_retention_bytes", int),
            ("KAFKA_MAX_EVENT_BYTES", "max_event_bytes", int),
            ("KAFKA_OUTBOX_LEASE_SECONDS", "outbox_lease_seconds", float),
            ("KAFKA_OUTBOX_PUBLISH_TIMEOUT_SECONDS", "outbox_publish_timeout_seconds", float),
            ("KAFKA_OUTBOX_POLL_INTERVAL_SECONDS", "outbox_poll_interval_seconds", float),
            ("KAFKA_OUTBOX_BATCH_SIZE", "outbox_batch_size", int),
            ("KAFKA_OUTBOX_BACKOFF_BASE_SECONDS", "outbox_backoff_base_seconds", float),
            ("KAFKA_OUTBOX_BACKOFF_CAP_SECONDS", "outbox_backoff_cap_seconds", float),
        ):
            if env_name in os.environ:
                kafka_raw[field_name] = caster(os.environ[env_name])

        reliable_raw = toml_data.get("reliable_delivery", {}).copy()
        if "RELIABLE_DIGEST_ENABLED" in os.environ:
            reliable_raw["enabled"] = os.environ["RELIABLE_DIGEST_ENABLED"].lower() in {"1", "true", "yes", "on"}
        if "RELIABLE_DIGEST_SUBSCRIPTION_IDS" in os.environ:
            subscription_ids = json.loads(os.environ["RELIABLE_DIGEST_SUBSCRIPTION_IDS"])
            if not isinstance(subscription_ids, list):
                raise ValueError("RELIABLE_DIGEST_SUBSCRIPTION_IDS must be a JSON array")
            if any(type(value) is not int for value in subscription_ids):
                raise ValueError("RELIABLE_DIGEST_SUBSCRIPTION_IDS must contain only integers")
            reliable_raw["subscription_ids"] = subscription_ids
        for env_name, field_name, caster in (
            ("RELIABLE_DIGEST_ALL_SUBSCRIPTIONS", "all_subscriptions", lambda value: value.lower() in {"1", "true", "yes", "on"}),
            ("RELIABLE_DIGEST_POLL_INTERVAL_SECONDS", "poll_interval_seconds", float),
            ("RELIABLE_DIGEST_CONSUMER_POLL_TIMEOUT_MS", "consumer_poll_timeout_ms", int),
            ("RELIABLE_DIGEST_INBOX_LEASE_SECONDS", "inbox_lease_seconds", float),
            ("RELIABLE_DIGEST_RENDER_LEASE_SECONDS", "render_lease_seconds", float),
            ("RELIABLE_DIGEST_RENDER_MAX_ATTEMPTS", "render_max_attempts", int),
            ("RELIABLE_DIGEST_DELIVERY_LEASE_SECONDS", "delivery_lease_seconds", float),
            ("RELIABLE_DIGEST_DELIVERY_SEND_TIMEOUT_SECONDS", "delivery_send_timeout_seconds", float),
            ("RELIABLE_DIGEST_DELIVERY_MAX_ATTEMPTS", "delivery_max_attempts", int),
            ("RELIABLE_DIGEST_DELIVERY_BACKOFF_BASE_SECONDS", "delivery_backoff_base_seconds", float),
            ("RELIABLE_DIGEST_DELIVERY_BACKOFF_CAP_SECONDS", "delivery_backoff_cap_seconds", float),
            ("RELIABLE_DIGEST_RENDER_MEMORY_ENABLED", "render_memory_enabled", lambda value: value.lower() in {"1", "true", "yes", "on"}),
        ):
            if env_name in os.environ:
                reliable_raw[field_name] = caster(os.environ[env_name])

        scheduler_raw = toml_data.get("scheduler", {}).copy()
        if "SCHEDULER_ENABLED" in os.environ:
            scheduler_raw["enabled"] = os.environ["SCHEDULER_ENABLED"].lower() in {"1", "true", "yes", "on"}
        if "SCHEDULER_INTERVAL_MINUTES" in os.environ:
            scheduler_raw["interval_minutes"] = int(os.environ["SCHEDULER_INTERVAL_MINUTES"])

        return cls(
            database=DatabaseSettings(**database_raw),
            scraper=ScraperSettings(**toml_data.get("scraper", {})),
            scheduler=SchedulerSettings(**scheduler_raw),
            bot=BotSettings(**bot_raw),
            llm=LlmSettings(**toml_data.get("llm", {})),
            assistant=AssistantSettings(**assistant_raw),
            knowledge=KnowledgeSettings(**knowledge_raw),
            admin=AdminSettings(**admin_raw),
            memory=MemorySettings(**memory_raw),
            kafka=KafkaSettings(**kafka_raw),
            reliable_delivery=ReliableDeliverySettings(**reliable_raw),
            logging=LoggingSettings(**toml_data.get("logging", {})),
        )


@lru_cache
def get_settings() -> Settings:
    """Cached settings singleton."""
    return Settings.from_toml()
