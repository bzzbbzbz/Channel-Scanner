# Stack Research

**Domain:** Telegram Channel Monitoring Bot (Python, PostgreSQL, LLM summarization, cron scheduling)
**Researched:** 2026-04-02
**Confidence:** HIGH

## Recommended Stack

### Core Technologies

| Technology | Version | Purpose | Why Recommended |
|------------|---------|---------|-----------------|
| Python | 3.12+ | Runtime | aiogram requires >=3.10; 3.12 has perf improvements, better error messages, and is the sweet spot for library compatibility |
| aiogram | 3.26.0 | Telegram Bot framework | De facto standard for async Python Telegram bots. Supports Bot API 9.5, built-in routers/blueprints, FSM, middlewares, inline keyboards. Active development (monthly releases). Uses aiohttp internally. |
| SQLAlchemy | 2.0.48 | ORM + query builder | 2.0 has first-class async support via `create_async_engine`. Declarative models, type-safe queries, async sessions. Mature, well-documented. |
| asyncpg | 0.31.0 | PostgreSQL async driver | 5x faster than psycopg3 per MagicStack benchmarks. Native PostgreSQL binary protocol. Used by SQLAlchemy's `postgresql-asyncpg` dialect. Zero deps. |
| APScheduler | 3.11.2 | Cron/interval task scheduling | Stable async scheduler with cron-style triggers, interval triggers, SQLAlchemy job store for persistence across restarts. APScheduler 4 is still alpha (4.0.0a6) — avoid it. |
| openai | 2.30.0 | LLM API client | Official SDK with `AsyncOpenAI` client. Supports chat.completions (stable) and responses API. Works with any OpenAI-compatible endpoint (set `base_url` for local/self-hosted LLMs). Auto-retries on 429/5xx. |
| httpx | 0.28.1 | Async HTTP client for scraping | Modern async HTTP client. Connection pooling, timeout control, retry logic. Used for fetching t.me/s/* pages. |
| selectolax | 0.4.7 | HTML parsing | Lexbor-based parser, 25x faster than BeautifulSoup (2.4s vs 61s in benchmarks). CSS selectors, minimal memory. Preferred over BS4 for high-volume scraping. |

### Supporting Libraries

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| alembic | 1.18.4 | Database migrations | All schema changes. Auto-generate migrations from SQLAlchemy models. Runs in transactions on PostgreSQL. |
| pydantic-settings | 2.13.1 | Configuration management | Loading config from env vars / .env files. Typed settings with validation. Replaces manual `os.environ.get()`. |
| lxml | (latest) | Fallback HTML parser | Only if selectolax can't handle a specific page. BS4+ lxml is the reliable fallback. |
| structlog | (latest) | Structured logging | For machine-parseable logs with context (channel_id, user_id, etc.). Better than stdlib logging for production monitoring. |
| tenacity | (latest) | Retry/backoff logic | For t.me scraping — handles 429 rate limits with exponential backoff. Complements httpx retries for application-level retry policies. |

### Development Tools

| Tool | Purpose | Notes |
|------|---------|-------|
| Docker + docker-compose | Local PostgreSQL + bot containerization | Compose for dev (bot + postgres), Dockerfile for production |
| ruff | Linter + formatter | Replaces flake8+isort+black. Fast, one tool. |
| mypy | Type checking | SQLAlchemy 2.0 has excellent type support. aiogram is typed. |
| pytest + pytest-asyncio | Testing | pytest-asyncio for async test functions. |

## Installation

```bash
# Core
pip install aiogram==3.26.0 sqlalchemy==2.0.48 asyncpg==0.31.0 APScheduler==3.11.2

# HTTP + Parsing
pip install httpx==0.28.1 selectolax==0.4.7

# LLM
pip install openai==2.30.0

# Database migrations
pip install alembic==1.18.4

# Configuration
pip install pydantic-settings==2.13.1

# Logging
pip install structlog

# Retry logic
pip install tenacity

# Dev dependencies
pip install -D ruff mypy pytest pytest-asyncio
```

## Alternatives Considered

| Recommended | Alternative | When to Use Alternative |
|-------------|-------------|-------------------------|
| aiogram 3.x | python-telegram-bot (PTB) | PTB is sync-first (added async later), more verbose API, less Pythonic routing. Only use if you need PTB's job queue or already have PTB code. |
| aiogram 3.x | telebot (pyTelegramBotAPI) | Sync-only, no native async. Fine for simple bots, but blocks on every I/O. Unsuitable for a bot that scrapes + talks to LLM + serves users concurrently. |
| SQLAlchemy 2.0 + asyncpg | psycopg (psycopg3) async | psycopg3 is the official PostgreSQL driver but asyncpg is 5x faster. SQLAlchemy's asyncpg dialect is well-tested. Use psycopg3 only if you need features asyncpg lacks (rare). |
| SQLAlchemy 2.0 + asyncpg | Tortoise ORM | Simpler but less powerful. Fine for CRUD-only apps. Our use case needs complex queries (filtering, aggregations, full-text search) — SQLAlchemy is better. |
| SQLAlchemy 2.0 + asyncpg | raw asyncpg queries | Faster for trivial queries but you lose migrations (alembic), model definitions, and type safety. Not worth it unless you're optimizing a known bottleneck. |
| APScheduler 3.x | asyncio.create_task + manual scheduling | Works for simple intervals but no persistence, no cron syntax, no dynamic job management. APScheduler adds <100 LOC and gives you production-grade scheduling. |
| APScheduler 3.x | Celery + beat | Overkill for a single-process bot. Celery needs a broker (Redis/RabbitMQ). Adds operational complexity. Only if you need distributed task execution. |
| selectolax | BeautifulSoup4 | Use BS4+lxml only if selectolax fails on specific HTML. The existing prototype uses BS4 — migrate to selectolax for performance. |
| openai SDK | litellm | litellm abstracts across LLM providers (OpenAI, Anthropic, local). Use if you need multi-provider support. For a single provider, the official SDK is more stable and better typed. |
| httpx | aiohttp | Both are async. httpx has a cleaner API, better timeout handling, and is what the openai SDK uses internally. aiohttp is already pulled in by aiogram, but httpx is better as a standalone scraping client. |

## What NOT to Use

| Avoid | Why | Use Instead |
|-------|-----|-------------|
| APScheduler 4.x | Still in alpha (4.0.0a6 as of Apr 2025). API is unstable, documentation incomplete, breaking changes between alpha releases. | APScheduler 3.11.x |
| requests (sync) | Blocks the event loop. Will deadlock aiogram's async bot. | httpx (async) |
| BeautifulSoup4 (as primary parser) | 25x slower than selectolax. For scraping 100+ channels on schedule, this adds up to minutes vs seconds. Use BS4 only as a fallback. | selectolax (Lexbor backend) |
| sqlite3 | No concurrent writes, no full-text search (FTS5 requires compilation), no JSONB. PostgreSQL is the project constraint anyway. | PostgreSQL |
| SQLAlchemy 1.4 | Legacy API. 2.0 has breaking changes but is strictly better (async-first, typed, modern patterns). | SQLAlchemy 2.0 |
| telebot / pyTelegramBotAPI | Synchronous. Will block the event loop when the bot is also scraping channels and calling LLM APIs. | aiogram 3.x |
| python-dotenv | Manual env loading without validation. pydantic-settings gives you typed config with validation + env loading. | pydantic-settings |
| Celery | Requires message broker (Redis/RabbitMQ). Single-process bot doesn't need distributed task queue. | APScheduler 3.x |

## Stack Patterns by Variant

**If using a local/self-hosted LLM (e.g., Ollama, vLLM):**
- Set `base_url` on the OpenAI client: `AsyncOpenAI(base_url="http://localhost:11434/v1", api_key="dummy")`
- Most local LLM servers expose an OpenAI-compatible API
- No code changes needed beyond configuration

**If scaling to 1000+ channels:**
- Add Redis for caching parsed posts (deduplication before DB write)
- Consider running scraper as a separate process with its own APScheduler instance
- Use selectolax (already recommended) — performance matters at scale

**If deploying on a low-resource VPS:**
- Use `gpt-4o-mini` or equivalent cheap model for summaries
- Set aggressive rate limits in APScheduler (fewer concurrent scrapes)
- PostgreSQL connection pool size: 5-10 (not 20+)

## Version Compatibility

| Package A | Compatible With | Notes |
|-----------|-----------------|-------|
| aiogram 3.26.0 | Python >=3.10, <3.15 | Tested up to Python 3.14 |
| SQLAlchemy 2.0.48 | asyncpg 0.31.0 | Use `postgresql+asyncpg://` dialect |
| alembic 1.18.4 | SQLAlchemy 2.0.x | Same author (Mike Bayer), designed to work together. Requires Python >=3.10 |
| APScheduler 3.11.2 | SQLAlchemy job store | Built-in `SQLAlchemyJobStore` for persisting scheduled jobs |
| openai 2.30.0 | httpx (bundled) | Uses httpx internally. AsyncOpenAI works natively with asyncio |
| pydantic-settings 2.13.1 | pydantic 2.x | Requires pydantic >=2.0 |
| selectolax 0.4.7 | Python >=3.9, <3.15 | Lexbor backend is the default and preferred. CPython only (no PyPy). |

## Sources

- pypi.org/project/aiogram — Version 3.26.0, requires Python >=3.10, supports Bot API 9.5 — **HIGH confidence**
- pypi.org/project/sqlalchemy — Version 2.0.48, async support with asyncpg dialect — **HIGH confidence**
- pypi.org/project/asyncpg — Version 0.31.0, 5x faster than psycopg3, native PostgreSQL binary protocol — **HIGH confidence**
- pypi.org/project/APScheduler — Version 3.11.2 (stable), 4.0 still alpha — **HIGH confidence**
- pypi.org/project/openai — Version 2.30.0, AsyncOpenAI, chat.completions API — **HIGH confidence**
- pypi.org/project/httpx — Version 0.28.1, async HTTP client (1.0 still in dev) — **HIGH confidence**
- pypi.org/project/beautifulsoup4 — Version 4.14.3 — **HIGH confidence**
- pypi.org/project/selectolax — Version 0.4.7, Lexbor backend 25x faster than BS4 — **HIGH confidence**
- pypi.org/project/alembic — Version 1.18.4, SQLAlchemy migration tool — **HIGH confidence**
- pypi.org/project/pydantic-settings — Version 2.13.1 — **HIGH confidence**

---
*Stack research for: Telegram Channel Monitoring Bot*
*Researched: 2026-04-02*
