# Project Stack

This file captures the factual technology stack currently used in this project.

## Application Stack

| Area | Technology |
| --- | --- |
| Telegram framework | `aiogram 3.x`, Telegram Bot API polling, FSM via `MemoryStorage`, inline and reply keyboards |
| Parser / source ingestion | Public Telegram channel pages via `https://t.me/s/{channel}`; no Telegram API is used for channel reading |
| HTTP / parsing | `httpx.AsyncClient`, `BeautifulSoup4`, CSS selectors, `markdownify`, rate limiting, exponential backoff for HTTP `429` |
| Vector DB | Not used currently |
| Embeddings | Not used currently |
| Model provider | OpenRouter-compatible Chat Completions API |
| LLM model chain | `tencent/hy3-preview:free`, `openai/gpt-oss-120b:free`, `nvidia/nemotron-3-super-120b-a12b:free`, `google/gemma-4-31b-it:free`, `openai/gpt-oss-20b:free`, `google/gemini-2.5-flash-lite` |
| Orchestration / agent framework | No LangChain, CrewAI, or similar runtime framework; orchestration is implemented with project services plus `APScheduler` |
| Runtime shape | Single Python process: scraper scheduler, digest delivery, and Telegram bot polling run together |
| Storage | PostgreSQL with `SQLAlchemy asyncio`, `asyncpg`, and Alembic migrations |
| Test database | In-memory SQLite via `aiosqlite` |
| Configuration | `config.toml` plus environment overrides such as `DATABASE_URL`, `DB_PASSWORD`, `BOT_TOKEN`, `OPENROUTER_API_KEY` |
| Deployment | Docker Compose with app container, `postgres:16`, and optional `pgAdmin` |
| App image | `python:3.12-slim` |
| Startup | Docker entrypoint runs `alembic upgrade head`, then starts `python -m src.main` |

## Current Application Behavior

- The bot manages users, settings, and named subscriptions.
- The scraper fetches public Telegram pages from `t.me/s/*`, parses posts, and stores them.
- The scheduler periodically runs scraping and digest delivery jobs.
- Digest delivery sends Telegram-safe HTML messages.
- LLM summaries are optional: if OpenRouter is unavailable or all models fail, delivery falls back to the short `200 chars` mode instead of blocking.

## AI Development Management

Project memory and AI-development workflow are managed through versioned files in the repository, not through a separate graph database.

| Area | Files / approach |
| --- | --- |
| Agent primer | `ai/knowledge-graph/agent-prime.xml` |
| Project knowledge graph | `ai/knowledge-graph/project-graph.xml` |
| E2E and scenario catalog | `ai/knowledge-graph/e2e-scenarios.yaml` |
| Backlog index | `.planning/BACKLOG.md` |
| Backlog items | `.planning/backlog/*.md` |
| Feature spec template | `.planning/backlog/_FEATURE-SPEC-TEMPLATE.md` |
| Technical debt log | `.planning/TECH_DEBT.md` |
| Agent operating rules | `AGENTS.md` |
| Legacy planning archive | `.planning/archive/gsd-legacy/*`; reference only, not the active source of truth |

## AI Development Process

- Agents read `ai/knowledge-graph/agent-prime.xml` before planning or editing.
- Agents load only the task-relevant neighborhood from `ai/knowledge-graph/project-graph.xml`.
- Changes are mapped to entities, flows, invariants, scenarios, and tests.
- Behavior or verification changes must update the matching files under `ai/knowledge-graph/*` in the same change.
- Backlog features are clarified against `.planning/backlog/_FEATURE-SPEC-TEMPLATE.md` before or during implementation.
- Deferred cleanup discovered during work is recorded in `.planning/TECH_DEBT.md`.
