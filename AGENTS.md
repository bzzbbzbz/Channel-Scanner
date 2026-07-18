# AGENTS.md

## First Read

- Read `ai/knowledge-graph/agent-prime.xml` before planning or editing.
- Use `ai/knowledge-graph/project-graph.xml` only for the task-relevant neighborhood, not as a full-context dump.
- If behavior changes, update the matching entries in:
  - `ai/knowledge-graph/project-graph.xml`
  - `ai/knowledge-graph/agent-prime.xml`
  - `ai/knowledge-graph/e2e-scenarios.yaml`

## Runtime Shape

- This app is a single Python process: APScheduler scraping plus Telegram bot polling in the same runtime. Entry point: `python -m src.main`.
- Startup order in `src/main.py`: load settings -> create async SQLAlchemy engine/session factory -> create Telegram HTTP client -> start scheduler -> optionally start bot runtime.
- If `BOT_TOKEN` is empty, the scraper/scheduler can still run; bot polling is skipped.

## Source Of Truth

- Settings come from `config.toml` with env var overrides in `src/config/settings.py`.
- Database URL is not taken from `alembic.ini`; Alembic reads settings via the app config path.
- `DATABASE_URL` overrides the DB URL directly; `DB_PASSWORD` rewrites the password inside the configured URL.
- `BOT_TOKEN` is the main bot token env var; `TELEGRAM_TOKEN` is only a fallback when `BOT_TOKEN` is unset.

## Commands

- Install dev deps: `pip install -e ".[dev]"`
- Run the app locally: `python -m src.main`
- Start the full stack: `docker compose up --build`
- Apply migrations: `alembic upgrade head`
- Create a migration: `alembic revision -m "message"`
- Autogenerate a migration after model changes: `alembic revision --autogenerate -m "message"`
- Run all tests: `pytest`
- Run one file: `pytest tests/integration/test_digest_delivery.py`
- Run one test: `pytest tests/integration/test_digest_delivery.py::test_digest_service_delivers_per_subscription`

## Logs And Runtime Diagnostics

- There are no project-managed `.log` files by default; app logs go to process stdout/stderr.
- Docker runtime logs: use `docker compose logs --since=30m app` for recent app output, or `docker compose logs -f app` while reproducing an issue.
- Service status: use `docker compose ps` to confirm `app`, `db`, and `pgadmin` state; use `docker compose ps app` for the bot process only.
- Startup health: in app logs look for `Scheduler started`, `Telegram bot polling started`, and `Run polling for bot ...`.
- Scheduler/scraper issues: inspect `docker compose logs --since=30m app` for `src.scheduler.jobs`, `apscheduler`, and `httpx` lines around scrape/digest job times.
- Assistant/LLM slowness: inspect app logs for `src.assistant`, `src.llm`, `OpenRouter`, `chat/completions`, `embeddings`, `Assistant model failed`, and model-pool probe warnings. `httpx` request logs appear only after the HTTP call finishes or errors, so a visible Telegram `typing` state with no new `chat/completions` completion log can mean the turn is still waiting on OpenRouter, mem0, or model-pool probing.
- Assistant chat persistence: query recent chat history with `docker compose exec -T db psql -U bot -d telegram_bot -c "select id, user_id, role, left(text, 240) as text, created_at from chat_messages order by id desc limit 20;"`. A recent `user` row without a following `assistant` or `system` row means the assistant turn did not complete and persist its response.
- Database state checks: use `docker compose exec -T db psql -U bot -d telegram_bot -c "<SQL>"`; prefer read-only `select` queries unless intentionally fixing data.
- Container process check: use `docker top telegram-parser-bot-app-1 -eo pid,ppid,stat,etime,cmd` when `ps` is unavailable inside the slim app image.
- After code changes that affect the running bot, apply them with `docker compose up -d --build app`, then re-check startup logs.

## Test Reality

- Tests use in-memory SQLite via `tests/conftest.py`; they do not require Docker or PostgreSQL.
- The production app uses PostgreSQL with `asyncpg`; keep DB-specific behavior in mind when changing queries or migrations.
- There is no separate black-box E2E suite yet. Current scenario coverage is tracked in `ai/knowledge-graph/e2e-scenarios.yaml` and is mostly `integration_backed`.

## High-Risk Behavior

- Channel membership is per named subscription, not global per user.
- `subscription_channels.subscribed_at` is critical: new subscriptions must not receive historical posts from before the channel was added.
- Delivery dedup is per `(subscription, post)`, not just per user.
- Scraping must stay idempotent for existing posts.
- Summary generation must never block delivery; the fallback is `200 символов` mode.
- Digest rendering targets Telegram-safe HTML, not arbitrary Markdown/HTML.

## Where To Verify Changes

- Bot and subscription flows: `tests/integration/test_bot_service.py`
- Digest selection, delivery, fallback: `tests/integration/test_digest_delivery.py`
- Scheduler and scraping job behavior: `tests/integration/test_scheduler.py`
- Repository and DB invariants: `tests/integration/test_db.py`

## Working Rules For Agents

- Always update the knowledge-graph files when a change adds, removes, or changes behavior, flows, invariants, or verification expectations.
- If the user asks to implement a backlog feature, do not start coding from the initial request, even if they phrase it as "add", "implement", or "do it now". First use the `question` tool to clarify ambiguous requirements, edge cases, acceptance details, and user-visible UX. Ask short questions one at a time, aiming for up to about 10 total if needed; stop earlier only once the feature is clear enough to specify safely.
- For backlog features, create or update a separate technical specification under `.planning/backlog/` using `.planning/backlog/_FEATURE-SPEC-TEMPLATE.md` before implementation. Do not treat an in-chat plan as a substitute for the spec file.
- Always add matching entries to `.planning/BACKLOG.md` before implementing backlog feature code; when implementation is complete, mark the matching backlog item/spec as closed.
- Before editing, identify impacted flows, invariants, and matching scenario entries from `ai/knowledge-graph/e2e-scenarios.yaml`.
- Use this graph-update checklist when behavior or verification changes:
  - Did user-visible behavior change?
  - Did a flow or invariant change?
  - Did test mapping or E2E expectations change?
  - If yes, update the matching `ai/knowledge-graph/*` files in the same change.
- Prefer narrow verification first:
  - `src/bot/` or subscription logic -> `tests/integration/test_bot_service.py`
  - `src/digest/` or `src/llm/` -> `tests/integration/test_digest_delivery.py`
  - `src/scheduler/` or `src/scraper/` -> `tests/integration/test_scheduler.py`
  - repository or persistence invariants -> `tests/integration/test_db.py`
- If a behavior change affects architecture or verification expectations, update the knowledge-graph files in the same change.
- End implementation reports with:
  - changed entities/files
  - impacted flows/invariants/scenarios
  - tests run
  - remaining gaps
  - technical debt noted in `.planning/TECH_DEBT.md`, if any

## Tech Debt

- If a task reveals important deferred cleanup that is out of scope for the current change, record it in `.planning/TECH_DEBT.md` instead of leaving it only in the final message.
