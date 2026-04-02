# Phase 1: Data Foundation - Context

**Gathered:** 2026-04-02
**Status:** Ready for planning

<domain>
## Phase Boundary

Scrape public Telegram channels via t.me/s/*, store posts with full metadata in PostgreSQL, run periodic scraping via scheduler. No duplicates, no data loss. This phase delivers the data pipeline — no user-facing features yet.

</domain>

<decisions>
## Implementation Decisions

### Scraping behavior
- Each scrape run fetches the last 20 posts (first page) per channel
- When user subscribes to a new channel — scrape fresh posts after subscription moment with user's interval
- Full history scraping (last 100+ posts) for "channel exploration" is a future feature
- Posts stored read-only — no edit/delete tracking, store once and never update
- New channels queued for next scraping cycle (no immediate scrape)
- Rate limit: 1 req/sec conservative, sequential across channels

### Data model
- Post content stored as Markdown (convert from HTML during scraping via t.me/s/*)
- Reactions stored as JSONB column: {"emoji": count}
- Post content: unlimited TEXT column (no truncation)
- Link previews stored as JSONB: {title, site_name, description, url}
- Channel identified by numeric ID (not username) to survive username changes
- Required post fields: post_id, channel_id, content, datetime
- Optional post fields: views, reactions, link_preview

### Error handling
- Channel not found / private → mark as 'error' in DB, skip in future scrapes, log warning
- Preserve channel numeric ID even on error (survives username changes)
- Individual post parse failure → save whatever was extracted + log error, continue scraping rest
- Logging: stdout structured JSON + errors to a PostgreSQL log table
- PostgreSQL transient failures → SQLAlchemy connection pool with auto-reconnect, retry on transient errors

### Scheduler design
- Fixed interval every 5 minutes (configurable in TOML config)
- Sequential scraping for now; switch to parallel later when channel count exceeds what fits in the interval
- Channel list loaded dynamically from DB each cycle
- Empty start — no seed channels, channels added by users later
- Future: residential proxies for scaling parallel scraping

### Infrastructure & tests
- Config: TOML file for settings (DB URL, scraping interval, rate limits) + env vars for secrets
- Project layout: src/ packages — scraper/, models/, scheduler/, config/
- Dependencies: pyproject.toml (Poetry or similar)
- DB migrations: Alembic from the start
- Tests: pytest + pytest-asyncio for async tests
  - Unit tests for parser (most complex part)
  - Integration tests for DB layer
  - Test for scheduler logic
- Runtime: Docker Compose (app + PostgreSQL + pgAdmin)
- Full stack in containers for development

### OpenCode's Discretion
- Exact HTML→Markdown conversion library choice
- PostgreSQL index strategy
- Exact TOML config file structure and key names
- Logging format details
- Alembic migration naming convention

</decisions>

<specifics>
## Specific Ideas

- Channel must be identified by numeric ID, not username — Telegram allows username changes, ID is permanent
- When channels grow beyond what sequential scraping handles in 5 minutes → parallel mode with residential proxies
- Existing skill at /opt/nanobot/.nanobot/workspace/skills/telegram-search/ has working parser code to reference
- TOML config for all tuneable settings (scraping interval, rate limits, DB connection params)

</specifics>

<deferred>
## Deferred Ideas

- Channel exploration feature (last 100 posts + LLM summarization) — future phase after LLM integration
- Parallel scraping with residential proxies — defer until channel count justifies it
- Per-channel configurable scraping intervals — v2 feature
- Proxy support — defer until scaling requires it

</deferred>

---

*Phase: 01-data-foundation*
*Context gathered: 2026-04-02*
