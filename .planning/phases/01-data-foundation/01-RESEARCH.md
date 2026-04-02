# Phase 1 Research: Data Foundation

**Phase:** 1
**Date:** 2026-04-02
**Status:** Complete

## Key Technical Findings

### 1. t.me/s/* HTML Structure

Reference parser at `/opt/nanobot/.nanobot/workspace/skills/telegram-search/telegram_parser.py` provides proven selectors:

- **Post container:** `div.tgme_widget_message` (each post is one div)
- **Post ID:** Extracted from `div[data-post]` attribute (format: `channel/12345`)
- **Content:** `div.tgme_widget_message_text` — inner HTML needs HTML→Markdown conversion
- **Date:** `time` element with `datetime` attribute (ISO 8601)
- **Views:** `span.tgme_widget_message_views` — text like "1.5K", "234"
- **Author:** `a.tgme_widget_message_owner_name`
- **Reactions:** `div.tgme_widget_message_reactions` → `span.tgme_reaction` → `i.emoji > b` for emoji, remaining text for count
- **Link preview:** `a.tgme_widget_message_link_preview` → nested `div.link_preview_title`, `div.link_preview_site_name`, `div.link_preview_description`
- **Pagination:** `a.tme_messages_more[href]` — URL contains `?before=XX` parameter

**Key insight:** The `data-post` attribute format is `channel_name/12345`. The numeric portion is the unique post_id. Channel name from this attribute is the username at time of posting.

### 2. HTML to Markdown Conversion

**Recommended: `markdownify`** library.
- Lightweight, uses BeautifulSoup under the hood
- Handles `<b>`, `<i>`, `<a>`, `<br>`, `<pre>`, `<code>`, lists
- Telegram posts commonly use: bold, italic, links, pre/code blocks, inline code
- Fallback: manual BeautifulSoup text extraction with separator=' '

**Alternative: `html2text`** — heavier, more configurable, but overkill for Telegram's limited HTML subset.

### 3. Async HTTP Scraping

**Recommended: `httpx`** (async mode)
- Native async/await support
- Connection pooling built-in
- Timeout and redirect handling
- Rate limiting: `asyncio.sleep()` between requests (1 req/sec per context decision)
- 429 handling: exponential backoff with jitter

The existing parser uses synchronous `requests` — we need to adapt to async `httpx`.

### 4. Database Stack

**SQLAlchemy 2.0 async** with **asyncpg** driver:
- `create_async_engine()` with `asyncpg://` URL
- `AsyncSession` with `async_sessionmaker`
- Connection pool with auto-reconnect: `pool_pre_ping=True`, `pool_recycle=3600`
- PostgreSQL JSONB for reactions and link_previews (indexed via GIN if needed later)

**Alembic** for migrations from the start:
- `alembic init alembic` in project root
- `env.py` configured for async (`run_async_migrations`)
- Migration naming convention for consistency

### 5. Scheduler

**APScheduler 3.x** with `AsyncIOScheduler`:
- Single event loop with the app (per CRON-03)
- `IntervalTrigger` for periodic scraping (default 5 min, configurable)
- Jobs can be added/removed dynamically
- `misfire_grace_time` to handle overlapping runs

**Alternative: `APScheduler 4.x`** — API changed significantly, 3.x has more stable docs and examples.

### 6. Configuration

**TOML via `tomllib`** (Python 3.11+) or `tomli` for older versions:
- Config file: `config.toml` (or `config.local.toml` for overrides)
- Sections: `[database]`, `[scraper]`, `[scheduler]`, `[logging]`
- Env var overrides for secrets: `TELEGRAM_DB_URL`, etc.
- Pydantic `BaseSettings` for typed config with env var support

### 7. Project Structure

```
telegram-parser-bot/
├── pyproject.toml
├── alembic/
│   ├── env.py
│   └── versions/
├── config.toml
├── config.local.toml (gitignored)
├── docker-compose.yml
├── Dockerfile
├── src/
│   ├── __init__.py
│   ├── config/          # TOML config loading, Pydantic settings
│   │   ├── __init__.py
│   │   └── settings.py
│   ├── models/          # SQLAlchemy models
│   │   ├── __init__.py
│   │   ├── base.py      # DeclarativeBase, common columns
│   │   ├── channel.py   # Channel model
│   │   └── post.py      # Post model
│   ├── scraper/         # HTTP scraping + HTML parsing
│   │   ├── __init__.py
│   │   ├── parser.py    # HTML → structured data
│   │   ├── client.py    # Async HTTP client with rate limiting
│   │   └── service.py   # Scrape orchestration per channel
│   ├── scheduler/       # APScheduler setup
│   │   ├── __init__.py
│   │   └── jobs.py      # Scraping job definition
│   └── repository/      # DB access layer
│       ├── __init__.py
│       ├── channel.py
│       └── post.py
├── tests/
│   ├── conftest.py
│   ├── unit/
│   │   ├── test_parser.py
│   │   └── test_service.py
│   └── integration/
│       ├── test_db.py
│       └── test_scheduler.py
└── .env (gitignored)
```

### 8. Docker Compose

```yaml
services:
  app:
    build: .
    depends_on:
      db:
        condition: service_healthy
    env_file: .env
    volumes:
      - ./config.toml:/app/config.toml
  db:
    image: postgres:16
    environment:
      POSTGRES_DB: telegram_bot
      POSTGRES_USER: bot
      POSTGRES_PASSWORD: ${DB_PASSWORD}
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U bot"]
      interval: 5s
      timeout: 5s
      retries: 5
  pgadmin:
    image: dpage/pgadmin4
    ports:
      - "5050:80"
    depends_on:
      db:
        condition: service_healthy
volumes:
  pgdata:
```

### 9. Deduplication Strategy

Per CONTEXT: posts are read-only (store once, never update). Deduplication via:
- **Unique constraint:** `(channel_id, post_id)` on posts table
- **INSERT ... ON CONFLICT DO NOTHING** — SQLAlchemy `insert().on_conflict_do_nothing()`
- The `data-post` attribute provides `username/12345` — extract numeric ID as `post_id`
- Channel identified by numeric ID (not username) per CONTEXT decision

### 10. Error Handling Patterns

- **HTTP 429:** Exponential backoff: 1s → 2s → 4s → 8s → 16s (max 30s), then retry up to 3 times
- **Channel not found / private:** Mark channel status as 'error', log warning, skip in future scrapes
- **Individual post parse failure:** Save partial data + error flag, continue scraping rest
- **DB transient failure:** SQLAlchemy pool_pre_ping + retry with backoff on OperationalError
- **Logging:** structlog or stdlib `logging` with JSON formatter to stdout + errors table in PostgreSQL

### 11. Views Count Parsing

Views are formatted as strings: "234", "1.5K", "2.3M". Need a parser:
```python
def parse_views(views_str: str) -> int:
    views_str = views_str.strip().replace(',', '')
    if 'K' in views_str:
        return int(float(views_str.replace('K', '')) * 1000)
    elif 'M' in views_str:
        return int(float(views_str.replace('M', '')) * 1_000_000)
    return int(views_str)
```

### 12. Idempotency Guarantee

Re-scraping the same channel must produce zero duplicates:
1. Scrape fetches first page (last 20 posts)
2. For each post: INSERT with ON CONFLICT DO NOTHING on (channel_id, post_id)
3. If all posts already exist → channel is up to date (but we still don't know if there are newer posts on a later page — we only scrape first page per CONTEXT decision)
4. Update `channel.last_scraped` timestamp regardless

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| t.me/s/* CSS classes change | High | Centralize all selectors in one module with constants |
| Rate limiting changes | Medium | Configurable delays, backoff in client |
| HTML→Markdown edge cases | Low | Test with real channel HTML, fallback to plain text |
| PostgreSQL connection drops | Medium | pool_pre_ping, retry logic in repository layer |

## RESEARCH COMPLETE
