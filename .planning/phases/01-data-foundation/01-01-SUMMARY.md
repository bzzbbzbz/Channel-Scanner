---
phase: 01-data-foundation
plan: 01
subsystem: database
tags: [sqlalchemy, asyncpg, alembic, pydantic, docker, postgresql, toml]

# Dependency graph
requires: []
provides:
  - SQLAlchemy async models (Channel, Post) with PostgreSQL schema
  - Pydantic Settings from TOML config + env var overrides
  - Docker Compose dev environment (app + postgres + pgadmin)
  - Alembic async migration runner configured
  - pytest async fixtures with in-memory SQLite
affects: [01-02-scraper, 01-03-scheduler, 02-api]

# Tech tracking
tech-stack:
  added: [sqlalchemy>=2.0, asyncpg, alembic, pydantic>=2.0, pydantic-settings, structlog, httpx, beautifulsoup4, markdownify, apscheduler>=3.10, pytest, pytest-asyncio, aiosqlite]
  patterns: [async-sqlalchemy, pydantic-settings-from-toml, docker-compose-healthchecks, alembic-async-migrations]

key-files:
  created:
    - src/config/settings.py
    - src/models/base.py
    - src/models/channel.py
    - src/models/post.py
    - alembic/env.py
    - docker-compose.yml
    - config.toml
    - pyproject.toml
    - tests/conftest.py
  modified: []

key-decisions:
  - "Used plain DeclarativeBase instead of MappedAsDataclass to avoid dataclass field ordering issues"
  - "Used SQLite in-memory for test fixtures instead of test PostgreSQL for portability"

patterns-established:
  - "TOML config → Pydantic Settings with env var override pattern"
  - "SQLAlchemy models with server_default=func.now() for timestamps"
  - "Docker Compose with health check dependency ordering"

requirements-completed: [STOR-01, STOR-03]

# Metrics
duration: 10min
completed: 2026-04-02
---

# Phase 1 Plan 1: Data Foundation Setup Summary

**Pydantic Settings from TOML + env vars, async SQLAlchemy Channel/Post models with JSONB, Docker Compose stack, and Alembic async migrations**

## Performance

- **Duration:** 10 min
- **Started:** 2026-04-02T19:02:04Z
- **Completed:** 2026-04-02T19:13:00Z
- **Tasks:** 2
- **Files modified:** 19

## Accomplishments
- Complete project scaffolding with all dependencies in pyproject.toml
- Pydantic Settings loading config.toml with DATABASE_URL/DB_PASSWORD env var overrides
- Docker Compose stack: postgres:16 (with healthcheck), app, pgadmin
- Channel model: BigInteger telegram_id (unique), status enum (active/error/paused), last_scraped tracking
- Post model: JSONB reactions/link_preview, BigInteger post_id, dedup UniqueConstraint (channel_id, post_id)
- Async Alembic env.py wired to Settings for database URL
- pytest async fixtures with in-memory SQLite engine

## Task Commits

Each task was committed atomically:

1. **Task 1: Project scaffolding + config system + Docker** - `a7c8189` (feat)
2. **Task 2: SQLAlchemy async models + Alembic migrations** - `ec85404` (feat)

## Files Created/Modified
- `pyproject.toml` - Project config with all dependencies (sqlalchemy, asyncpg, alembic, httpx, pydantic, etc.)
- `config.toml` - Default config: 5-min intervals, 1 req/s rate limit, 20 posts max
- `src/config/settings.py` - Pydantic Settings from TOML with env var override (DATABASE_URL, DB_PASSWORD)
- `src/config/__init__.py` - Re-exports Settings and get_settings
- `src/__init__.py` - Package init
- `src/models/base.py` - DeclarativeBase with constraint naming conventions
- `src/models/channel.py` - Channel model: telegram_id, username, name, status, last_scraped, timestamps
- `src/models/post.py` - Post model: post_id, content, datetime, views, reactions (JSONB), link_preview (JSONB), dedup constraint
- `src/models/__init__.py` - Re-exports Base, Channel, ChannelStatus, Post
- `alembic.ini` - Alembic config (URL configured in env.py from Settings)
- `alembic/env.py` - Async migration runner using Settings database URL
- `alembic/script.py.mako` - Migration template
- `alembic/versions/.gitkeep` - Placeholder for migration files
- `docker-compose.yml` - 3 services: db (postgres:16 + healthcheck), app, pgadmin
- `Dockerfile` - Python 3.12 slim, installs deps, copies src
- `.env.example` - DB_PASSWORD, pgAdmin credentials
- `.gitignore` - Python, Docker, env patterns
- `tests/conftest.py` - Async pytest fixtures with in-memory SQLite

## Decisions Made
- **Dropped MappedAsDataclass:** Used plain DeclarativeBase to avoid Python dataclass field ordering issues (non-default fields after defaults). Simpler and more maintainable for database models.
- **SQLite for test fixtures:** Used aiosqlite in-memory SQLite instead of test PostgreSQL for portability — tests can run without Docker.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed relationship field using mapped_column instead of relationship()**
- **Found during:** Task 2 (SQLAlchemy models)
- **Issue:** Channel.posts was defined with `mapped_column(back_populates=..., lazy=...)` which passed relationship kwargs to a column constructor, causing TypeError
- **Fix:** Changed to `relationship(back_populates=..., lazy=...)` which is the correct SQLAlchemy API
- **Files modified:** src/models/channel.py
- **Verification:** Models import successfully, all column types and constraints verified
- **Committed in:** ec85404 (Task 2 commit)

**2. [Rule 1 - Bug] Dropped MappedAsDataclass to fix dataclass field ordering**
- **Found during:** Task 2 (SQLAlchemy models)
- **Issue:** MappedAsDataclass requires all non-default fields before default fields; Channel model had `status` (default) before `last_scraped` (no default), causing TypeError
- **Fix:** Removed MappedAsDataclass mixin from Base, using plain DeclarativeBase — cleaner for DB models that don't need dataclass behavior
- **Files modified:** src/models/base.py, src/models/channel.py, src/models/post.py
- **Verification:** All models import and metadata registers both tables
- **Committed in:** ec85404 (Task 2 commit)

---

**Total deviations:** 2 auto-fixed (2 bugs)
**Impact on plan:** Both fixes necessary for correctness. No scope creep.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Data models and config system ready for scraper implementation (Plan 02)
- Docker Compose stack ready for local development
- Alembic ready to generate initial migration once database is available
- Test infrastructure in place for unit/integration tests

## Self-Check: PASSED

- All 10 key files verified on disk
- Both task commits (a7c8189, ec85404) found in git history
- Success criteria verified: model imports, Settings import, UniqueConstraint, BigInteger telegram_id

---
*Phase: 01-data-foundation*
*Completed: 2026-04-02*
