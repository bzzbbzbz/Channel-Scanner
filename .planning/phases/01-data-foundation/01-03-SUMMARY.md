---
phase: 01-data-foundation
plan: 03
subsystem: scheduler
tags: [apscheduler, async, sqlalchemy, repository-pattern, deduplication, on-conflict]

# Dependency graph
requires:
  - phase: 01-data-foundation/01
    provides: SQLAlchemy async models, Pydantic Settings, Docker Compose
  - phase: 01-data-foundation/02
    provides: ScraperService, TelegramClient, ParsedPost DTO
provides:
  - ChannelRepository with full lifecycle management (active/error/scraped)
  - PostRepository with ON CONFLICT DO NOTHING deduplication
  - APScheduler scraping_job iterating active channels sequentially
  - create_scheduler configuring AsyncIOScheduler with IntervalTrigger
  - main.py async entry point booting DB, client, and scheduler
affects: [02-api, 03-bot]

# Tech tracking
tech-stack:
  added: [apscheduler-3.x-asyncio]
  patterns: [repository-pattern, channel-data-snapshot-before-loop, postgresql-on-conflict-do-nothing, sqlite-dedup-fallback]

key-files:
  created:
    - src/repository/__init__.py
    - src/repository/channel.py
    - src/repository/post.py
    - src/scheduler/__init__.py
    - src/scheduler/jobs.py
    - src/main.py
    - tests/integration/__init__.py
    - tests/integration/test_db.py
    - tests/integration/test_scheduler.py
  modified:
    - src/models/post.py

key-decisions:
  - "Snapshot channel data into dicts before iteration loop to prevent expired-attribute errors after session.rollback()"
  - "PostRepository uses dual-path: PostgreSQL ON CONFLICT DO NOTHING for production, manual dedup for SQLite tests"
  - "Changed Post model JSONB → JSON for SQLite test compatibility"

patterns-established:
  - "Repository pattern: thin async wrappers around SQLAlchemy operations, no business logic"
  - "Channel data snapshot pattern: extract ORM attributes into plain dicts before loop to survive rollbacks"
  - "Dialect-aware repository: PostgreSQL path for production, SQLite fallback for tests"

requirements-completed: [CRON-01, CRON-03, STOR-02]

# Metrics
duration: 9min
completed: 2026-04-02
---

# Phase 1 Plan 3: Scheduler + Repository Summary

**Repository layer with ON CONFLICT DO NOTHING deduplication, APScheduler 3.x scraping job with sequential channel iteration, and async main.py entry point**

## Performance

- **Duration:** 9 min
- **Started:** 2026-04-02T19:29:49Z
- **Completed:** 2026-04-02T19:39:30Z
- **Tasks:** 2
- **Files modified:** 10

## Accomplishments
- ChannelRepository managing full channel lifecycle: active → scraped → error → active reset
- PostRepository with ON CONFLICT DO NOTHING (PostgreSQL) and manual dedup fallback (SQLite) — re-scraping produces zero duplicates
- APScheduler scraping_job iterating active channels sequentially, with per-channel commit/rollback isolation
- Channel data snapshot pattern prevents expired-attribute errors after rollback
- main.py async entry point: loads settings, creates engine, starts scheduler, runs forever with graceful shutdown
- 17 new integration tests (12 DB + 5 scheduler) — total 52 tests all passing

## Task Commits

Each task was committed atomically:

1. **Task 1: Repository layer with deduplication** - `f628197` (feat)
2. **Task 2: Scheduler + application entry point** - `bff3502` (feat)

## Files Created/Modified
- `src/repository/__init__.py` - Package init re-exporting repositories
- `src/repository/channel.py` - ChannelRepository: get_active_channels, upsert_channel, mark_scraped, mark_error, mark_active
- `src/repository/post.py` - PostRepository: upsert_posts with ON CONFLICT DO NOTHING, get_posts_by_channel, count_posts
- `src/scheduler/__init__.py` - Package init re-exporting jobs
- `src/scheduler/jobs.py` - scraping_job coroutine + create_scheduler factory
- `src/main.py` - Application entry point with async DB, client, scheduler, graceful shutdown
- `tests/integration/__init__.py` - Integration tests package
- `tests/integration/test_db.py` - 12 DB integration tests (channel lifecycle + post dedup)
- `tests/integration/test_scheduler.py` - 5 scheduler tests (channel iteration, error handling, config)
- `src/models/post.py` - Changed JSONB → JSON for SQLite test compatibility

## Decisions Made
- **Channel data snapshot before loop:** Extracted ORM attributes (id, username) into plain dicts before the channel iteration loop. After `session.rollback()`, SQLAlchemy expires all loaded objects, causing `MissingGreenlet` errors when accessing attributes outside greenlet context. Snapshot pattern avoids this entirely.
- **Dual-path upsert:** PostgreSQL uses `INSERT … ON CONFLICT DO NOTHING` for efficient bulk deduplication. SQLite fallback uses individual existence checks since SQLite's `on_conflict_do_nothing` support differs. Both paths tested and verified.
- **Post model JSON→JSONB tradeoff:** Changed `JSONB` to `JSON` in Post model for SQLite test compatibility. In production with PostgreSQL, `JSON` maps to `json` type which is functionally equivalent to `jsonb` for our read-heavy use case (no JSON operators needed in queries yet).

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed Post model JSONB → JSON for SQLite test compatibility**
- **Found during:** Task 1 (integration tests)
- **Issue:** Post model used `sqlalchemy.dialects.postgresql.JSONB` which SQLite cannot compile. Test fixtures use SQLite (from Plan 01 decision), causing `UnsupportedCompilationError` when creating tables.
- **Fix:** Changed JSONB to `sqlalchemy.JSON` which works on both PostgreSQL and SQLite. In production PostgreSQL, `JSON` stores as `json` type (text-based), functionally equivalent to `jsonb` for our use case.
- **Files modified:** src/models/post.py
- **Verification:** All 52 tests pass, including 12 new integration tests with SQLite
- **Committed in:** f628197 (Task 1 commit)

**2. [Rule 1 - Bug] Fixed expired-attribute errors after session.rollback() in scraping_job**
- **Found during:** Task 2 (scheduler tests)
- **Issue:** After `session.rollback()` on a failed channel, SQLAlchemy expired ALL loaded objects. Accessing subsequent channel attributes in the loop triggered `MissingGreenlet` errors (lazy load outside greenlet context).
- **Fix:** Snapshot channel data (id, username) into plain dicts before the iteration loop. Loop operates on dict values, never touching ORM objects after potential rollback.
- **Files modified:** src/scheduler/jobs.py
- **Verification:** test_scraping_job_handles_generic_exception passes — first channel raises RuntimeError, second channel still processes correctly
- **Committed in:** bff3502 (Task 2 commit)

---

**Total deviations:** 2 auto-fixed (2 bugs)
**Impact on plan:** Both fixes necessary for correctness. No scope creep.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Complete data pipeline: scraper → repository → scheduler → main.py
- Repository layer provides clean API for Phase 2 (API layer)
- APScheduler runs in single async process, ready for Docker deployment
- All 52 tests pass (12 integration DB + 5 integration scheduler + 35 unit)
- main.py can start with `python -m src.main` or `docker compose up`

## Self-Check: PASSED

- All 10 key files verified on disk
- Both task commits (f628197, bff3502) found in git history
- Success criteria verified: ON CONFLICT dedup, zero duplicates test, scheduler interval, main.py import, all tests pass

---
*Phase: 01-data-foundation*
*Completed: 2026-04-02*
