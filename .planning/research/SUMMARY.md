# Project Research Summary

**Project:** Telegram Channel Monitor Bot
**Domain:** Telegram bot — channel monitoring, web scraping, LLM summarization, scheduled digests
**Researched:** 2026-04-02
**Confidence:** HIGH

## Executive Summary

This is a Telegram bot that monitors public channels via t.me/s/* web scraping, stores posts in PostgreSQL, and delivers periodic digests to users in multiple formats (truncated, full text, LLM-summarized). The product type is well-established — several competitors exist — but none combine configurable digest formats, pre-set channel collections, and engagement-based hot post detection. The recommended approach is a single-process Python async monolith using aiogram 3.x for the bot framework, APScheduler 3.x for cron jobs, SQLAlchemy 2.0 + asyncpg for data access, and the OpenAI SDK for LLM summarization. This architecture is intentionally simple: one asyncio event loop runs the bot, scheduler, and scraper concurrently, deferring distributed complexity until scale demands it (1K+ users).

The primary risks are external-dependency fragility. The t.me/s/* scraper depends on unofficial HTML that Telegram can change without notice — selector breakage is not a question of "if" but "when," so monitoring and validation must be built from day one. Rate limiting from both Telegram (scraping) and the Bot API (30 msg/sec broadcast cap) constrain throughput. LLM API costs can explode linearly if summaries are generated per-user instead of per-post. Mitigating these risks early — centralized selectors, scrape health metrics, broadcast queues with rate control, and per-post summary caching — is essential to avoid production fires.

## Key Findings

### Recommended Stack

The stack is mature, well-documented, and all components have first-class async support. Every library chosen is the de facto standard in its category for Python async development.

**Core technologies:**
- **Python 3.12+:** Runtime — 3.12 hits the sweet spot for library compatibility (aiogram requires >=3.10) and performance
- **aiogram 3.26.0:** Telegram Bot framework — async-first, router pattern, inline keyboards, Bot API 9.5 support
- **SQLAlchemy 2.0.48 + asyncpg 0.31.0:** ORM + PostgreSQL driver — async sessions, 5x faster than psycopg3, type-safe queries
- **APScheduler 3.11.2:** Cron scheduling — stable async scheduler with SQLAlchemy job store (avoid 4.x, still alpha)
- **openai 2.30.0:** LLM client — AsyncOpenAI, works with any OpenAI-compatible endpoint via `base_url`
- **httpx 0.28.1 + selectolax 0.4.7:** Scraping — async HTTP + Lexbor-based HTML parser (25x faster than BeautifulSoup)
- **pydantic-settings 2.13.1:** Config — typed settings from env vars with validation
- **alembic 1.18.4:** Migrations — auto-generate from SQLAlchemy models

### Expected Features

**Must have (P1 — table stakes for launch):**
- Channel scraping via t.me/s/* — the entire value proposition depends on reading posts
- PostgreSQL storage with post metadata (text, views, date, reactions, channel)
- User registration via /start with Telegram user ID
- Channel subscription management (add/remove) via inline buttons
- Scheduled digest delivery (hourly/daily) — the main user-facing output
- Two digest formats: first 200 chars (truncation) and full text
- Keyword filtering (per-user allowlist) — signal extraction is why users choose digest bots
- Deduplication across channels — without it, digests feel spammy
- Basic bot commands (/start, /help, /settings)

**Should have (P2 — competitive differentiators):**
- LLM-generated summaries (third digest format) — core differentiator, most competitors just forward text
- Hot posts detection by views/reactions relative to channel average
- Pre-set channel collections by topic — zero competitors bundle curated lists, major onboarding advantage
- Instant notification frequency (batched 5-min checks, not true real-time)
- Inline button management UI — more discoverable than slash commands
- Ad/spam regex filtering — Russian TG channels are notoriously spammy

**Defer (v2+):**
- Natural-language Q&A over posts (requires RAG/vector DB)
- Media support (storage/bandwidth implications)
- Web dashboard (bot is the interface)
- Multi-language UI (v1 is Russian-only per project constraints)
- Social features (shared digests)

### Architecture Approach

A single-process async monolith with four layers: Presentation (aiogram handlers/keyboards), Service (business logic), Ingestion (scraper, scheduler, LLM client), and Data (PostgreSQL via repository pattern). Services are pure business logic with no framework dependencies, called by both bot handlers and scheduler jobs through dependency injection. This prevents logic duplication and keeps the system testable.

**Major components:**
1. **Scraper** — isolated async module with rate limiting, retry logic, and structured parsing (httpx + selectolax)
2. **Service Layer** — ChannelService, UserService, SubscriptionService, SummaryService, NotificationService orchestrate all business rules
3. **Repository Layer** — one repository per table, services depend on repository interfaces not raw SQL
4. **Scheduler** — APScheduler with separate jobs for scraping and digest dispatch, using interval/cron triggers
5. **LLM Client** — stateless OpenAI-compatible client, injected into SummaryService, with fallback to truncation on failure

### Critical Pitfalls

1. **t.me/s/* HTML selector fragility** — Telegram changes markup without notice. Build scrape validation (monitor posts_found vs posts_parsed ratio), centralize selectors, and alert on extraction rate drops below 80%.
2. **Rate limiting and IP blocking** — Minimum 2-3 seconds between requests, stagger channel scraping, exponential backoff on 429. Test with 50+ channels before launch.
3. **Deduplication requires stable post IDs** — Use `data-post` attribute (`channel/12345`), not post text. Composite unique constraint on `(channel_name, post_number)`. Re-scraping must be idempotent.
4. **LLM cost explosion** — Summarize once per post, cache in DB. Never summarize per-user. Set daily call budgets with fallback to truncation. Consider cheaper models (GPT-4o-mini) for routine summaries.
5. **Bot API broadcast limits** — 30 msg/sec global, 1 msg/sec per chat. Queue digests with controlled send rate, handle `retry_after` on 429, split long messages at channel boundaries (4096 char limit).

## Implications for Roadmap

Based on combined research, suggested phase structure:

### Phase 1: Data Foundation & Scraper
**Rationale:** Every downstream feature depends on data being collected and persisted. This is the critical path — the scraper is both the highest-risk component (external dependency on t.me/s/* HTML) and the most foundational.
**Delivers:** Working scraper that fetches, parses, deduplicates, and stores posts from public Telegram channels with health monitoring.
**Addresses:** Channel scraping (P1), PostgreSQL storage (P1), deduplication (P1)
**Avoids:** HTML selector fragility, rate limiting/IP blocking, view count parsing errors, deduplication without stable IDs
**Stack:** httpx, selectolax, asyncpg/SQLAlchemy, alembic, pydantic-settings, tenacity
**Key tests:** Scrape 50 channels without 429s; re-scrape same channel with 0 new inserts; parse "4.22M views" correctly

### Phase 2: Bot Core & User Management
**Rationale:** User registration and channel subscriptions are leaf dependencies — almost every feature flows from having a registered user with channel preferences. Build this before digests or notifications.
**Delivers:** Functional Telegram bot with /start registration, channel subscription management via inline buttons, and user preferences storage.
**Addresses:** User registration (P1), channel subscription CRUD (P1), /start and /help commands (P1), inline button UI (P2)
**Avoids:** callback_data 64-byte limit (use compact `action:id:page` format), overwhelming inline keyboard layouts
**Stack:** aiogram 3.26.0, SQLAlchemy models for users/subscriptions/channels
**Key tests:** 32-char channel username in callback_data stays under 64 bytes; subscription persists across bot restart

### Phase 3: Digest Engine & Notification
**Rationale:** Digest delivery is the primary user-facing value. With data (Phase 1) and users (Phase 2) in place, the digest engine ties them together. Build without LLM first (truncation/full-text only) to validate the pipeline.
**Delivers:** Scheduled hourly/daily digests sent to users, with message splitting for long digests, rate-limited broadcast, and two formats (200 chars, full text).
**Addresses:** Scheduled digest delivery (P1), basic digest format (P1), full text format (P1), keyword filtering (P1)
**Avoids:** Bot API broadcast limits (25 msg/sec send rate), 4096-char message limit (split at channel boundaries), timezone mismatches (store user timezone)
**Stack:** APScheduler 3.11.2, aiogram send_message with parse_mode=HTML
**Key tests:** Send to 100 mock users without 429s; 30-channel digest splits into multiple messages correctly

### Phase 4: LLM Summarization
**Rationale:** LLM summaries are the core differentiator but not a blocker for the basic pipeline. Adding LLM after the digest engine works means the "first 200 chars" format serves as a guaranteed fallback.
**Delivers:** LLM-generated summaries as a third digest format, with per-post caching, cost tracking, and graceful fallback to truncation.
**Addresses:** LLM summaries (P2)
**Avoids:** LLM cost explosion (summarize once per post, cache in DB, daily budget), API downtime (fallback to truncation)
**Stack:** openai 2.30.0 AsyncOpenAI client, configurable base_url for local/self-hosted LLMs
**Key tests:** Disconnect LLM API, verify user gets truncated format instead of error; verify same post is not summarized twice

### Phase 5: Polish & Differentiators
**Rationale:** Competitive features that improve retention and onboarding. These are meaningful but not required for the core loop (scrape → store → digest → deliver) to work.
**Delivers:** Hot posts detection, pre-set channel collections, ad/spam regex filtering, instant notification frequency.
**Addresses:** Hot posts (P2), pre-set collections (P2), ad/spam filtering (P2), instant frequency (P2)
**Stack:** Pure service-layer features, no new external dependencies
**Key tests:** Hot post detection surfaces outlier engagement; preset "AI" collection subscribes user to 10+ channels

### Phase Ordering Rationale

- **Phase 1 first** because the scraper is the highest-risk, highest-dependency component. Failure here blocks everything.
- **Phase 2 before Phase 3** because digests need users with subscription preferences. Building digests without the user model creates throwaway code.
- **Phase 3 without LLM** validates the entire scrape→store→digest→deliver pipeline with the simplest formats. LLM is a value-add, not a foundation.
- **Phase 4 isolated** because LLM integration introduces external API costs and failure modes. It should be the only new variable when added.
- **Phase 5 last** because competitive features depend on having a working core to enhance. They're the icing, not the cake.

### Research Flags

Phases likely needing deeper research during planning:
- **Phase 1:** t.me/s/* HTML structure — selectors should be validated against live pages at implementation time; pagination via `?before=XX` needs testing
- **Phase 4:** LLM prompt engineering for Russian-language summaries — no existing prompt templates found in research; will need iterative testing

Phases with standard patterns (skip research-phase):
- **Phase 2:** aiogram Router/handler patterns are well-documented and straightforward
- **Phase 3:** APScheduler + async digest dispatch is a common pattern with clear documentation
- **Phase 5:** Pure business logic features with no novel integrations

## Confidence Assessment

| Area | Confidence | Notes |
|------|------------|-------|
| Stack | HIGH | All versions verified against PyPI. Compatibility matrix confirmed. Every library is the standard in its category. |
| Features | MEDIUM | Feature priorities based on competitor analysis (4 repos examined). Some assumptions about user expectations (e.g., keyword filtering as P1) need validation with real users. |
| Architecture | HIGH | Single-process async monolith is a well-proven pattern for bots of this scale. Repository + service layer separation is standard. Build order follows dependency graph. |
| Pitfalls | HIGH | Verified against official Telegram Bot API docs (Bot API 9.5), live t.me/s/* pages, and PostgreSQL documentation. Rate limits, message limits, and callback_data limits are documented facts. |

**Overall confidence:** HIGH

### Gaps to Address

- **t.me/s/* pagination behavior:** Research confirms `?before=XX` parameter exists, but full pagination logic (how many posts per page, when to stop paginating) needs hands-on testing during Phase 1 implementation.
- **LLM prompt for Russian summaries:** No existing prompt templates found in competitor code. Will need prompt engineering iteration during Phase 4. Consider starting with a simple instruction and iterating based on output quality.
- **View count locale handling:** t.me/s/* pages render in the channel's language. The "views" text and number formatting may vary. The parser needs testing against diverse channels (Russian, English, Arabic) during Phase 1.
- **Optimal scraping frequency:** Research suggests 5-minute intervals for active channels, 30 minutes for quiet ones, but the adaptive algorithm needs tuning based on real-world post frequency data collected over time.

## Sources

### Primary (HIGH confidence)
- pypi.org — Version verification for all recommended libraries (aiogram 3.26.0, SQLAlchemy 2.0.48, asyncpg 0.31.0, APScheduler 3.11.2, openai 2.30.0, httpx 0.28.1, selectolax 0.4.7, alembic 1.18.4, pydantic-settings 2.13.1)
- Telegram Bot API official docs (core.telegram.org/bots/api) — Bot API 9.5, message limits (4096 chars), callback_data limit (64 bytes)
- Telegram Bots FAQ (core.telegram.org/bots/faq) — Rate limits: 1 msg/sec per chat, 30 msg/sec broadcast
- Live t.me/s/* pages — HTML structure verification, view count format analysis, pagination behavior

### Secondary (MEDIUM confidence)
- GitHub: Riniba/TelegramMonitor (234 stars) — Competitor feature analysis, C# keyword monitoring
- GitHub: shalom2552/InstaBriefBot — Competitor with GPT summaries, Telethon + Aiogram
- GitHub: vetalin/telegram-daily-digest — TypeScript daily digest competitor
- GitHub: N-SUDY/News-Forwarding-Bot — ML duplicate detection approach
- aiogram 3.x documentation — Router/Dispatcher patterns, inline keyboard builders

### Tertiary (LOW confidence)
- GitHub: ahmeterenodaci/telegram-message-scraper — t.me/s/* scraping validation (6 stars, minimal codebase)
- APScheduler documentation — AsyncScheduler patterns (some docs reference v4 alpha; stick to v3 patterns)

---
*Research completed: 2026-04-02*
*Ready for roadmap: yes*
