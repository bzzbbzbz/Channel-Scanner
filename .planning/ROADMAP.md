# Roadmap: Telegram Channel Monitor Bot

## Overview

Build a Telegram bot that monitors public channels via t.me/s/* scraping, stores posts in PostgreSQL, and delivers configurable digests to users. The journey: first establish a reliable data pipeline (scrape → store → deduplicate), then build the user-facing bot (registration → subscriptions → preferences), and finally deliver the core value (personalized digests with filtering, hot posts, and LLM summaries).

## Phases

**Phase Numbering:**
- Integer phases (1, 2, 3): Planned milestone work
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED)

Decimal phases appear between their surrounding integers in numeric order.

- [x] **Phase 1: Data Foundation** - Scrape public Telegram channels, store posts in PostgreSQL, schedule periodic ingestion
- [ ] **Phase 2: Bot & Subscriptions** - User registration, channel subscriptions, preferences, and inline UI
- [ ] **Phase 3: Digests & Intelligence** - Scheduled digests, LLM summaries, keyword filtering, hot post detection

## Phase Details

### Phase 1: Data Foundation
**Goal**: Posts from public Telegram channels are continuously collected and stored without duplicates
**Depends on**: Nothing (first phase)
**Requirements**: SCRP-01, SCRP-02, SCRP-03, STOR-01, STOR-02, STOR-03, CRON-01, CRON-03
**Success Criteria** (what must be TRUE):
  1. Bot scrapes posts from any public channel via t.me/s/* and stores them with full metadata (text, views, reactions, date, link previews)
  2. Re-scraping the same channel produces zero duplicate posts (idempotent via data-post attribute)
  3. Scraping handles HTTP 429 rate limiting with exponential backoff without data loss
  4. Scheduler runs periodic channel scraping at configurable intervals in a single async process
  5. Channel information (name, username, last_scraped timestamp) is maintained and queryable
**Plans**: 3 plans

Plans:
- [x] 01-01-PLAN.md — Project scaffolding, config system, DB models, migrations, Docker Compose
- [x] 01-02-PLAN.md — Telegram HTML parser, async HTTP client, scraper service with rate limiting
- [x] 01-03-PLAN.md — Repository layer with deduplication, scheduler, application entry point

### Phase 2: Bot & Subscriptions
**Goal**: Users can register, subscribe to channels (including curated collections), and configure their digest preferences via inline buttons
**Depends on**: Phase 1
**Requirements**: USER-01, USER-02, USER-03, USER-04, USER-05, SUBS-01, SUBS-02, SUBS-03, SUBS-04, SUBS-05, UI-01, UI-02, UI-03, UI-04
**Success Criteria** (what must be TRUE):
  1. User registers via /start and can access /help and /settings commands
  2. User can subscribe and unsubscribe to individual channels via inline buttons
  3. User can view their current subscription list
  4. User can subscribe to pre-set channel collections by topic (AI, crypto, business, etc.) as seed data
  5. User can select digest format (200 chars / full text / LLM summary) and notification frequency (immediate / hourly / daily) via inline buttons
  6. All inline interactions use compact callback_data encoding that stays under the 64-byte Telegram limit
**Plans**: TBD

Plans:
- [ ] 02-01: TBD
- [ ] 02-02: TBD

### Phase 3: Digests & Intelligence
**Goal**: Users receive personalized, filtered digests on schedule with hot post highlights and LLM-generated summaries
**Depends on**: Phase 2
**Requirements**: DIGE-01, DIGE-02, DIGE-03, DIGE-04, DIGE-05, DIGE-06, DIGE-07, CRON-02, FILT-01, FILT-02, HOTP-01, HOTP-02
**Success Criteria** (what must be TRUE):
  1. User receives scheduled digests (hourly/daily) containing posts from subscribed channels in their chosen format (200 chars, full text, or LLM summary)
  2. User receives immediate notifications when new posts match their keyword allowlist
  3. Digests respect Telegram Bot API rate limits (30 msg/sec) and split long messages at the 4096-char boundary
  4. LLM-generated summaries are cached per post (not per user) with fallback to truncation on API failure
  5. Hot posts are detected based on views/reactions relative to channel average and visually marked in digests
  6. Keyword allowlist and blocklist filter posts before inclusion in digest
  7. Posts appearing in multiple subscribed channels are deduplicated in the digest
**Plans**: TBD

Plans:
- [ ] 03-01: TBD
- [ ] 03-02: TBD

## Progress

**Execution Order:**
Phases execute in numeric order: 1 → 2 → 3

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Data Foundation | 3/3 | Complete | 2026-04-02 |
| 2. Bot & Subscriptions | 0/? | Not started | - |
| 3. Digests & Intelligence | 0/? | Not started | - |
