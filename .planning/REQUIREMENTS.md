# Requirements: Telegram Channel Monitor Bot

**Defined:** 2026-04-02
**Core Value:** Пользователь получает релевантные сводки из отслеживаемых Telegram-каналов в одном месте

## v1 Requirements

### Scraping & Storage

- [ ] **SCRP-01**: Bot can parse posts from public Telegram channels via t.me/s/* with pagination
- [ ] **SCRP-02**: Bot extracts post metadata: text, post_id, date, views, reactions, author, link previews
- [ ] **SCRP-03**: Bot handles rate limiting (HTTP 429) with exponential backoff
- [ ] **STOR-01**: Bot stores posts in PostgreSQL with full metadata (text, views, reactions, datetime, channel)
- [ ] **STOR-02**: Bot deduplicates posts across channels using post_id (data-post attribute)
- [ ] **STOR-03**: Bot stores channel information (name, username, last_scraped timestamp)

### User Management

- [ ] **USER-01**: User can register via /start command (Telegram user ID as primary key)
- [ ] **USER-02**: User can set digest format preference (200 chars / full text)
- [ ] **USER-03**: User can set notification frequency (immediate / hourly / daily)
- [ ] **USER-04**: User can set timezone for scheduled digests
- [ ] **USER-05**: User can access /help and /settings commands

### Subscriptions

- [ ] **SUBS-01**: User can subscribe to a channel via inline button or command
- [ ] **SUBS-02**: User can unsubscribe from a channel via inline button
- [ ] **SUBS-03**: User can view their current subscriptions list
- [ ] **SUBS-04**: User can subscribe to pre-set channel collections by topic (AI, crypto, business, etc.)
- [ ] **SUBS-05**: Bot provides curated channel collections as seed data for onboarding

### Digest & Notifications

- [ ] **DIGE-01**: Bot generates digest in "first 200 chars" format per post
- [ ] **DIGE-02**: Bot generates digest in "full text" format per post
- [ ] **DIGE-03**: Bot generates digest in "LLM summary" format (brief Russian-language recap)
- [ ] **DIGE-04**: Bot sends digests on schedule based on user frequency preference (hourly/daily)
- [ ] **DIGE-05**: Bot sends immediate notifications when new posts match user filters
- [ ] **DIGE-06**: Bot respects Telegram Bot API rate limits (30 msg/sec broadcast)
- [ ] **DIGE-07**: Bot deduplicates posts in digest across subscribed channels

### Filtering & Hot Posts

- [ ] **FILT-01**: User can set keyword allowlist for post filtering
- [ ] **FILT-02**: User can set keyword blocklist for post filtering
- [ ] **HOTP-01**: Bot detects hot posts based on views/reactions relative to channel average
- [ ] **HOTP-02**: Bot marks hot posts in digests with visual indicator

### Scheduling & Infrastructure

- [ ] **CRON-01**: Scheduler runs periodic channel scraping (configurable interval per channel)
- [ ] **CRON-02**: Scheduler runs periodic digest generation and delivery
- [ ] **CRON-03**: Bot runs as single async process (aiogram + APScheduler in one event loop)

### Inline UI

- [ ] **UI-01**: Bot uses inline keyboard buttons for subscription management (add/remove channels)
- [ ] **UI-02**: Bot uses inline keyboard buttons for digest format selection
- [ ] **UI-03**: Bot uses inline keyboard buttons for notification frequency selection
- [ ] **UI-04**: Bot uses compact callback_data encoding (DB-referenced state IDs, 64-byte limit)

## v2 Requirements

### Enhanced Features

- **ADSF-01**: Regex-based ad/spam filtering for Russian-language patterns in digests
- **INST-01**: Near-real-time "instant" notification mode (5-min polling for subscribed channels)
- **TOPC-01**: Auto-classification of uncategorized channels by topic
- **TRAN-01**: Multi-language bot interface (English support)

### Future Consideration

- **QAAS-01**: Natural-language Q&A over stored posts (RAG pipeline with vector DB)
- **MEDI-01**: Media support (image thumbnails, video links) in digests
- **SHAR-01**: Digest sharing between users
- **WEBB-01**: Web dashboard for power users

## Out of Scope

| Feature | Reason |
|---------|--------|
| Private/closed channel support | Requires Telegram user account (Telethon), ToS risk, massively increases complexity |
| Media downloading/storage | 10-100x storage cost, transcoding needed, LLM can't summarize images without vision API |
| Web dashboard | Doubles frontend work, bot IS the interface |
| Multi-language UI (v1) | Scope creep, v1 Russian only per PROJECT.md |
| Voice/audio notifications | Not requested, high complexity |
| OAuth / external auth | Telegram user ID sufficient for v1 |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| SCRP-01 | Phase 1 | Pending |
| SCRP-02 | Phase 1 | Pending |
| SCRP-03 | Phase 1 | Pending |
| STOR-01 | Phase 1 | Pending |
| STOR-02 | Phase 1 | Pending |
| STOR-03 | Phase 1 | Pending |
| CRON-01 | Phase 1 | Pending |
| CRON-03 | Phase 1 | Pending |
| USER-01 | Phase 2 | Pending |
| USER-02 | Phase 2 | Pending |
| USER-03 | Phase 2 | Pending |
| USER-04 | Phase 2 | Pending |
| USER-05 | Phase 2 | Pending |
| SUBS-01 | Phase 2 | Pending |
| SUBS-02 | Phase 2 | Pending |
| SUBS-03 | Phase 2 | Pending |
| SUBS-04 | Phase 2 | Pending |
| SUBS-05 | Phase 2 | Pending |
| UI-01 | Phase 2 | Pending |
| UI-02 | Phase 2 | Pending |
| UI-03 | Phase 2 | Pending |
| UI-04 | Phase 2 | Pending |
| DIGE-01 | Phase 3 | Pending |
| DIGE-02 | Phase 3 | Pending |
| DIGE-03 | Phase 3 | Pending |
| DIGE-04 | Phase 3 | Pending |
| DIGE-05 | Phase 3 | Pending |
| DIGE-06 | Phase 3 | Pending |
| DIGE-07 | Phase 3 | Pending |
| CRON-02 | Phase 3 | Pending |
| FILT-01 | Phase 3 | Pending |
| FILT-02 | Phase 3 | Pending |
| HOTP-01 | Phase 3 | Pending |
| HOTP-02 | Phase 3 | Pending |

**Coverage:**
- v1 requirements: 34 total
- Mapped to phases: 34
- Unmapped: 0 ✓

---
*Requirements defined: 2026-04-02*
*Last updated: 2026-04-02 after initial definition*
