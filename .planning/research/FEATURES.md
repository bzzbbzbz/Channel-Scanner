# Feature Research

**Domain:** Telegram Channel Monitor / Reader Bot
**Researched:** 2026-04-02
**Confidence:** MEDIUM

## Feature Landscape

### Table Stakes (Users Expect These)

Features users assume exist. Missing these = product feels incomplete.

| Feature | Why Expected | Complexity | Notes |
|---------|--------------|------------|-------|
| Channel subscription management | Every competitor has add/remove channels. This is the core interaction model. | LOW | Inline buttons for add/remove. Straightforward CRUD. |
| Scheduled digest delivery | Users subscribe to channels specifically to get periodic summaries. Without this, the bot has no delivery mechanism. | MEDIUM | Cron-based dispatch at user-configured intervals (hourly/daily). |
| Message scraping from channels | The entire value proposition depends on reading posts. No scraping = no product. | MEDIUM | t.me/s/* via requests+BeautifulSoup. Rate limiting (429) handling required. |
| Persistent storage with metadata | Users expect historical context, not just latest posts. Views/reactions/date are basic metadata. | MEDIUM | PostgreSQL with posts, channels, users, subscriptions tables. |
| Keyword/topic filtering | Every monitoring bot has this. Users want signal, not noise. | MEDIUM | Per-user keyword allowlist/blocklist. Stored in DB, applied at digest generation. |
| User registration & settings | Users expect /start to work and to persist their preferences. | LOW | Telegram user ID as primary key. Store preferences (format, frequency, timezone). |
| /start, /help, basic bot commands | Table stakes for any Telegram bot. Users expect command-based interaction. | LOW | Standard aiogram command handlers. |
| Deduplication | Same news posted across multiple channels. Users hate seeing duplicates in digests. | MEDIUM | Hash-based or content similarity dedup at digest generation time. |

### Differentiators (Competitive Advantage)

Features that set the product apart. Not required, but valuable.

| Feature | Value Proposition | Complexity | Notes |
|---------|-------------------|------------|-------|
| LLM-generated summaries | Core differentiator. InstaBriefBot and telegram-daily-digest are the only competitors doing this, and both are immature (0-2 stars). Most bots just forward raw text. | HIGH | Requires LLM API integration. Prompt engineering for Russian-language summaries. Cost per digest scales with post volume. |
| Multiple digest formats (200 chars / summary / full) | No competitor offers configurable format depth. Gives users control over information density. | MEDIUM | Three rendering paths. "Summary" path depends on LLM. Other two are truncation/fulltext. |
| Hot posts detection (by views/reactions) | Surface what matters most. No open-source competitor does engagement-based ranking well. Acts as a "must-read" filter. | LOW | Sort by view count relative to channel average. Flag outliers above threshold. |
| Pre-set channel collections by topic | Reduces onboarding friction. New users pick "AI", "crypto", "business" and get a curated set. No competitor bundles this. | LOW | Curated channel lists stored as seed data. Users can still add/remove individually. |
| Flexible notification frequency (instant/hourly/daily) | Most competitors are either real-time OR daily. Offering all three captures different user types. | MEDIUM | "Instant" requires per-post processing; hourly/daily are batch. Three scheduling modes. |
| Inline button management UI | Most bots use slash commands. Inline buttons for subscription management are more discoverable and mobile-friendly. | LOW | aiogram InlineKeyboardMarkup. Standard pattern. |
| Ad/spam filtering in digests | telegram-daily-digest advertises "remove fakes, ads and trash." This is valuable for Russian TG channels which are notoriously spammy. | MEDIUM | Pattern-based filtering (regex for common ad patterns) + optional LLM classification. Start with regex. |

### Anti-Features (Commonly Requested, Often Problematic)

Features that seem good but create problems.

| Feature | Why Requested | Why Problematic | Alternative |
|---------|---------------|-----------------|-------------|
| Real-time push for every message | FOMO, want instant updates on specific channels | Defeats the digest purpose. Creates notification fatigue. Rate-limited by t.me/s/* polling. Every competitor that does real-time requires Telegram API user account (Telethon), not t.me/s/*. | Offer "instant" frequency as a batched mode (check every 5 min, send if new posts found). Not true real-time. |
| Private/closed channel support | Users want to monitor private groups they're in | Requires full Telegram user account auth (Telethon/Pyrogram), not just t.me/s/* scraping. Massively increases complexity and legal/ToS risk. | Stay with public channels via t.me/s/*. This is an explicit v1 constraint in PROJECT.md. |
| Media processing (photos, videos, voice) | Rich content is common in TG channels | Media download/storage is 10-100x the storage cost. Video processing requires transcoding. LLM can't summarize images without vision API. | v1: Text only. Extract text from posts, include links to original for media. Explicitly out of scope in PROJECT.md. |
| Web dashboard | Power users want a web UI for management | Doubles frontend work. The bot IS the interface. Adding a web UI means auth, sessions, responsive design, hosting. | All management through bot inline buttons and commands. If needed later, generate a simple static page from DB. |
| Multi-language interface | International users | Scope creep. UI strings everywhere. i18n framework needed. Translation maintenance. | v1: Russian only (per PROJECT.md constraints). Hardcode strings. i18n only after PMF. |
| Natural-language Q&A over stored posts | "What happened with X today?" | Requires RAG pipeline, embedding storage, vector DB. Massive scope addition. InstaBriefBot does this but it's their only feature. | Stick to keyword filtering + LLM summaries. Q&A is a v2+ feature if demand exists. |
| Social features (shared digests, friend subscriptions) | Viral growth, sharing | Requires user relationship model, access control, shared state. Premature for MVP. | Focus on single-user experience. "Share this digest" button as lightweight alternative. |

## Feature Dependencies

```
Channel Scraping (t.me/s/*)
    └──requires──> Post Storage (PostgreSQL)
                        ├──enables──> Keyword Filtering
                        │                └──requires──> User Preferences
                        │                                     └──requires──> User Registration
                        ├──enables──> Hot Posts Detection
                        ├──enables──> Digest Generation
                        │                ├──requires──> User Preferences (format, frequency)
                        │                ├──requires──> Scheduled Delivery (cron)
                        │                └──enhanced by──> LLM Summaries
                        │                                     └──requires──> LLM API Integration
                        └──enables──> Deduplication

Pre-set Channel Collections
    └──requires──> Channel Storage
    └──enhanced by──> Topic Categorization

Inline Button Management
    └──requires──> User Registration
    └──requires──> Channel Subscription CRUD

Ad/Spam Filtering
    └──enhances──> Digest Generation
    └──requires──> Post Storage
```

### Dependency Notes

- **Channel Scraping requires Post Storage:** Scraped data must be persisted before any downstream feature works. This is the critical path.
- **Digest Generation requires User Preferences:** Cannot format or schedule a digest without knowing user's chosen format and frequency.
- **LLM Summaries enhances Digest Generation:** The "summary" format is meaningless without LLM. Other formats (200 chars, full) work without it. LLM is a value-add, not a blocker.
- **Keyword Filtering requires User Preferences:** Filters are per-user. Must have user model before filters work.
- **User Registration is a leaf dependency:** Almost everything flows from having a registered user. Implement early.
- **Pre-set Channel Collections enhances onboarding:** Not strictly required for the system to work, but dramatically improves first-run experience.

## MVP Definition

### Launch With (v1)

Minimum viable product - what's needed to validate the concept.

- [ ] Channel scraping via t.me/s/* — Core value prop. Without this, nothing works.
- [ ] PostgreSQL storage with post metadata (text, views, date, channel) — Required for every downstream feature.
- [ ] User registration via /start — Required for personalization.
- [ ] Channel subscription management (add/remove) via inline buttons — Core user interaction.
- [ ] Scheduled digest delivery (hourly/daily) — The main output users experience.
- [ ] Basic digest format: first 200 chars per post — Simplest format, no LLM needed.
- [ ] Full text digest format — Second format, just send full post text.
- [ ] Keyword filtering (allowlist) — Users need signal extraction to justify using the bot.
- [ ] Deduplication across channels — Quality of life. Without it, digests feel spammy.
- [ ] /start, /help, /settings commands — Basic bot UX.

### Add After Validation (v1.x)

Features to add once core is working.

- [ ] LLM-generated summaries (third format) — Trigger: users ask "can I get summaries instead of full text?" Validate LLM API cost/performance first.
- [ ] Hot posts detection — Trigger: users overwhelmed by digest volume. Needs engagement data collection first.
- [ ] Pre-set channel collections — Trigger: users don't know which channels to subscribe to. Curated onboarding reduces friction.
- [ ] Instant notification frequency — Trigger: users want near-real-time for specific channels. Requires more aggressive polling.
- [ ] Ad/spam regex filtering — Trigger: users complain about ads in digests. Start with Russian-language ad patterns.

### Future Consideration (v2+)

Features to defer until product-market fit is established.

- [ ] Natural-language Q&A over stored posts — Requires RAG/vector DB. Significant scope.
- [ ] Media support (images, video thumbnails) — Storage and bandwidth implications.
- [ ] Topic auto-classification of channels — ML-based categorization of uncategorized channels.
- [ ] Digest sharing between users — Social features.
- [ ] Web dashboard — Only if bot UX proves insufficient.
- [ ] Multi-language interface — Only if demand from non-Russian users justifies it.

## Feature Prioritization Matrix

| Feature | User Value | Implementation Cost | Priority |
|---------|------------|---------------------|----------|
| Channel scraping (t.me/s/*) | HIGH | MEDIUM | P1 |
| Post storage (PostgreSQL) | HIGH | MEDIUM | P1 |
| User registration | HIGH | LOW | P1 |
| Channel subscription CRUD | HIGH | LOW | P1 |
| Scheduled digest (hourly/daily) | HIGH | MEDIUM | P1 |
| Basic digest format (200 chars) | HIGH | LOW | P1 |
| Full text digest format | MEDIUM | LOW | P1 |
| Keyword filtering | HIGH | MEDIUM | P1 |
| Deduplication | MEDIUM | MEDIUM | P1 |
| LLM summaries | HIGH | HIGH | P2 |
| Hot posts detection | MEDIUM | LOW | P2 |
| Pre-set channel collections | MEDIUM | LOW | P2 |
| Instant notification frequency | MEDIUM | MEDIUM | P2 |
| Inline button management | MEDIUM | LOW | P2 |
| Ad/spam filtering | LOW | MEDIUM | P3 |
| Multi-language interface | LOW | HIGH | P3 |
| Natural-language Q&A | MEDIUM | HIGH | P3 |
| Web dashboard | LOW | HIGH | P3 |
| Media support | MEDIUM | HIGH | P3 |

**Priority key:**
- P1: Must have for launch
- P2: Should have, add when possible
- P3: Nice to have, future consideration

## Competitor Feature Analysis

| Feature | Riniba/TelegramMonitor | InstaBriefBot | telegram-daily-digest | Mizuki News Bot | Our Approach |
|---------|----------------------|---------------|----------------------|-----------------|-------------|
| Scraping method | Telegram API (user account) | Telethon (user account) | Unknown | Telethon (user account) | t.me/s/* (no account needed) |
| Digest delivery | Real-time forward only | On-demand /sync | Daily digest | Real-time forward | Scheduled (hourly/daily/instant) |
| AI summaries | No | Yes (GPT) | Yes | No | Yes (LLM API) |
| Keyword filtering | Yes (core feature) | Yes (GPT-extracted) | No | Banned word filter | Yes (user allowlist) |
| Duplicate detection | No | No | Yes (ad/trash removal) | Yes (TF-IDF ML) | Yes (content hashing) |
| Channel collections | No | No | No | No | Yes (pre-set by topic) |
| Multi-user | Yes | Yes (authorized IDs) | Yes | Yes (subscription tiers) | Yes |
| Storage | Not specified | SQLite | Prisma DB | JSON files | PostgreSQL |
| Hot posts | No | No | No | No | Yes (engagement-based) |
| Web UI | Yes | No | No (Next.js planned) | No (GUI for model training) | No (bot only) |
| Format options | Raw forward only | Summary only | Summary only | Formatted forward | 3 formats (200chars/summary/full) |

### Competitive Advantage Summary

Our key differentiators relative to analyzed competitors:

1. **No Telegram account required** — t.me/s/* scraping means no API keys, no user sessions, no ToS risk. Most competitors require Telethon/Pyrogram user accounts.
2. **Configurable digest formats** — No one else offers 3 depth levels. This is genuinely novel.
3. **Pre-set channel collections** — Zero competitors bundle curated channel lists. This is a major onboarding advantage.
4. **Hot posts detection** — Engagement-based surfacing. No open-source competitor does this.
5. **PostgreSQL backend** — Most competitors use SQLite or JSON. PG gives us scalability and full-text search.

### Competitive Disadvantages

1. **Public channels only** — t.me/s/* limits to public. Competitors using Telethon can read private channels. This is an accepted tradeoff per PROJECT.md.
2. **Polling latency** — t.me/s/* scraping is polling-based, not real-time. Competitors with user accounts get push updates. "Instant" mode will have 5-minute latency minimum.
3. **No media** — v1 is text-only. Some competitors forward media.

## Sources

- GitHub: Riniba/TelegramMonitor (234 stars) — C# keyword monitoring bot with web UI. Uses Telegram API user account.
- GitHub: shalom2552/InstaBriefBot — Python bot with GPT summaries, Telethon + Aiogram. On-demand sync, NL Q&A.
- GitHub: vetalin/telegram-daily-digest — TypeScript daily digest with ad/trash removal.
- GitHub: N-SUDY/News-Forwarding-Bot — Python bot with ML duplicate detection, subscription tiers, payment flow.
- GitHub: ahmeterenodaci/telegram-message-scraper (6 stars) — JS library for scraping t.me/s/*. Proves the scraping approach works.
- PROJECT.md — Project constraints and feature requirements as defined by the team.

---
*Feature research for: Telegram Channel Monitor Bot*
*Researched: 2026-04-02*
