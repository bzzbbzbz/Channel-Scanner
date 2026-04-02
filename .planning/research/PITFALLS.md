# Pitfalls Research

**Domain:** Telegram Channel Monitor Bot (web scraping + Bot API + LLM summarization)
**Researched:** 2026-04-02
**Confidence:** HIGH (verified against official Telegram Bot API docs, live t.me/s/* pages, and PostgreSQL documentation)

## Critical Pitfalls

### Pitfall 1: t.me/s/* HTML Selector Fragility

**What goes wrong:**
Telegram can change the HTML structure of t.me/s/* pages at any time without notice. CSS classes, DOM nesting, and data attributes are not a stable API. A scraping bot built on `BeautifulSoup` selectors breaks silently — you stop getting new posts and don't realize it until users complain about stale data.

**Why it happens:**
Developers treat t.me/s/* like an API with implicit contracts. It isn't. Telegram has no obligation to maintain this HTML structure. The page exists for human browsing, not machine consumption.

**How to avoid:**
- Build a scraper validation layer: after each parse, assert that expected data fields (post text, date, views) were actually extracted. If extraction rate drops below a threshold (e.g., <80% of posts yield data), alert immediately.
- Keep selectors in a centralized config, not scattered across code. When Telegram changes markup, you fix one place.
- Write integration tests that fetch a known live channel and verify parse output structure.
- Monitor scraping health: track `posts_found vs posts_parsed` ratio per channel per run. A sudden drop = selector breakage.

**Warning signs:**
- Parse success rate drops below 90% for any channel
- Empty post text fields in database for channels that are actively posting
- `None` / empty values where post content should be
- Scraping returns 0 new posts for channels known to be active

**Phase to address:**
Phase 1 (scraper core) — build validation and monitoring from day one, not as an afterthought.

---

### Pitfall 2: t.me/s/* Rate Limiting and IP Blocking

**What goes wrong:**
Scraping too aggressively gets your server IP temporarily blocked or throttled by Telegram. The page returns HTTP 429, empty responses, or Cloudflare challenges. For a bot monitoring 50+ channels every 5 minutes, this happens fast.

**Why it happens:**
t.me/s/* is served through Cloudflare. There's no documented rate limit, but empirical evidence shows that sustained high-frequency requests from a single IP trigger protection. Developers test with 2-3 channels in dev, deploy to 50+ channels in prod, and get blocked on day one.

**How to avoid:**
- Implement per-request delay: minimum 2-3 seconds between requests to t.me/s/*
- Stagger channel scraping across the polling interval rather than hitting all channels simultaneously
- Use exponential backoff on 429/5xx responses: double wait time, max 5 minutes
- Track `last_scraped_at` per channel and only scrape if enough time has passed
- Consider a scraping queue (e.g., `asyncio.Queue`) with controlled concurrency
- Never use parallel requests to the same t.me/s/* page

**Warning signs:**
- Increasing 429 response codes in logs
- Scraping latency spikes (normally <2s, suddenly >10s)
- Empty HTML responses with Cloudflare challenge pages
- Posts suddenly stop appearing for ALL channels simultaneously

**Phase to address:**
Phase 1 (scraper core) — rate limiting and backoff must be built into the scraper from the start.

---

### Pitfall 3: View Count and Reaction Parsing Is Deceptively Hard

**What goes wrong:**
t.me/s/* shows view counts as localized human-readable strings like "4.22M views", "781K views", "991K views". Reactions appear as emoji+count like "👍20.9K", "❤6.14K". Developers write naive parsers that handle "K" and "M" but miss edge cases: numbers without suffix ("1,234" with locale-specific separators), very large numbers, missing view counts on newer posts, or reactions with zero counts.

**Why it happens:**
The format looks simple at first glance. Developers parse a few examples, write regex, and move on. Edge cases only surface in production with diverse channels.

**How to avoid:**
- Build a dedicated parser with exhaustive test cases covering: plain numbers ("1234"), locale-formatted ("1,234", "1.234"), abbreviated ("1.2K", "4.22M", "1.5B"), missing values, and "views" suffix in different languages (the page language follows the channel's language setting)
- Store raw string alongside parsed value for debugging
- Log unparseable values for manual review
- Consider that t.me/s/* page locale may vary per channel — the "views" text can be in the channel's language

**Warning signs:**
- View counts in database that are all multiples of 1000 (K parsed but not multiplied)
- Negative or zero view counts for popular channels
- Reaction counts that don't match what's visible on the page
- Different parsing results for the same channel over time

**Phase to address:**
Phase 1 (scraper core) — build and test the parser thoroughly before building anything on top of it.

---

### Pitfall 4: Deduplication Without Stable Post IDs

**What goes wrong:**
t.me/s/* doesn't provide a stable, unique post ID in the HTML. Developers try to deduplicate by post text + date, which fails when: posts are edited (same ID, new text), posts have identical text (common in news channels), or timestamps shift on re-scrape. The database fills with duplicates, or real posts get silently dropped.

**Why it happens:**
The HTML does contain post IDs in the `data-post` attribute (format: `channel/12345`) and in the URL structure. But developers miss this because they focus on visible content, not DOM attributes. Or they use message text as a natural key, which is not unique.

**How to avoid:**
- Extract post ID from `data-post` attribute on each message element (format: `"channel_name/12345"`)
- Use `channel_name + post_number` as the unique key in PostgreSQL (composite unique constraint)
- Never use post text as part of the uniqueness check
- Handle edited posts: if post ID exists but text differs, update the record and set `edited_at`
- Build idempotency: re-scraping the same page should produce zero new inserts

**Warning signs:**
- Growing duplicate count in `SELECT text, COUNT(*) FROM posts GROUP BY text HAVING COUNT(*) > 1`
- Post count growing faster than the channel actually posts
- Users seeing the same post multiple times in summaries

**Phase to address:**
Phase 1 (scraper core) — get deduplication right before building any downstream features.

---

### Pitfall 5: LLM API Cost Explosion

**What goes wrong:**
Summarizing every post from every channel for every user with different format preferences leads to massive LLM API costs. A bot monitoring 50 channels averaging 20 posts/day = 1000 posts/day. If 10 users each want summaries of their 20 subscribed channels = potentially thousands of LLM calls per day. At $0.01-0.03 per call, this becomes $30-90/day or $900-2700/month.

**Why it happens:**
Developers don't think about cost per-user-per-channel-per-format. They test with 1 user, 3 channels, and the costs look trivial. Linear scaling kills them.

**How to avoid:**
- **Summarize once per post, not per user.** Generate summary when post is first scraped, store in DB. Users get the cached summary.
- **Only generate LLM summaries for the "brief summary" format.** "First 200 chars" and "full text" need zero LLM calls.
- **Batch posts for summarization.** Instead of summarizing each post individually, batch the last N posts from a channel and summarize together (fewer API calls, better context).
- **Set daily/hourly LLM call budgets.** Hard-stop when budget exceeded, fall back to "first 200 chars" format.
- **Track cost per call and expose in admin metrics.** Know your burn rate.
- **Consider using a cheaper/smaller model for summaries** (e.g., GPT-4o-mini, Claude Haiku) instead of full-size models.

**Warning signs:**
- LLM API costs growing linearly with user count
- Same post being summarized multiple times for different users
- Summary generation taking longer than scraping
- Users on "free" tier burning LLM budget with trivial summaries

**Phase to address:**
Phase 2 (LLM integration) — design cost architecture before writing the first LLM call.

---

### Pitfall 6: Bot API Rate Limits on Broadcast

**What goes wrong:**
Telegram limits bots to ~30 messages per second for bulk notifications, and ~1 message per second per individual chat (per Telegram FAQ). When the hourly/daily digest fires and you need to send 500 users their summaries, you hit rate limits. Messages fail silently with 429, or get severely delayed.

**Why it happens:**
The Telegram FAQ explicitly states: "In a single chat, avoid sending more than one message per second... For bulk notifications, bots are not able to broadcast more than about 30 messages per second." Developers don't discover this until they have enough users to trigger the limit.

**How to avoid:**
- Implement a message queue with controlled send rate (max 25 msg/sec to leave headroom)
- Use exponential backoff on 429: parse `retry_after` from the error response and wait that many seconds
- For digest messages, spread sends across time: don't blast all users at HH:00
- Split long digests into multiple messages (4096 char limit per message) with controlled delays between parts
- Track `last_message_sent_at` per user to avoid hitting per-chat 1 msg/sec limit
- Consider using `sendMessage` with `parse_mode=HTML` to fit more content (formatting is denser than Markdown)

**Warning signs:**
- 429 errors in Bot API response logs
- Users reporting they didn't receive their scheduled digest
- Messages arriving out of order
- `retry_after` parameter appearing in API error responses

**Phase to address:**
Phase 2 (notification system) — build rate-limit-aware sending from the start.

---

### Pitfall 7: Inline Keyboard `callback_data` 64-Byte Limit

**What goes wrong:**
Telegram's `callback_data` field on `InlineKeyboardButton` is limited to 64 bytes. Developers try to encode channel IDs, actions, and pagination state in callback data and exceed this limit. The button silently fails or the callback is never received.

**Why it happens:**
64 bytes seems like a lot until you try to encode: `action:unsubscribe:channel:very_long_channel_name_here:page:3`. With channel usernames that can be up to 32 characters, plus action identifiers, you run out fast.

**How to avoid:**
- Use short action codes: `unsub:12345:3` instead of `action:unsubscribe:channel_name:page:3`
- Store state in PostgreSQL, reference it by a short ID in `callback_data`
- Use a pattern: `action_type:entity_id:page_number` where entity_id is the DB primary key (integer)
- Never include human-readable strings in callback_data
- Test with the longest possible channel username (32 chars) to verify you stay under 64 bytes
- Alternative: use `callback_url` for web app buttons, but for pure bot UX, stick to compact `callback_data`

**Warning signs:**
- Inline buttons stop working after adding longer channel names
- Callback queries not arriving at the bot
- Error in Bot API response about callback_data length
- Buttons work for short channel names but fail for long ones

**Phase to address:**
Phase 3 (inline keyboard UI) — design the callback data encoding scheme upfront.

---

### Pitfall 8: Telegram Message Length and Formatting Limits

**What goes wrong:**
Telegram messages are limited to 4096 characters (including formatting entities). A daily digest of 20 channels with summaries easily exceeds this. Messages get truncated, or `sendMessage` returns an error. Also, mixing `parse_mode` (HTML vs Markdown) incorrectly causes parse errors that reject the entire message.

**Why it happens:**
4096 sounds like plenty until you're concatenating summaries from multiple channels with headers, separators, and formatting tags. The actual usable text is much less than 4096 chars when HTML tags are counted.

**How to avoid:**
- Always split long messages into multiple messages, each under 4096 chars
- Split at channel boundaries (natural break points in a digest)
- Count characters including HTML tags when checking length
- Use `parse_mode=HTML` consistently (more predictable than MarkdownV2 which requires extensive escaping)
- Build a message builder utility that handles splitting automatically
- Test with worst-case scenarios: 50 channels, all with long summaries

**Warning signs:**
- `message is too long` errors from Bot API
- Digests missing channels at the end (silently dropped)
- Messages that look fine in dev (few channels) but fail in prod (many channels)

**Phase to address:**
Phase 2 (notification system) — build the message splitter as part of the digest sender.

---

## Technical Debt Patterns

| Shortcut | Immediate Benefit | Long-term Cost | When Acceptable |
|----------|-------------------|----------------|-----------------|
| Store raw HTML of posts instead of parsed data | Faster initial development | Database bloat, no structured queries, can't search/filter | Never — parse upfront |
| Single cron interval for all channels | Simple scheduling | Inefficient scraping (inactive channels checked as often as active ones), rate limit risk | MVP only, replace with per-channel intervals |
| Store user preferences in JSON column | Flexible schema, easy to add fields | Can't query efficiently, can't enforce constraints, migration pain | Acceptable for v1 — migrate to proper columns when schema stabilizes |
| No retry on failed LLM calls | Simpler code | Silent summary gaps, users see "first 200 chars" when they wanted summaries | Never — at minimum, retry once with fallback |
| Hardcode channel presets in source | Quick to ship | Can't update without redeploy | MVP only — move to DB immediately after |
| Synchronous scraping (one channel at a time) | Simpler code | Scraping 50 channels sequentially at 3s delay = 2.5 minutes minimum per cycle | Never for >10 channels — use async from day one |

## Integration Gotchas

| Integration | Common Mistake | Correct Approach |
|-------------|----------------|------------------|
| t.me/s/* pagination | Not handling `?before=XX` parameter for historical posts | Use `?before=post_id` to paginate; track `last_post_id` per channel to avoid re-scraping old posts |
| t.me/s/* channel not found | Assuming all channels always return 200 | Handle 404 (channel deleted/renamed), handle empty page (channel has no posts), handle Cloudflare challenge pages |
| Telegram Bot API `sendMessage` | Sending MarkdownV2 without escaping special characters | Use `parse_mode=HTML` — fewer reserved characters, more predictable. Escape `<`, `>`, `&`, `"` only |
| Telegram Bot API `editMessageText` | Not handling "message not modified" error | Catch error code 400 with "message is not modified" — it's benign, not a real error |
| Telegram Bot API `answerCallbackQuery` | Not answering callback queries (button appears stuck) | Always call `answerCallbackQuery` even if no action needed — Telegram requires acknowledgment |
| PostgreSQL with asyncpg | Using synchronous psycopg2 with asyncio bot framework | Use `asyncpg` or `SQLAlchemy async` — mixing sync DB driver with asyncio blocks the event loop |
| LLM API | Sending full post text without truncation to LLM | Truncate post text to reasonable length (2000 chars) before sending to LLM — saves tokens and cost |
| APScheduler / cron | Running scraping and notification in same scheduler process | Separate scraping scheduler from notification scheduler — they have different failure modes and timing needs |

## Performance Traps

| Trap | Symptoms | Prevention | When It Breaks |
|------|----------|------------|----------------|
| Unindexed queries on posts table | Slow digest generation, timeout on notification sends | Index on `(channel_id, telegram_post_id)`, `(channel_id, posted_at)`, `(channel_id, posted_at DESC)` from day one | 10K+ posts (happens in weeks with active channels) |
| Fetching all posts then filtering in Python | High memory usage, slow response times | Filter in SQL with `WHERE` clauses and `LIMIT` — never load all posts into memory | 100K+ posts |
| No connection pooling for PostgreSQL | Connection exhaustion under load, "too many connections" errors | Use `asyncpg.Pool` with configurable min/max connections | 50+ concurrent operations |
| N+1 queries for user digest generation | Digest for 100 users takes minutes instead of seconds | Batch query all needed posts in one query, then distribute to user digests in code | 50+ users |
| Storing full post HTML in DB | Table bloat, slow backups, query degradation | Store only parsed text + metadata. If raw HTML needed for re-parsing, store in separate table or S3 | 100K+ posts |
| Polling t.me/s/* too frequently | IP block, wasted bandwidth, no new content | Adaptive polling: active channels every 5min, quiet channels every 30min, based on post frequency | 20+ channels from single IP |

## Security Mistakes

| Mistake | Risk | Prevention |
|---------|------|------------|
| Bot token in source code / config file in git | Token leaked → anyone controls your bot | Use environment variable, never commit. Add token to `.gitignore` patterns |
| No input validation on channel names from users | SQL injection or path traversal in scraping URL | Whitelist channel name format (`[a-zA-Z0-9_]{5,32}`) before using in DB queries or URL construction |
| Storing user data without retention policy | GDPR/legal risk, database bloat | Implement data retention: auto-delete inactive users and their preferences after N months |
| No authentication for admin commands | Anyone can trigger mass scraping or reset database | Verify user ID against admin whitelist before executing admin commands |
| Scraping through same IP as admin panel | Single IP block takes down both scraping and management | Use separate outgoing IPs or proxy for scraping vs. admin access |

## UX Pitfalls

| Pitfall | User Impact | Better Approach |
|---------|-------------|-----------------|
| Too many inline buttons per message | Overwhelming UI, users can't find what they need, Telegram renders poorly on mobile | Max 5 buttons per row, max 3-4 rows per message. Use pagination for channel lists. Use sub-menus. |
| No feedback when scraping fails | User adds channel but never sees posts — thinks bot is broken | Send confirmation: "Channel added. First posts will appear in your next digest." Show last scraped time in channel info. |
| Changing inline keyboard without context | User taps button on old message, gets unexpected result (stale state) | Include state version in callback_data; if stale, show "this menu is outdated, here's the current one" |
| Digest at fixed time regardless of timezone | User in UTC+3 gets digest at 3 AM | Store user timezone (ask on `/start`), schedule digests in user's local time |
| No way to pause/resume notifications | User goes on vacation, gets 200 unread digests, unsubscribes permanently | Add "Pause for N days" inline button. Auto-resume and notify user when pause ends. |
| Error messages in English for Russian-speaking users | Confusion, support burden | All user-facing messages in Russian from day one (per project scope). Error messages should be human-readable, not technical. |

## "Looks Done But Isn't" Checklist

- [ ] **Scraper:** Extracts posts but doesn't handle edited posts — verify by editing a known post and re-scraping
- [ ] **Scraper:** Handles channel name changes (user subscribed to @old_name, channel renames to @new_name) — verify dedup works across rename
- [ ] **Deduplication:** Works for identical posts (same text, same channel, different post IDs) — verify with channels that repost content
- [ ] **Rate limiting:** Handles 429 with exponential backoff, not just fixed delay — verify with aggressive scraping test
- [ ] **LLM summaries:** Falls back gracefully when LLM API is down — verify by disconnecting and checking user gets "first 200 chars" instead of error
- [ ] **Digest delivery:** Splits long messages correctly at channel boundaries — verify with 30+ channel subscription
- [ ] **Inline keyboard:** callback_data stays under 64 bytes with worst-case channel names — verify with 32-char channel username
- [ ] **Cron jobs:** Handles missed executions (server was down during scheduled time) — verify by stopping server, restarting after scheduled time, checking if job runs
- [ ] **PostgreSQL:** Has indexes before loading real data — verify `EXPLAIN ANALYZE` on digest query with 100K+ rows
- [ ] **User registration:** Handles `/start` with deep link parameters (for future referral system) — verify URL-encoded params don't break
- [ ] **Bot API errors:** Handles "bot was blocked by user" gracefully (stop sending, mark inactive) — verify with blocked user test
- [ ] **Timezone:** Digest scheduled at "9:00 AM" means user's 9:00 AM, not server's — verify with user in different timezone than server

## Recovery Strategies

| Pitfall | Recovery Cost | Recovery Steps |
|---------|---------------|----------------|
| Broken HTML selectors | LOW | Fix selectors in config file, redeploy. No data loss if raw HTML was stored. |
| IP blocked by Telegram | MEDIUM | Wait (usually hours), reduce scrape frequency, or switch IP/proxy. No data loss (just delayed). |
| Duplicate posts in DB | MEDIUM | Run deduplication migration: keep earliest, delete duplicates. Add unique constraint to prevent recurrence. |
| LLM API key exhausted/revoked | LOW | Switch to fallback API key or provider. Existing summaries still in DB. New summaries use "first 200 chars" until key restored. |
| Bot token compromised | HIGH | Revoke via @BotFather immediately. Generate new token. Update all instances. All webhook URLs invalidated. |
| PostgreSQL data corruption | HIGH | Restore from backup. Requires regular pg_dump cron. Verify backup restore procedure before you need it. |
| Missed cron jobs (server downtime) | LOW | Track `last_successful_scrape` per channel. On restart, scrape from last known gap. No data loss if gap is small. |

## Pitfall-to-Phase Mapping

| Pitfall | Prevention Phase | Verification |
|---------|------------------|--------------|
| HTML selector fragility | Phase 1: Scraper | Integration test: parse live channel, verify structured output |
| Rate limiting / IP block | Phase 1: Scraper | Load test: scrape 50 channels in sequence, verify no 429s |
| View count parsing | Phase 1: Scraper | Unit tests covering "1.2K", "4.22M", "991K", "1,234", "42" formats |
| Deduplication | Phase 1: Scraper | Scrape same channel twice, verify 0 new inserts on second pass |
| LLM cost explosion | Phase 2: LLM Integration | Cost tracking dashboard: verify per-post cost, not per-user |
| Bot API broadcast limits | Phase 2: Notifications | Load test: send to 100 mock users, verify no 429s |
| Message length limits | Phase 2: Notifications | Test with 50-channel digest, verify split into multiple messages |
| callback_data 64-byte limit | Phase 3: Inline Keyboard | Test all button types with 32-char channel usernames |
| Timezone handling | Phase 3: User Settings | Verify digest time differs for users in UTC+3 vs UTC+0 |
| PostgreSQL indexes | Phase 1: Scraper | `EXPLAIN ANALYZE` on all queries after 100K test rows |
| Pause/resume UX | Phase 3: User Settings | User pauses, verifies no messages, auto-resumes and gets notification |

## Sources

- Telegram Bot API official documentation (https://core.telegram.org/bots/api) — Bot API 9.5, March 2026
- Telegram Bots FAQ (https://core.telegram.org/bots/faq) — Rate limits: 1 msg/sec per chat, 30 msg/sec broadcast
- Live t.me/s/* pages examined: t.me/s/durov, t.me/s/durov?before=43 — HTML structure, pagination, view count formats
- PostgreSQL indexing best practices for time-series-like data patterns
- Telegram Bot API changelog (Bot API 9.2-9.5) — recent changes tracked for compatibility

---
*Pitfalls research for: Telegram Channel Monitor Bot*
*Researched: 2026-04-02*
