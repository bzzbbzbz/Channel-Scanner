# Architecture Research

**Domain:** Telegram Channel Monitor Bot
**Researched:** 2026-04-02
**Confidence:** HIGH

## Standard Architecture

### System Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    Presentation Layer                        │
│  ┌──────────────┐  ┌──────────────────┐  ┌───────────────┐  │
│  │ Bot Handlers  │  │ Callback Handlers │  │  Inline UI    │  │
│  │ /start /help  │  │ subscriptions     │  │  keyboards    │  │
│  │ /subscribe    │  │ settings          │  │               │  │
│  └──────┬───────┘  └────────┬─────────┘  └───────┬───────┘  │
├─────────┴───────────────────┴─────────────────────┴──────────┤
│                     Service Layer                            │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌───────────────┐   │
│  │ Channel  │ │ User     │ │Summary   │ │ Notification  │   │
│  │ Service  │ │ Service  │ │ Service  │ │ Service       │   │
│  └────┬─────┘ └────┬─────┘ └────┬─────┘ └──────┬────────┘   │
├───────┴────────────┴───────────┴───────────────┴─────────────┤
│                    Ingestion Layer                            │
│  ┌──────────────┐  ┌──────────────────┐  ┌───────────────┐  │
│  │ Scraper      │  │ Scheduler        │  │ LLM Client    │  │
│  │ t.me/s/*     │  │ APScheduler      │  │ OpenAI/Local  │  │
│  └──────┬───────┘  └────────┬─────────┘  └───────┬───────┘  │
├─────────┴───────────────────┴─────────────────────┴──────────┤
│                     Data Layer                               │
│  ┌──────────┐  ┌───────────┐  ┌───────────┐  ┌───────────┐  │
│  │ posts    │  │ channels  │  │ users     │  │ subscrip- │  │
│  │          │  │           │  │           │  │ tions     │  │
│  └──────────┘  └───────────┘  └───────────┘  └───────────┘  │
│                     PostgreSQL                               │
└─────────────────────────────────────────────────────────────┘
```

### Component Responsibilities

| Component | Responsibility | Typical Implementation |
|-----------|----------------|------------------------|
| **Bot Handlers** | Process user commands (/start, /help, /subscribe) | aiogram Router + message handlers |
| **Callback Handlers** | Handle inline button presses (subscribe, unsubscribe, settings) | aiogram CallbackQuery handlers |
| **Inline UI** | Build inline keyboard markup for subscription management | aiogram InlineKeyboardBuilder |
| **Channel Service** | CRUD for monitored channels, preset collections | Service class with repository pattern |
| **User Service** | User registration, preferences, notification settings | Service class with repository pattern |
| **Summary Service** | Orchestrate post summarization (truncate / LLM summary / full) | Service class delegating to LLM or simple truncation |
| **Notification Service** | Build digest messages, dispatch to users per their schedule | Service class querying posts + formatting |
| **Scraper** | Fetch and parse t.me/s/* pages, extract post data | httpx/BeautifulSoup async scraper |
| **Scheduler** | Periodic scraping jobs, hourly/daily digest dispatch | APScheduler AsyncScheduler with interval triggers |
| **LLM Client** | Call OpenAI-compatible API for post summarization | httpx async client or openai SDK |
| **PostgreSQL** | Persistent storage for posts, channels, users, subscriptions | asyncpg or SQLAlchemy async |

## Recommended Project Structure

```
src/
├── bot/                    # Telegram bot presentation layer
│   ├── __init__.py
│   ├── main.py             # Bot entry point, Dispatcher setup
│   ├── routers/            # aiogram Router modules
│   │   ├── __init__.py
│   │   ├── start.py        # /start, /help handlers
│   │   ├── subscribe.py    # /subscribe, channel management
│   │   ├── settings.py     # notification frequency, format settings
│   │   └── presets.py      # preset collection browsing
│   ├── keyboards/          # Inline keyboard builders
│   │   ├── __init__.py
│   │   ├── subscribe.py    # subscription management keyboards
│   │   └── settings.py     # settings keyboards
│   └── middlewares/        # aiogram middlewares (db session, user auth)
│       └── __init__.py
├── scraper/                # t.me/s/* parsing engine
│   ├── __init__.py
│   ├── client.py           # async HTTP client for t.me/s/*
│   ├── parser.py           # BeautifulSoup HTML parsing logic
│   └── models.py           # parsed post dataclasses
├── scheduler/              # Job scheduling
│   ├── __init__.py
│   ├── setup.py            # APScheduler config, register jobs
│   └── jobs/               # individual job implementations
│       ├── __init__.py
│       ├── scrape.py       # periodic channel scraping job
│       └── digest.py       # hourly/daily digest dispatch job
├── services/               # business logic layer
│   ├── __init__.py
│   ├── channel.py          # channel management logic
│   ├── subscription.py     # subscription management
│   ├── summary.py          # summary generation (LLM + truncation)
│   └── notification.py     # notification dispatch logic
├── db/                     # data access layer
│   ├── __init__.py
│   ├── connection.py       # asyncpg pool setup
│   ├── models.py           # SQLAlchemy models or table definitions
│   └── repositories/       # data access classes
│       ├── __init__.py
│       ├── posts.py
│       ├── channels.py
│       ├── users.py
│       └── subscriptions.py
├── llm/                    # LLM integration
│   ├── __init__.py
│   └── client.py           # OpenAI-compatible API client
├── config.py               # settings via pydantic-settings
└── main.py                 # application entry point
```

### Structure Rationale

- **bot/:** Isolates aiogram-specific code (handlers, keyboards, middlewares). aiogram Router pattern naturally maps to separate files per concern.
- **scraper/:** Self-contained parsing engine. Decoupled from bot so it can run independently or be tested without Telegram.
- **scheduler/:** APScheduler jobs in separate module. Each job is a thin orchestrator calling services.
- **services/:** Pure business logic. No framework dependencies. Called by both bot handlers and scheduler jobs.
- **db/:** Data layer with repository pattern. Swap asyncpg for SQLAlchemy without touching services.
- **llm/:** LLM abstraction. Swap OpenAI for local model without changing summary service.

## Architectural Patterns

### Pattern 1: Single-Process Async Monolith

**What:** One asyncio event loop runs the bot, scheduler, and scraper concurrently.
**When to use:** V1 with <10K users. Avoids distributed system complexity.
**Trade-offs:** Simple deployment (one process) vs. can't scale components independently.

**Example:**
```python
async def main():
    db_pool = await create_db_pool(config.database_url)
    scheduler = AsyncScheduler()
    bot = Bot(token=config.bot_token)
    dp = Dispatcher()

    register_routers(dp, db_pool)
    register_jobs(scheduler, db_pool)

    async with scheduler:
        await scheduler.start_in_background()
        await dp.start_polling(bot)
```

### Pattern 2: Repository Pattern for Data Access

**What:** Each database table gets a repository class. Services depend on repository interfaces, not raw SQL.
**When to use:** Always. Keeps business logic decoupled from database details.
**Trade-offs:** More boilerplate vs. easy to swap DB layer and test services with mocks.

**Example:**
```python
class PostRepository:
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def get_unsent_posts(self, channel_id: int, since: datetime) -> list[Post]:
        rows = await self._pool.fetch(
            "SELECT * FROM posts WHERE channel_id = $1 AND created_at > $2 ORDER BY created_at",
            channel_id, since,
        )
        return [Post(**dict(r)) for r in rows]

    async def upsert_posts(self, posts: list[Post]) -> int:
        ...
```

### Pattern 3: Service Layer Orchestration

**What:** Services encapsulate business logic. Handlers and jobs call services, never repositories directly.
**When to use:** Always. Prevents logic duplication between bot handlers and scheduler jobs.
**Trade-offs:** More indirection vs. single source of truth for business rules.

**Example:**
```python
class NotificationService:
    def __init__(self, post_repo: PostRepository, user_repo: UserRepository, llm: LLMClient, bot: Bot):
        self._posts = post_repo
        self._users = user_repo
        self._llm = llm
        self._bot = bot

    async def send_hourly_digest(self):
        subscriptions = await self._users.get_subscriptions_with_frequency("hourly")
        for sub in subscriptions:
            posts = await self._posts.get_recent(sub.channel_id, hours=1)
            if not posts:
                continue
            digest = await self._format_digest(posts, sub.summary_format)
            await self._bot.send_message(sub.user_id, digest)
```

### Pattern 4: Scraper as Isolated Module with Rate Limiting

**What:** Scraper is a self-contained async class with built-in rate limiting and retry logic.
**When to use:** Web scraping always needs rate limiting. Isolating it makes it testable.
**Trade-offs:** Slightly more abstraction vs. prevents IP bans and makes scraping testable.

## Data Flow

### Scrape Flow (scheduled periodically)

```
APScheduler (interval trigger)
    ↓
ScrapeJob.run()
    ↓
Scraper.fetch_channel(channel_slug)
    ↓ (HTTP GET t.me/s/channel_slug)
    ↓ (parse HTML with BeautifulSoup)
    ↓
Scraper.parse_posts(html)
    ↓ [list of ParsedPost]
PostRepository.upsert_posts(posts)
    ↓ (INSERT ON CONFLICT DO UPDATE)
PostgreSQL
```

### Digest Dispatch Flow (scheduled hourly/daily)

```
APScheduler (cron/interval trigger)
    ↓
DigestJob.run()
    ↓
NotificationService.send_hourly_digest()  /  send_daily_digest()
    ↓
SubscriptionRepository.get_by_frequency("hourly")
    ↓
for each subscription:
    PostRepository.get_unsent_posts(channel_id, since)
    ↓
    SummaryService.format(posts, format_type)
        ↓ if "brief_summary": LLMClient.summarize(posts)
        ↓ if "first_200": truncate each post text
        ↓ if "full": join full text
    ↓
    Bot.send_message(user_id, formatted_digest)
    ↓
    mark posts as sent for this user
```

### User Interaction Flow

```
User sends /start
    ↓
Dispatcher → StartRouter.handle_start()
    ↓
UserRepository.upsert(user_id, username)
    ↓
Bot sends welcome message with inline keyboard

User presses "Subscribe to AI channels"
    ↓
Dispatcher → SubscribeRouter.handle_callback("preset:ai")
    ↓
ChannelService.get_preset_channels("ai")
    ↓
SubscriptionRepository.subscribe(user_id, channel_ids)
    ↓
Bot sends confirmation with channel list

User presses "Settings" → "Hourly digest"
    ↓
Dispatcher → SettingsRouter.handle_callback("freq:hourly")
    ↓
UserRepository.update_frequency(user_id, "hourly")
    ↓
Bot confirms setting changed
```

### Key Data Flows

1. **Ingestion:** Scheduler → Scraper → Parser → PostgreSQL (push, periodic)
2. **Summarization:** NotificationService → LLM Client → external API → formatted text (pull, on-demand per digest)
3. **Notification:** Scheduler → NotificationService → query posts → format → Bot.send_message (push, periodic)
4. **Subscription management:** User → Bot handler → Service → Repository → PostgreSQL (interactive, on-demand)

## Scaling Considerations

| Scale | Architecture Adjustments |
|-------|--------------------------|
| 0-1K users | Single-process monolith. All components in one asyncio loop. |
| 1K-10K users | Separate scraper worker process. Add Redis for deduplication cache. Batch LLM calls. |
| 10K+ users | Multiple scraper workers with task queue (Celery/RQ). Separate notification dispatch service. |

### Scaling Priorities

1. **First bottleneck:** t.me rate limiting. Solution: stagger scraping jobs, add delay between requests, rotate user agents.
2. **Second bottleneck:** LLM API costs/latency. Solution: cache summaries, batch summarize, use cheaper models for brief summaries.
3. **Third bottleneck:** Telegram Bot API message rate limits (30 msg/sec). Solution: queue notifications with per-user rate limiting.

## Anti-Patterns

### Anti-Pattern 1: Scraping Inside Bot Handlers

**What people do:** Call scraper synchronously when user subscribes to a new channel.
**Why it's wrong:** Blocks the bot event loop. User waits for HTTP request + parse. Causes timeout.
**Do this instead:** Scheduler handles all scraping on interval. New subscriptions see data after next scrape cycle. Optionally trigger one immediate scrape via scheduler job.

### Anti-Pattern 2: Storing Full HTML Instead of Extracted Data

**What people do:** Save raw HTML from t.me/s/* pages to "parse later".
**Why it's wrong:** Wastes storage, HTML structure changes break deferred parsing, no clear schema.
**Do this instead:** Extract structured data (text, views, date, reactions) immediately during scraping. Store normalized data in typed columns.

### Anti-Pattern 3: One LLM Call Per Post Per User

**What people do:** Generate unique summary for each user for each post.
**Why it's wrong:** N users × M posts = N×M API calls. Extremely expensive and slow.
**Do this instead:** Summarize once per post (or once per batch of posts from same channel). Cache the summary. All users sharing that channel get the same summary.

### Anti-Pattern 4: Polling-Based Bot Without Webhook Consideration

**What people do:** Use long-polling without considering migration path.
**Why it's wrong:** Long-polling is fine for V1 but needs planning for multi-instance deployment.
**Do this instead:** Abstract bot update source. Start with polling, but keep webhook migration path clear.

## Integration Points

### External Services

| Service | Integration Pattern | Notes |
|---------|---------------------|-------|
| t.me/s/* | async HTTP GET (httpx) | Rate limit: ~1 req/sec. Randomize user-agent. Handle 429 with exponential backoff. |
| Telegram Bot API | aiogram (long-polling) | 30 msg/sec global limit. Use parse_mode=HTML. Inline keyboards for all interactions. |
| LLM API (OpenAI) | httpx async or openai SDK | Cache summaries. Set max_tokens. Handle timeouts gracefully — fall back to truncation. |
| PostgreSQL | asyncpg connection pool | Use connection pool (min=2, max=10). Migration with Alembic. |

### Internal Boundaries

| Boundary | Communication | Notes |
|----------|---------------|-------|
| Bot Handlers ↔ Services | Direct function calls | Services injected via middleware or explicit init |
| Services ↔ Repositories | Direct function calls | Repos injected via constructor |
| Scheduler Jobs ↔ Services | Direct function calls | Jobs are thin wrappers calling service methods |
| Scraper ↔ Parser | Direct function calls | Scraper fetches HTML, Parser extracts data |
| Services ↔ LLM Client | Async function calls | LLM client is stateless, injected into services |

### Dependency Injection Flow

```
main.py creates:
    db_pool → injected into all repositories
    repositories → injected into all services
    Bot instance → injected into NotificationService
    LLM Client → injected into SummaryService
    Services → injected into bot handlers (via middleware) and scheduler jobs
```

## Build Order (Component Dependencies)

```
Phase 1: Data Layer + Scraper (no dependencies)
    ├── db/models.py          (table schemas)
    ├── db/connection.py      (pool setup)
    ├── db/repositories/      (CRUD operations)
    ├── scraper/client.py     (HTTP client)
    ├── scraper/parser.py     (HTML parsing)
    └── config.py             (settings)

Phase 2: Core Services (depends on Phase 1)
    ├── services/channel.py
    ├── services/subscription.py
    └── services/notification.py (without LLM, truncation only)

Phase 3: Bot (depends on Phase 2)
    ├── bot/main.py           (Dispatcher setup)
    ├── bot/routers/          (all handlers)
    ├── bot/keyboards/        (inline keyboards)
    └── bot/middlewares/      (DI middleware)

Phase 4: Scheduler (depends on Phase 2)
    ├── scheduler/setup.py    (APScheduler config)
    └── scheduler/jobs/       (scrape + digest jobs)

Phase 5: LLM Integration (depends on Phase 2)
    ├── llm/client.py         (API client)
    └── services/summary.py   (LLM + fallback logic)

Phase 6: Polish (depends on all above)
    ├── Preset channel collections
    ├── Hot post detection (views/likes threshold)
    └── Keyword/topic filtering
```

## Sources

- aiogram 3.x documentation (https://docs.aiogram.dev/en/latest/) — Router/Dispatcher patterns, inline keyboards
- APScheduler 4.x documentation (https://apscheduler.readthedocs.io/en/master/) — AsyncScheduler, interval/cron triggers, PostgreSQL data store
- asyncpg documentation — connection pooling patterns
- PROJECT.md context — t.me/s/* scraper prototype, Python + PostgreSQL constraints

---
*Architecture research for: Telegram Channel Monitor Bot*
*Researched: 2026-04-02*
