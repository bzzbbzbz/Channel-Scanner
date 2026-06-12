# Progress Log

## 2026-04-26 - v1 baseline slice without LLM

Completed in this slice:

- fixed container startup path: Docker now starts the real application entrypoint instead of a non-existent `src.scheduler` module
- added container entrypoint logic to run `alembic upgrade head` before app startup
- fixed Docker build order so the package can actually be installed during image build
- added the initial Alembic migration for the current foundation models
- fixed PostgreSQL-specific post upsert to use the PostgreSQL dialect `insert()` required for `ON CONFLICT DO NOTHING`
- fixed PostgreSQL enum compatibility for `Channel.status` so fresh schema creation does not fail on invalid enum defaults
- updated baseline docs: root `README.md`, `.env.example`
- preserved the existing scraper/scheduler structure without expanding into bot UX scope

Validation run for this slice is recorded in the final task report.

## 2026-04-26 - bot v1 polling slice

Completed in this slice:

- added Telegram bot polling runtime inside the existing single-process app
- wired bot startup into `src.main` alongside the existing scheduler
- added startup-time bot command registration from code
- added persisted user domain: Telegram identity, digest format, frequency, timezone
- added channel subscription management with add/list/remove flows
- added inline button and callback handling for settings and subscriptions
- added database models, repositories, and Alembic migration for users and subscriptions
- updated channel persistence to support bot-created public-username channels
- added unit/integration coverage for bot domain logic and input normalization
- updated root docs and `.env.example` for the bot runtime

Deferred in this slice:

- actual digest generation and delivery
- advanced filters and hot-post logic

## 2026-04-26 - digest delivery v1

Completed in this slice:

- added persisted digest delivery state with per-user post deduplication
- added digest selection from subscribed channels with hourly/daily timezone-aware due checks
- added `short` and `full` digest formatting with empty-text fallback and Telegram-safe batching
- added scheduled digest delivery through the Telegram Bot API inside the existing single-process runtime
- added delivery-state persistence only after successful send completion for the included posts
- added Alembic migration, digest-focused tests, and updated docs

Deferred in this slice:

- LLM summaries
- hot-post ranking
- advanced filters

## 2026-04-27 - UX, i18n, and subscription baseline fixes

Completed in this slice:

- switched the bot home UX from command-menu-first to a compact reply keyboard plus inline context actions
- translated the main buttons and flows to Russian by default and added persisted `ru` / `en` language selection with Telegram `language_code` auto-detection
- replaced single-channel subscribe flow with bulk add and bulk remove flows using one-message channel lists
- added channel list normalization for `@channel`, `https://t.me/channel`, `t.me/channel`, and bare usernames, with deduplication and partial-success summaries
- improved timezone UX with quick-select buttons and preserved IANA support while adding manual UTC offset input like `+5` and `UTC-3`
- fixed digest delivery of historical posts after a new subscription by persisting a subscription baseline timestamp and filtering pending posts against it
- updated digest rendering to Telegram-safe HTML with clickable post headers and working converted links in message bodies
- added Alembic migration plus unit/integration coverage for the new timezone parsing, digest links, language persistence, and subscription baseline behavior

Deferred in this slice:

- LLM summaries
- hot-post ranking
- advanced filters

## 2026-04-27 - summary digests and navigation polish

Completed in this slice:

- replaced the old full-text digest option with `200 символов` and `Пересказ`
- added summary submodes: brief, detailed, and custom prompt with persisted user settings
- integrated an OpenRouter-compatible client with the approved fallback chain and a guaranteed `200 символов` fallback on all model failures
- started saving delivered summary metadata per user/post: rendered summary text, effective mode, model name, and custom prompt snapshot when used
- updated settings/subscriptions navigation to favor reply keyboard on home and inline keyboards for contextual actions
- added language flags, `✅` active markers, and current custom prompt preview in triple backticks
- made manual timezone, bulk add, bulk remove, and custom prompt flows best-effort delete prompt and user messages before returning to the related screen
- updated tests and docs for the summary flow, OpenRouter env vars, and the new migration

## 2026-04-27 - keyboard-driven navigation refinement

Completed in this slice:

- traced the remaining UX gap to Telegram reply-keyboard behavior: pressing `Настройки` / `Подписки` sends a normal user message, so the bot was answering with a fresh message instead of reusing the existing inline screen
- changed section entry and command-based opening to reuse one tracked inline screen message when possible, while keeping the compact home reply keyboard as the top-level launcher
- reduced chat noise by best-effort deleting the home button tap message and by returning bulk add/remove, timezone, and custom prompt flows back into the same edited screen instead of sending an extra screen message
- documented the Telegram platform constraint: nested, in-place navigation is not achievable with reply keyboard alone; the closest practical UX is reply keyboard for home plus inline keyboard message editing for section screens

## 2026-04-27 - named subscriptions and screen cleanup

Completed in this slice:

- replaced the old flat `user -> channels` model with `user -> named subscriptions -> channels`
- added per-subscription settings for digest format, summary mode, custom prompt, frequency, enabled state, rename, and delete
- updated digest delivery and persisted delivery state to operate per subscription instead of per user
- added a backfill Alembic migration that converts existing channel memberships into one default named subscription per user
- simplified top-level settings to timezone and language only
- changed inline navigation so `Закрыть` deletes the current screen, timezone picker uses `Назад`, and close actions no longer reopen language/home screens
- refreshed reply keyboard labels with emoji and kept add/remove channel flows on the original edited subscription screen
- updated integration and unit coverage for named subscriptions and per-subscription digest delivery

## 2026-04-27 - callback routing and summary HTML hardening

Completed in this slice:

- fixed subscription callback routing so generic `subscription:frequency|format|summary:*` screens no longer swallow the more specific `...:set:...` actions
- added regression coverage for all subscription frequency, digest-format, and summary-mode callback flows
- rebuilt LLM summary prompts into explicit `<task>`, `<instructions>`, and `<text>` sections
- updated built-in brief/detailed/custom summary instructions to target Telegram-safe HTML with stable links, plain-text bullets, and normal newline rendering
- preserved existing summary post-processing while expanding it to keep a small safe HTML subset and escape unsupported tags
