# Архитектура Channel Scanner

Channel Scanner запускается как один Python-процесс: Telegram bot polling, APScheduler jobs, scraper, LLM-суммаризация и доставка дайджестов работают в одном runtime.

```mermaid
flowchart TD
    user[Telegram user] --> bot[aiogram bot runtime]
    bot --> service[Bot service]
    service --> db[(PostgreSQL)]

    scheduler[APScheduler] --> scrape[Scraping job]
    scheduler --> digest[Digest delivery job]
    scheduler --> modelRefresh[OpenRouter model refresh]

    scrape --> telegramPages[Public t.me/s pages]
    telegramPages --> parser[HTML parser]
    parser --> db

    digest --> db
    digest --> llm[OpenRouter-compatible LLM]
    digest --> memory[mem0 local memory]
    digest --> sender[Telegram Bot API sender]
    sender --> user

    bot --> assistant[Natural-language assistant]
    assistant --> tools[User-scoped product tools]
    assistant --> llm
    assistant --> memory
    tools --> db

    config[config.toml + env vars] --> bot
    config --> scheduler
    config --> digest
```

## Ключевые решения

- Один процесс упрощает локальный запуск и portfolio deployment: не нужен отдельный worker для планировщика.
- PostgreSQL остается production-хранилищем, а тесты используют in-memory SQLite для быстрой интеграционной проверки.
- Scraper читает публичные страницы `t.me/s/*`, поэтому для чтения каналов не нужен Telegram Client API.
- Доставка дайджестов дедуплицируется по паре `subscription + post`, а не только по пользователю.
- LLM-суммаризация опциональна: при ошибках модели доставка откатывается к короткому режиму.
- `.data/` используется только для локального runtime-state и исключена из git и Docker build context.
