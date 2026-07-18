"""Minimal localized bot text catalog."""

from __future__ import annotations

from pathlib import Path


_CONTENT_DIR = Path(__file__).with_name("content")


def _load_content(filename: str) -> str:
    return (_CONTENT_DIR / filename).read_text(encoding="utf-8").strip()

TEXTS = {
    "ru": {
        "app_title": "Channel Scanner",
        "home_hint": _load_content("start_ru.md"),
        "home_short": "Выберите раздел.",
        "private_only": "Бот работает только в личных сообщениях.",
        "private_only_alert": "Только личный чат",
        "cancelled": "Текущий ввод отменен.",
        "help": (
            "Команды\n\n"
            "/start - открыть бот\n"
            "/help - показать помощь\n"
            "/settings - изменить настройки\n"
            "/subscriptions - управлять подписками\n"
            "/cancel - отменить текущий ввод\n\n"
            "Ограничения v1: только личные чаты, только публичные каналы и только дайджесты по расписанию."
        ),
        "button_settings": "⚙️ Настройки",
        "button_subscriptions": "📚 Подписки",
        "button_close": "Закрыть",
        "button_back": "Назад",
        "button_add_channels": "➕ Добавить каналы",
        "button_remove_channels": "➖ Удалить каналы",
        "button_create_subscription": "➕ Создать подписку",
        "button_add_from_presets": "➕ Добавить из существующих",
        "button_confirm_preset": "Создать подписку",
        "button_rename_subscription": "✏️ Переименовать",
        "button_delete_subscription": "🗑 Удалить подписку",
        "button_open_subscription": "Открыть",
        "button_toggle_on": "🔕 Выключить",
        "button_toggle_off": "🔔 Включить",
        "button_frequency": "⏱ Частота уведомлений",
        "button_digest_format": "📝 Промпты",
        "button_processing_log": "📊 Обработка за 24 часа",
        "button_timezone": "Часовой пояс",
        "button_language": "Язык",
        "button_digest_200": "200 символов",
        "button_digest_summary": "Пересказ",
        "button_summary_brief": "Кратко",
        "button_summary_detailed": "Подробно",
        "button_summary_custom": "Свой вариант",
        "button_edit_filter_prompt": "Изменить фильтр",
        "button_edit_summary_prompt": "Изменить пересказ",
        "button_prompts_default": "По умолчанию",
        "button_timezone_manual": "Ввести вручную",
        "button_language_ru": "🇷🇺 Русский",
        "button_language_en": "🇬🇧 English",
        "digest_updated": "Формат дайджеста обновлен",
        "summary_mode_updated": "Режим пересказа обновлен",
        "custom_prompt_updated": "Свой prompt сохранен",
        "prompts_reset": "Промпты возвращены к значениям по умолчанию",
        "frequency_updated": "Частота обновлена",
        "language_updated": "Язык обновлен",
        "timezone_updated": "Часовой пояс обновлен",
        "subscription_created": "Подписка создана",
        "subscription_updated": "Подписка обновлена",
        "subscription_deleted": "Подписка удалена",
        "timezone_prompt": (
            "Выберите часовой пояс кнопкой ниже или отправьте его текстом.\n\n"
            "Поддерживаются быстрые варианты `UTC+2` ... `UTC+6`, а также ручной ввод вроде `Europe/Berlin`, `UTC+3`, `-5`."
        ),
        "create_subscription_prompt": "Отправьте название новой подписки одним сообщением.",
        "rename_subscription_prompt": "Отправьте новое название подписки одним сообщением.",
        "add_channels_prompt": (
            "Отправьте список каналов одним сообщением.\n\n"
            "Поддерживаются форматы: `@channel`, `https://t.me/channel`, `t.me/channel`, `channel`.\n"
            "Разделители: запятая или новая строка."
        ),
        "remove_channels_prompt": (
            "Текущие подписки:\n\n{subscriptions}\n\n"
            "Отправьте список каналов для удаления одним сообщением.\n"
            "Поддерживаются те же форматы и разделители, что и при добавлении."
        ),
        "language_prompt": "Выберите язык.",
        "custom_prompt_prompt": (
            "Отправьте свой prompt одним сообщением.\n\n"
            "Бот будет передавать этот prompt как инструкцию и отдельно добавлять текст поста."
        ),
        "filter_prompt_prompt": "Скопируйте промпт для AI-фильтра и пришлите отредактированный вариант.",
        "summary_prompt_prompt": "Скопируйте промпт для AI-пересказа и пришлите отредактированный вариант.",
        "bulk_add_result": "Добавление каналов завершено.",
        "bulk_remove_result": "Удаление каналов завершено.",
        "preset_list_title": "Готовые наборы каналов\n\nВыберите пресет, чтобы создать новую подписку.",
        "preset_confirm": "Создать подписку из пресета «{name}»?\n\nКаналы:\n{channels}",
        "preset_create_result": "Подписка из пресета создана.",
        "preset_no_channels": "Не удалось создать подписку: ни один канал пресета недоступен.",
        "preset_unknown": "Неизвестный пресет.",
        "limit_subscriptions": "Лимит: можно создать не больше {limit} подписок.",
        "limit_channels": "Лимит: в одной подписке можно хранить не больше {limit} каналов.",
        "limit_assistant_tools": "Достигнут лимит действий ассистента за один запрос: {limit}. Уточните задачу или разбейте ее на несколько сообщений.",
        "result_added": "Добавлены: {items}",
        "result_already": "Уже были в подписках: {items}",
        "result_removed": "Удалены: {items}",
        "result_not_subscribed": "Не были в подписках: {items}",
        "result_not_found": "Не найдены или недоступны: {items}",
        "result_invalid": "Некорректный ввод: {items}",
        "result_limit_exceeded": "Не добавлены из-за лимита: {items}",
        "result_nothing": "Ничего не изменилось.",
    },
    "en": {
        "app_title": "Channel Scanner",
        "home_hint": (
            "Hi! I help you follow Telegram channels and receive convenient digests from them.\n\n"
            "What I can do:\n"
            "- collect new posts from selected channels;\n"
            "- group channels into separate topic-based subscriptions;\n"
            "- send digests for your subscriptions;\n"
            "- summarize posts so you can understand the main point faster;\n"
            "- help manage subscriptions and channels.\n\n"
            "How the digest works:\n"
            "1. On schedule, I check the channels you selected.\n"
            "2. I collect new posts since the previous digest.\n"
            "3. The AI filter removes ads, noise, and irrelevant posts.\n"
            "4. The AI summary turns what remains into short notes with source links.\n\n"
            "You can control every step: schedule, channel list, AI filter prompt, and AI summary prompt.\n\n"
            "The main advantage: you do not have to remember exact commands. You can write in natural language, for example:\n\n"
            "\"Add @example to the News subscription\"\n"
            "\"Show my subscriptions\"\n"
            "\"Remove this channel from the subscription\"\n"
            "\"What can you do?\"\n"
            "\"How do I use the bot?\"\n\n"
            "I will try to understand what you need and either do it or suggest the next step."
        ),
        "home_short": "Choose a section.",
        "private_only": "The bot works only in private chats.",
        "private_only_alert": "Private chat only",
        "cancelled": "Current input cancelled.",
        "help": (
            "Commands\n\n"
            "/start - open the bot\n"
            "/help - show help\n"
            "/settings - change settings\n"
            "/subscriptions - manage subscriptions\n"
            "/cancel - cancel current input\n\n"
            "v1 limits: private chats only, public channels only, scheduled digests only."
        ),
        "button_settings": "⚙️ Settings",
        "button_subscriptions": "📚 Subscriptions",
        "button_close": "Close",
        "button_back": "Back",
        "button_add_channels": "➕ Add channels",
        "button_remove_channels": "➖ Remove channels",
        "button_create_subscription": "➕ Create subscription",
        "button_add_from_presets": "➕ Add from existing",
        "button_confirm_preset": "Create subscription",
        "button_rename_subscription": "✏️ Rename",
        "button_delete_subscription": "🗑 Delete subscription",
        "button_open_subscription": "Open",
        "button_toggle_on": "🔕 Disable",
        "button_toggle_off": "🔔 Enable",
        "button_frequency": "⏱ Frequency",
        "button_digest_format": "📝 Prompts",
        "button_processing_log": "📊 Processing: 24 hours",
        "button_timezone": "Timezone",
        "button_language": "Language",
        "button_digest_200": "200 chars",
        "button_digest_summary": "Summary",
        "button_summary_brief": "Brief",
        "button_summary_detailed": "Detailed",
        "button_summary_custom": "Custom",
        "button_edit_filter_prompt": "Edit filter",
        "button_edit_summary_prompt": "Edit summary",
        "button_prompts_default": "Default",
        "button_timezone_manual": "Enter manually",
        "button_language_ru": "🇷🇺 Русский",
        "button_language_en": "🇬🇧 English",
        "digest_updated": "Digest format updated",
        "summary_mode_updated": "Summary mode updated",
        "custom_prompt_updated": "Custom prompt saved",
        "prompts_reset": "Prompts restored to defaults",
        "frequency_updated": "Frequency updated",
        "language_updated": "Language updated",
        "timezone_updated": "Timezone updated",
        "subscription_created": "Subscription created",
        "subscription_updated": "Subscription updated",
        "subscription_deleted": "Subscription deleted",
        "timezone_prompt": (
            "Choose a timezone below or send it as text.\n\n"
            "Quick picks: `UTC+2` ... `UTC+6`. Manual input still supports values like `Europe/Berlin`, `UTC+3`, `-5`."
        ),
        "create_subscription_prompt": "Send the new subscription name in one message.",
        "rename_subscription_prompt": "Send the new subscription name in one message.",
        "add_channels_prompt": (
            "Send a channel list in one message.\n\n"
            "Supported formats: `@channel`, `https://t.me/channel`, `t.me/channel`, `channel`.\n"
            "Separators: comma or newline."
        ),
        "remove_channels_prompt": (
            "Current subscriptions:\n\n{subscriptions}\n\n"
            "Send the channel list to remove in one message.\n"
            "The same formats and separators are supported."
        ),
        "language_prompt": "Choose a language.",
        "custom_prompt_prompt": (
            "Send your custom prompt in one message.\n\n"
            "The bot will pass it as the instruction and add the post text separately."
        ),
        "filter_prompt_prompt": "Copy the AI filter prompt and send the edited version.",
        "summary_prompt_prompt": "Copy the AI summary prompt and send the edited version.",
        "bulk_add_result": "Channel add completed.",
        "bulk_remove_result": "Channel removal completed.",
        "preset_list_title": "Ready-made channel sets\n\nChoose a preset to create a new subscription.",
        "preset_confirm": "Create a subscription from the \"{name}\" preset?\n\nChannels:\n{channels}",
        "preset_create_result": "Preset subscription created.",
        "preset_no_channels": "Could not create the subscription: none of the preset channels are available.",
        "preset_unknown": "Unknown preset.",
        "limit_subscriptions": "Limit reached: you can create up to {limit} subscriptions.",
        "limit_channels": "Limit reached: one subscription can contain up to {limit} channels.",
        "limit_assistant_tools": "Assistant action limit reached for one request: {limit}. Please clarify the task or split it into several messages.",
        "result_added": "Added: {items}",
        "result_already": "Already subscribed: {items}",
        "result_removed": "Removed: {items}",
        "result_not_subscribed": "Not subscribed: {items}",
        "result_not_found": "Not found or not publicly readable: {items}",
        "result_invalid": "Invalid input: {items}",
        "result_limit_exceeded": "Not added due to the limit: {items}",
        "result_nothing": "Nothing changed.",
    },
}


def t(language: str, key: str, **kwargs: str) -> str:
    """Return a localized string with a Russian fallback."""
    locale = language if language in TEXTS else "ru"
    template = TEXTS[locale][key]
    return template.format(**kwargs) if kwargs else template
