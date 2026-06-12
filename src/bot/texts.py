"""Minimal localized bot text catalog."""

from __future__ import annotations

TEXTS = {
    "ru": {
        "app_title": "Telegram Parser Bot v1",
        "home_hint": "Используйте кнопки ниже для настроек и подписок.",
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
        "button_rename_subscription": "✏️ Переименовать",
        "button_delete_subscription": "🗑 Удалить подписку",
        "button_open_subscription": "Открыть",
        "button_toggle_on": "🔕 Выключить",
        "button_toggle_off": "🔔 Включить",
        "button_frequency": "⏱ Частота уведомлений",
        "button_digest_format": "📝 Формат дайджеста",
        "button_timezone": "Часовой пояс",
        "button_language": "Язык",
        "button_digest_200": "200 символов",
        "button_digest_summary": "Пересказ",
        "button_summary_brief": "Кратко",
        "button_summary_detailed": "Подробно",
        "button_summary_custom": "Свой вариант",
        "button_timezone_manual": "Ввести вручную",
        "button_language_ru": "🇷🇺 Русский",
        "button_language_en": "🇬🇧 English",
        "digest_updated": "Формат дайджеста обновлен",
        "summary_mode_updated": "Режим пересказа обновлен",
        "custom_prompt_updated": "Свой prompt сохранен",
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
        "bulk_add_result": "Добавление каналов завершено.",
        "bulk_remove_result": "Удаление каналов завершено.",
        "result_added": "Добавлены: {items}",
        "result_already": "Уже были в подписках: {items}",
        "result_removed": "Удалены: {items}",
        "result_not_subscribed": "Не были в подписках: {items}",
        "result_not_found": "Не найдены или недоступны: {items}",
        "result_invalid": "Некорректный ввод: {items}",
        "result_nothing": "Ничего не изменилось.",
    },
    "en": {
        "app_title": "Telegram Parser Bot v1",
        "home_hint": "Use the buttons below to manage settings and subscriptions.",
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
        "button_rename_subscription": "✏️ Rename",
        "button_delete_subscription": "🗑 Delete subscription",
        "button_open_subscription": "Open",
        "button_toggle_on": "🔕 Disable",
        "button_toggle_off": "🔔 Enable",
        "button_frequency": "⏱ Frequency",
        "button_digest_format": "📝 Digest format",
        "button_timezone": "Timezone",
        "button_language": "Language",
        "button_digest_200": "200 chars",
        "button_digest_summary": "Summary",
        "button_summary_brief": "Brief",
        "button_summary_detailed": "Detailed",
        "button_summary_custom": "Custom",
        "button_timezone_manual": "Enter manually",
        "button_language_ru": "🇷🇺 Русский",
        "button_language_en": "🇬🇧 English",
        "digest_updated": "Digest format updated",
        "summary_mode_updated": "Summary mode updated",
        "custom_prompt_updated": "Custom prompt saved",
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
        "bulk_add_result": "Channel add completed.",
        "bulk_remove_result": "Channel removal completed.",
        "result_added": "Added: {items}",
        "result_already": "Already subscribed: {items}",
        "result_removed": "Removed: {items}",
        "result_not_subscribed": "Not subscribed: {items}",
        "result_not_found": "Not found or not publicly readable: {items}",
        "result_invalid": "Invalid input: {items}",
        "result_nothing": "Nothing changed.",
    },
}


def t(language: str, key: str, **kwargs: str) -> str:
    """Return a localized string with a Russian fallback."""
    locale = language if language in TEXTS else "ru"
    template = TEXTS[locale][key]
    return template.format(**kwargs) if kwargs else template
