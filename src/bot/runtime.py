"""Telegram bot runtime running in-process via polling."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from datetime import datetime, timedelta, timezone
import logging

from aiogram import Bot, Dispatcher, F, Router
from aiogram.enums import ChatAction, ChatType, ParseMode
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BotCommand,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)
from sqlalchemy.ext.asyncio import async_sessionmaker

from src.assistant.memory import AssistantMemoryService
from src.assistant.service import AssistantAgentService
from src.bot.service import (
    BotService,
    ChannelPreset,
    TelegramIdentity,
    ProductLimitExceededError,
    format_digest_prompt_settings_text,
    format_digest_processing_stats,
    format_settings_text,
    format_subscription_detail_text,
    format_subscriptions_text,
    get_channel_preset,
    list_channel_presets,
    subscription_button_label,
)
from src.bot.texts import t
from src.config.settings import Settings
from src.llm import OpenRouterModelPool
from src.models.subscription import Subscription
from src.models.user import DeliveryFrequency, DigestFormat, SummaryMode, User
from src.scraper.client import TelegramClient
from src.telegram_formatting import telegram_safe_html

logger = logging.getLogger(__name__)

COMMON_TIMEZONES = ["UTC+2", "UTC+3", "UTC+4", "UTC+5", "UTC+6"]


class BotStates(StatesGroup):
    awaiting_bulk_add = State()
    awaiting_bulk_remove = State()
    awaiting_timezone = State()
    awaiting_filter_prompt = State()
    awaiting_custom_prompt = State()
    awaiting_create_subscription = State()
    awaiting_rename_subscription = State()


def _active(text: str, enabled: bool) -> str:
    return f"✅ {text}" if enabled else text


def is_supported_chat(
    chat_type: ChatType | str,
    chat_id: int,
    allowed_e2e_chat_id: int | None = None,
) -> bool:
    return chat_type in {ChatType.PRIVATE, ChatType.PRIVATE.value} or (
        allowed_e2e_chat_id is not None and chat_id == allowed_e2e_chat_id
    )


def _home_reply_keyboard(language: str) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=t(language, "button_settings")), KeyboardButton(text=t(language, "button_subscriptions"))],
        ],
        resize_keyboard=True,
        input_field_placeholder=t(language, "home_short"),
    )


def _settings_keyboard(language: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"🕒 {t(language, 'button_timezone')}", callback_data="settings:timezone")],
            [InlineKeyboardButton(text=f"🌐 {t(language, 'button_language')}", callback_data="settings:language")],
            [InlineKeyboardButton(text=t(language, "button_close"), callback_data="screen:close")],
        ]
    )


def _language_keyboard(current_language: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=_active(t("ru", "button_language_ru"), current_language == "ru"),
                    callback_data="settings:language:set:ru",
                ),
                InlineKeyboardButton(
                    text=_active(t("en", "button_language_en"), current_language == "en"),
                    callback_data="settings:language:set:en",
                ),
            ],
            [InlineKeyboardButton(text=t(current_language, "button_back"), callback_data="settings:back")],
            [InlineKeyboardButton(text=t(current_language, "button_close"), callback_data="screen:close")],
        ]
    )


def _timezone_keyboard(language: str, current_timezone: str) -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton(
                text=_active(timezone_name, timezone_name == current_timezone),
                callback_data=f"settings:timezone:set:{timezone_name}",
            )
        ]
        for timezone_name in COMMON_TIMEZONES
    ]
    keyboard.append([InlineKeyboardButton(text=t(language, "button_timezone_manual"), callback_data="settings:timezone:manual")])
    keyboard.append([InlineKeyboardButton(text=t(language, "button_back"), callback_data="settings:back")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def _subscriptions_keyboard(subscriptions: list[Subscription], user: User) -> InlineKeyboardMarkup:
    keyboard: list[list[InlineKeyboardButton]] = []
    for subscription in subscriptions:
        keyboard.append(
            [
                InlineKeyboardButton(
                    text=subscription_button_label(subscription, user),
                    callback_data=f"subscriptions:open:{subscription.id}",
                )
            ]
        )
    keyboard.append([InlineKeyboardButton(text=t(user.language, "button_create_subscription"), callback_data="subscriptions:create")])
    keyboard.append([InlineKeyboardButton(text=t(user.language, "button_add_from_presets"), callback_data="subscriptions:presets")])
    keyboard.append([InlineKeyboardButton(text=t(user.language, "button_close"), callback_data="screen:close")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def _preset_list_keyboard(language: str) -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton(text=preset.name, callback_data=f"subscriptions:preset:open:{preset.id}")]
        for preset in list_channel_presets()
    ]
    keyboard.append([InlineKeyboardButton(text=t(language, "button_back"), callback_data="subscriptions:back")])
    keyboard.append([InlineKeyboardButton(text=t(language, "button_close"), callback_data="screen:close")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def _preset_confirm_keyboard(preset: ChannelPreset, language: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=t(language, "button_confirm_preset"), callback_data=f"subscriptions:preset:create:{preset.id}")],
            [InlineKeyboardButton(text=t(language, "button_back"), callback_data="subscriptions:presets")],
            [InlineKeyboardButton(text=t(language, "button_close"), callback_data="screen:close")],
        ]
    )


def _format_preset_confirm_text(preset: ChannelPreset, language: str) -> str:
    channels = "\n".join(f"- @{username}" for username in preset.channels)
    return t(language, "preset_confirm", name=preset.name, channels=channels)


def _subscription_detail_keyboard(subscription: Subscription, language: str) -> InlineKeyboardMarkup:
    toggle_key = "button_toggle_on" if subscription.enabled else "button_toggle_off"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=t(language, "button_add_channels"), callback_data=f"subscription:add:{subscription.id}")],
            [InlineKeyboardButton(text=t(language, "button_remove_channels"), callback_data=f"subscription:remove:{subscription.id}")],
            [InlineKeyboardButton(text=t(language, "button_frequency"), callback_data=f"subscription:frequency:{subscription.id}")],
            [InlineKeyboardButton(text=t(language, "button_digest_format"), callback_data=f"subscription:format:{subscription.id}")],
            [InlineKeyboardButton(text=t(language, "button_processing_log"), callback_data=f"subscription:processing_log:{subscription.id}")],
            [InlineKeyboardButton(text=t(language, toggle_key), callback_data=f"subscription:toggle:{subscription.id}")],
            [InlineKeyboardButton(text=t(language, "button_rename_subscription"), callback_data=f"subscription:rename:{subscription.id}")],
            [InlineKeyboardButton(text=t(language, "button_delete_subscription"), callback_data=f"subscription:delete:{subscription.id}")],
            [InlineKeyboardButton(text=t(language, "button_back"), callback_data="subscriptions:back")],
            [InlineKeyboardButton(text=t(language, "button_close"), callback_data="screen:close")],
        ]
    )


def _processing_log_keyboard(subscription: Subscription, language: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=t(language, "button_back"), callback_data=f"subscriptions:open:{subscription.id}")],
            [InlineKeyboardButton(text=t(language, "button_close"), callback_data="screen:close")],
        ]
    )


def _frequency_keyboard(subscription: Subscription, language: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=_active("Каждый час" if language == "ru" else "Hourly", subscription.frequency == DeliveryFrequency.HOURLY),
                    callback_data=f"subscription:frequency:set:{subscription.id}:hourly",
                ),
                InlineKeyboardButton(
                    text=_active("Раз в день" if language == "ru" else "Daily", subscription.frequency == DeliveryFrequency.DAILY),
                    callback_data=f"subscription:frequency:set:{subscription.id}:daily",
                ),
            ],
            [InlineKeyboardButton(text=t(language, "button_back"), callback_data=f"subscriptions:open:{subscription.id}")],
        ]
    )


def _format_keyboard(subscription: Subscription, language: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=t(language, "button_edit_filter_prompt"),
                    callback_data=f"subscription:filter_prompt:{subscription.id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text=t(language, "button_edit_summary_prompt"),
                    callback_data=f"subscription:summary_prompt:{subscription.id}",
                )
            ],
            [InlineKeyboardButton(text=t(language, "button_prompts_default"), callback_data=f"subscription:prompts:reset:{subscription.id}")],
            [InlineKeyboardButton(text=t(language, "button_back"), callback_data=f"subscriptions:open:{subscription.id}")],
        ]
    )


def _summary_keyboard(subscription: Subscription, language: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=_active(t(language, "button_summary_brief"), subscription.summary_mode == SummaryMode.BRIEF),
                    callback_data=f"subscription:summary:set:{subscription.id}:brief",
                )
            ],
            [
                InlineKeyboardButton(
                    text=_active(t(language, "button_summary_detailed"), subscription.summary_mode == SummaryMode.DETAILED),
                    callback_data=f"subscription:summary:set:{subscription.id}:detailed",
                )
            ],
            [
                InlineKeyboardButton(
                    text=_active(t(language, "button_summary_custom"), subscription.summary_mode == SummaryMode.CUSTOM),
                    callback_data=f"subscription:summary:set:{subscription.id}:custom",
                )
            ],
            [InlineKeyboardButton(text=t(language, "button_back"), callback_data=f"subscription:format:{subscription.id}")],
        ]
    )


def _format_bulk_result(language: str, title: str, sections: list[tuple[str, list[str]]]) -> str:
    lines = [title]
    for key, items in sections:
        if items:
            lines.append("")
            lines.append(t(language, key, items=", ".join(items)))
    if len(lines) == 1:
        lines.extend(["", t(language, "result_nothing")])
    return "\n".join(lines)


def _format_limit_error(language: str, exc: ProductLimitExceededError) -> str:
    if exc.code == "max_subscriptions_per_user":
        return t(language, "limit_subscriptions", limit=str(exc.limit))
    if exc.code == "max_channels_per_subscription":
        return t(language, "limit_channels", limit=str(exc.limit))
    return str(exc)


def build_router(
    service: BotService,
    allowed_e2e_chat_id: int | None = None,
    assistant_service: AssistantAgentService | None = None,
) -> Router:
    router = Router()
    screen_messages: dict[int, int] = {}

    async def ensure_private(message: Message) -> bool:
        if not is_supported_chat(message.chat.type, message.chat.id, allowed_e2e_chat_id):
            await message.answer(t("ru", "private_only"))
            return False
        return True

    async def ensure_callback_private(callback: CallbackQuery) -> bool:
        if callback.message is None or not is_supported_chat(
            callback.message.chat.type,
            callback.message.chat.id,
            allowed_e2e_chat_id,
        ):
            await callback.answer(t("ru", "private_only_alert"), show_alert=True)
            return False
        return True

    async def ensure_registered(message: Message) -> User | None:
        if message.from_user is None:
            return None
        return await service.ensure_user(
            TelegramIdentity(
                telegram_user_id=message.from_user.id,
                chat_id=message.chat.id,
                chat_type=message.chat.type,
                username=message.from_user.username,
                first_name=message.from_user.first_name,
                last_name=message.from_user.last_name,
                language_code=message.from_user.language_code,
            )
        )

    async def ensure_registered_from_callback(callback: CallbackQuery) -> User | None:
        if callback.from_user is None or callback.message is None:
            return None
        return await service.ensure_user(
            TelegramIdentity(
                telegram_user_id=callback.from_user.id,
                chat_id=callback.message.chat.id,
                chat_type=callback.message.chat.type,
                username=callback.from_user.username,
                first_name=callback.from_user.first_name,
                last_name=callback.from_user.last_name,
                language_code=callback.from_user.language_code,
            )
        )

    def _remember_screen(chat_id: int, message_id: int) -> None:
        screen_messages[chat_id] = message_id

    async def delete_message_safe(message: Message | None) -> None:
        if message is None:
            return
        try:
            await message.delete()
        except Exception:
            logger.debug("Message delete skipped", exc_info=True)

    async def cleanup_flow_messages(message: Message, state: FSMContext) -> None:
        data = await state.get_data()
        prompt_message_id = data.get("prompt_message_id")
        if prompt_message_id:
            try:
                await message.bot.delete_message(chat_id=message.chat.id, message_id=prompt_message_id)
            except Exception:
                logger.debug("Prompt delete skipped", exc_info=True)
        await delete_message_safe(message)

    async def show_typing_until_cancelled(message: Message) -> None:
        while True:
            try:
                await message.bot.send_chat_action(chat_id=message.chat.id, action=ChatAction.TYPING)
            except Exception:
                logger.debug("Typing action skipped", exc_info=True)
            await asyncio.sleep(4)

    async def render_screen(
        chat_id: int,
        bot: Bot,
        text: str,
        reply_markup: InlineKeyboardMarkup,
        parse_mode: str | None = None,
    ) -> None:
        screen_message_id = screen_messages.get(chat_id)
        if screen_message_id is not None:
            try:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=screen_message_id,
                    text=text,
                    reply_markup=reply_markup,
                    parse_mode=parse_mode,
                )
                return
            except Exception:
                logger.debug("Screen edit skipped", exc_info=True)
                screen_messages.pop(chat_id, None)

        screen_message = await bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=reply_markup,
            parse_mode=parse_mode,
        )
        _remember_screen(chat_id, screen_message.message_id)

    async def render_settings_screen(message: Message, user: User) -> None:
        await render_screen(message.chat.id, message.bot, format_settings_text(user, user.language), _settings_keyboard(user.language))

    async def render_subscriptions_screen(message: Message, user: User, notice: str | None = None) -> None:
        subscriptions = await service.list_subscriptions(user.telegram_user_id)
        text = format_subscriptions_text(
            subscriptions,
            user.language,
            max_subscriptions=service.max_subscriptions_per_user,
            max_channels=service.max_channels_per_subscription,
        )
        if notice:
            text = f"{notice}\n\n{text}"
        await render_screen(message.chat.id, message.bot, text, _subscriptions_keyboard(subscriptions, user))

    async def render_preset_list_screen(message: Message, user: User, notice: str | None = None) -> None:
        text = t(user.language, "preset_list_title")
        if notice:
            text = f"{notice}\n\n{text}"
        await render_screen(message.chat.id, message.bot, text, _preset_list_keyboard(user.language))

    async def render_subscription_detail_screen(message: Message, user: User, subscription_id: int, notice: str | None = None) -> None:
        subscription = await service.get_subscription(user.telegram_user_id, subscription_id)
        if subscription is None:
            await render_subscriptions_screen(message, user)
            return
        text = format_subscription_detail_text(subscription, user)
        if notice:
            text = f"{notice}\n\n{text}"
        await render_screen(message.chat.id, message.bot, text, _subscription_detail_keyboard(subscription, user.language))

    async def close_screen(callback: CallbackQuery, state: FSMContext) -> None:
        if callback.message is None:
            return
        await state.clear()
        screen_messages.pop(callback.message.chat.id, None)
        await callback.answer()
        await delete_message_safe(callback.message)

    @router.message(Command("start"))
    async def start_command(message: Message, state: FSMContext) -> None:
        if not await ensure_private(message):
            return
        user = await ensure_registered(message)
        if user is None:
            return
        await state.clear()
        await message.answer(
            t(user.language, "home_hint"),
            parse_mode=ParseMode.HTML,
            reply_markup=_home_reply_keyboard(user.language),
        )

    @router.message(Command("help"))
    async def help_command(message: Message) -> None:
        if not await ensure_private(message):
            return
        user = await ensure_registered(message)
        if user is None:
            return
        await message.answer(t(user.language, "help"), reply_markup=_home_reply_keyboard(user.language))

    @router.message(Command("settings"))
    async def settings_command(message: Message, state: FSMContext) -> None:
        if not await ensure_private(message):
            return
        user = await ensure_registered(message)
        if user is None:
            return
        await state.clear()
        await render_settings_screen(message, user)

    @router.message(Command("subscriptions"))
    async def subscriptions_command(message: Message, state: FSMContext) -> None:
        if not await ensure_private(message):
            return
        user = await ensure_registered(message)
        if user is None:
            return
        await state.clear()
        await render_subscriptions_screen(message, user)

    @router.message(Command("cancel"))
    async def cancel_command(message: Message, state: FSMContext) -> None:
        if not await ensure_private(message):
            return
        user = await ensure_registered(message)
        if user is None:
            return
        await state.clear()
        await message.answer(t(user.language, "cancelled"), reply_markup=_home_reply_keyboard(user.language))

    @router.message(StateFilter(None))
    async def home_reply_buttons(message: Message, state: FSMContext) -> None:
        if not await ensure_private(message):
            return
        user = await ensure_registered(message)
        if user is None:
            return
        text = (message.text or "").strip()
        if text in {t("ru", "button_settings"), t("en", "button_settings")}:
            await state.clear()
            await render_settings_screen(message, user)
            await delete_message_safe(message)
            return
        if text in {t("ru", "button_subscriptions"), t("en", "button_subscriptions")}:
            await state.clear()
            await render_subscriptions_screen(message, user)
            await delete_message_safe(message)
            return
        if text and assistant_service is not None:
            typing_task = asyncio.create_task(show_typing_until_cancelled(message))
            try:
                result = await assistant_service.handle_message(user, text)
            finally:
                typing_task.cancel()
                with suppress(asyncio.CancelledError):
                    await typing_task
            for system_message in result.system_messages:
                await message.answer(telegram_safe_html(system_message), parse_mode=ParseMode.HTML)
            if result.reply_text:
                await message.answer(
                    telegram_safe_html(result.reply_text),
                    parse_mode=ParseMode.HTML,
                    reply_markup=_home_reply_keyboard(user.language),
                )

    @router.callback_query(F.data == "screen:close")
    async def close_callback(callback: CallbackQuery, state: FSMContext) -> None:
        if not await ensure_callback_private(callback):
            return
        await close_screen(callback, state)

    @router.callback_query(F.data == "settings:back")
    async def settings_back_callback(callback: CallbackQuery, state: FSMContext) -> None:
        if not await ensure_callback_private(callback):
            return
        user = await ensure_registered_from_callback(callback)
        if user is None or callback.message is None:
            return
        await state.clear()
        _remember_screen(callback.message.chat.id, callback.message.message_id)
        await callback.answer()
        await callback.message.edit_text(format_settings_text(user, user.language), reply_markup=_settings_keyboard(user.language))

    @router.callback_query(F.data == "settings:language")
    async def language_prompt_callback(callback: CallbackQuery) -> None:
        if not await ensure_callback_private(callback):
            return
        user = await ensure_registered_from_callback(callback)
        if user is None or callback.message is None:
            return
        await callback.answer()
        await callback.message.edit_text(t(user.language, "language_prompt"), reply_markup=_language_keyboard(user.language))

    @router.callback_query(F.data.startswith("settings:language:set:"))
    async def language_set_callback(callback: CallbackQuery) -> None:
        if not await ensure_callback_private(callback):
            return
        language = callback.data.rsplit(":", 1)[-1]
        user = await service.update_language(callback.from_user.id, language)
        await callback.answer(t(user.language, "language_updated"))
        if callback.message is not None:
            await callback.message.edit_text(format_settings_text(user, user.language), reply_markup=_settings_keyboard(user.language))

    @router.callback_query(F.data == "settings:timezone")
    async def timezone_prompt_callback(callback: CallbackQuery, state: FSMContext) -> None:
        if not await ensure_callback_private(callback):
            return
        user = await ensure_registered_from_callback(callback)
        if user is None or callback.message is None:
            return
        await state.clear()
        await callback.answer()
        await callback.message.edit_text(
            t(user.language, "timezone_prompt"),
            reply_markup=_timezone_keyboard(user.language, user.timezone),
            parse_mode="Markdown",
        )

    @router.callback_query(F.data.startswith("settings:timezone:set:"))
    async def timezone_set_callback(callback: CallbackQuery) -> None:
        if not await ensure_callback_private(callback):
            return
        timezone_name = callback.data.split(":", 3)[-1]
        user = await service.update_timezone(callback.from_user.id, timezone_name)
        await callback.answer(t(user.language, "timezone_updated"))
        if callback.message is not None:
            await callback.message.edit_text(format_settings_text(user, user.language), reply_markup=_settings_keyboard(user.language))

    @router.callback_query(F.data == "settings:timezone:manual")
    async def timezone_manual_callback(callback: CallbackQuery, state: FSMContext) -> None:
        if not await ensure_callback_private(callback):
            return
        user = await ensure_registered_from_callback(callback)
        if user is None or callback.message is None:
            return
        await state.set_state(BotStates.awaiting_timezone)
        prompt_message = await callback.message.answer(t(user.language, "timezone_prompt"), parse_mode="Markdown")
        await state.update_data(prompt_message_id=prompt_message.message_id)
        await callback.answer()

    @router.message(BotStates.awaiting_timezone)
    async def timezone_message(message: Message, state: FSMContext) -> None:
        if not await ensure_private(message):
            return
        user = await ensure_registered(message)
        if user is None:
            return
        try:
            user = await service.update_timezone(message.from_user.id, message.text or "")
        except ValueError as exc:
            await message.answer(str(exc))
            return
        await cleanup_flow_messages(message, state)
        await state.clear()
        await render_settings_screen(message, user)

    @router.callback_query(F.data == "subscriptions:back")
    async def subscriptions_back_callback(callback: CallbackQuery, state: FSMContext) -> None:
        if not await ensure_callback_private(callback):
            return
        user = await ensure_registered_from_callback(callback)
        if user is None or callback.message is None:
            return
        await state.clear()
        _remember_screen(callback.message.chat.id, callback.message.message_id)
        await callback.answer()
        await render_subscriptions_screen(callback.message, user)

    @router.callback_query(F.data == "subscriptions:create")
    async def subscriptions_create_callback(callback: CallbackQuery, state: FSMContext) -> None:
        if not await ensure_callback_private(callback):
            return
        user = await ensure_registered_from_callback(callback)
        if user is None or callback.message is None:
            return
        await state.set_state(BotStates.awaiting_create_subscription)
        prompt_message = await callback.message.answer(t(user.language, "create_subscription_prompt"))
        await state.update_data(prompt_message_id=prompt_message.message_id)
        await callback.answer()

    @router.callback_query(F.data == "subscriptions:presets")
    async def subscriptions_presets_callback(callback: CallbackQuery, state: FSMContext) -> None:
        if not await ensure_callback_private(callback):
            return
        user = await ensure_registered_from_callback(callback)
        if user is None or callback.message is None:
            return
        await state.clear()
        _remember_screen(callback.message.chat.id, callback.message.message_id)
        await callback.answer()
        await render_preset_list_screen(callback.message, user)

    @router.callback_query(F.data.startswith("subscriptions:preset:open:"))
    async def preset_confirm_callback(callback: CallbackQuery, state: FSMContext) -> None:
        if not await ensure_callback_private(callback):
            return
        user = await ensure_registered_from_callback(callback)
        if user is None or callback.message is None:
            return
        await state.clear()
        preset_id = callback.data.rsplit(":", 1)[-1]
        preset = get_channel_preset(preset_id)
        _remember_screen(callback.message.chat.id, callback.message.message_id)
        if preset is None:
            await callback.answer(t(user.language, "preset_unknown"), show_alert=True)
            await render_preset_list_screen(callback.message, user)
            return
        await callback.answer()
        await render_screen(
            callback.message.chat.id,
            callback.message.bot,
            _format_preset_confirm_text(preset, user.language),
            _preset_confirm_keyboard(preset, user.language),
        )

    @router.callback_query(F.data.startswith("subscriptions:preset:create:"))
    async def preset_create_callback(callback: CallbackQuery, state: FSMContext) -> None:
        if not await ensure_callback_private(callback):
            return
        user = await ensure_registered_from_callback(callback)
        if user is None or callback.message is None:
            return
        await state.clear()
        preset_id = callback.data.rsplit(":", 1)[-1]
        try:
            result = await service.create_subscription_from_preset(callback.from_user.id, preset_id)
        except ProductLimitExceededError as exc:
            notice = _format_limit_error(user.language, exc)
            await callback.answer(notice, show_alert=True)
            await render_subscriptions_screen(callback.message, user, notice)
            return
        except ValueError:
            _remember_screen(callback.message.chat.id, callback.message.message_id)
            await callback.answer(t(user.language, "preset_unknown"), show_alert=True)
            await render_preset_list_screen(callback.message, user)
            return

        if result.subscription is None:
            notice = t(user.language, "preset_no_channels")
            await callback.answer(notice, show_alert=True)
            await render_preset_list_screen(callback.message, user, notice)
            return

        notice = _format_bulk_result(
            user.language,
            t(user.language, "preset_create_result"),
            [
                ("result_added", result.added),
                ("result_not_found", result.not_found),
                ("result_limit_exceeded", result.limit_exceeded),
            ],
        )
        await callback.answer(t(user.language, "subscription_created"))
        await render_subscription_detail_screen(callback.message, user, result.subscription.id, notice)

    @router.message(BotStates.awaiting_create_subscription)
    async def create_subscription_message(message: Message, state: FSMContext) -> None:
        if not await ensure_private(message):
            return
        user = await ensure_registered(message)
        if user is None:
            return
        try:
            subscription = await service.create_subscription(message.from_user.id, message.text or None)
        except ProductLimitExceededError as exc:
            await message.answer(_format_limit_error(user.language, exc))
            return
        await cleanup_flow_messages(message, state)
        await state.clear()
        await render_subscription_detail_screen(message, user, subscription.id, t(user.language, "subscription_created"))

    @router.callback_query(F.data.startswith("subscriptions:open:"))
    async def subscription_open_callback(callback: CallbackQuery, state: FSMContext) -> None:
        if not await ensure_callback_private(callback):
            return
        user = await ensure_registered_from_callback(callback)
        if user is None or callback.message is None:
            return
        subscription_id = int(callback.data.rsplit(":", 1)[-1])
        await state.clear()
        _remember_screen(callback.message.chat.id, callback.message.message_id)
        await callback.answer()
        await render_subscription_detail_screen(callback.message, user, subscription_id)

    @router.callback_query(F.data.startswith("subscription:add:"))
    async def add_channel_prompt(callback: CallbackQuery, state: FSMContext) -> None:
        if not await ensure_callback_private(callback):
            return
        user = await ensure_registered_from_callback(callback)
        if user is None or callback.message is None:
            return
        subscription_id = int(callback.data.rsplit(":", 1)[-1])
        await state.set_state(BotStates.awaiting_bulk_add)
        prompt_message = await callback.message.answer(t(user.language, "add_channels_prompt"), parse_mode="Markdown")
        await state.update_data(prompt_message_id=prompt_message.message_id, subscription_id=subscription_id)
        await callback.answer()

    @router.message(BotStates.awaiting_bulk_add)
    async def bulk_add_message(message: Message, state: FSMContext) -> None:
        if not await ensure_private(message):
            return
        user = await ensure_registered(message)
        if user is None:
            return
        data = await state.get_data()
        subscription_id = int(data["subscription_id"])
        result = await service.subscribe_many(message.from_user.id, subscription_id, message.text or "")
        await cleanup_flow_messages(message, state)
        await state.clear()
        await render_subscription_detail_screen(
            message,
            user,
            subscription_id,
            _format_bulk_result(
                user.language,
                t(user.language, "bulk_add_result"),
                [
                    ("result_added", result.added),
                    ("result_already", result.already_subscribed),
                    ("result_not_found", result.not_found),
                    ("result_invalid", result.invalid),
                    ("result_limit_exceeded", result.limit_exceeded),
                ],
            ),
        )

    @router.callback_query(F.data.startswith("subscription:remove:"))
    async def remove_channel_prompt(callback: CallbackQuery, state: FSMContext) -> None:
        if not await ensure_callback_private(callback):
            return
        user = await ensure_registered_from_callback(callback)
        if user is None or callback.message is None:
            return
        subscription_id = int(callback.data.rsplit(":", 1)[-1])
        channels = await service.list_channels(user.telegram_user_id, subscription_id)
        subscriptions = "\n".join(f"@{channel.username}" for channel in channels if channel.username) or "-"
        await state.set_state(BotStates.awaiting_bulk_remove)
        prompt_message = await callback.message.answer(t(user.language, "remove_channels_prompt", subscriptions=subscriptions))
        await state.update_data(prompt_message_id=prompt_message.message_id, subscription_id=subscription_id)
        await callback.answer()

    @router.message(BotStates.awaiting_bulk_remove)
    async def bulk_remove_message(message: Message, state: FSMContext) -> None:
        if not await ensure_private(message):
            return
        user = await ensure_registered(message)
        if user is None:
            return
        data = await state.get_data()
        subscription_id = int(data["subscription_id"])
        result = await service.unsubscribe_many(message.from_user.id, subscription_id, message.text or "")
        await cleanup_flow_messages(message, state)
        await state.clear()
        await render_subscription_detail_screen(
            message,
            user,
            subscription_id,
            _format_bulk_result(
                user.language,
                t(user.language, "bulk_remove_result"),
                [
                    ("result_removed", result.removed),
                    ("result_not_subscribed", result.not_subscribed),
                    ("result_invalid", result.invalid),
                ],
            ),
        )

    @router.callback_query(F.data.startswith("subscription:toggle:"))
    async def toggle_subscription_callback(callback: CallbackQuery) -> None:
        if not await ensure_callback_private(callback):
            return
        user = await ensure_registered_from_callback(callback)
        if user is None or callback.message is None:
            return
        subscription_id = int(callback.data.rsplit(":", 1)[-1])
        await service.toggle_subscription_enabled(callback.from_user.id, subscription_id)
        await callback.answer(t(user.language, "subscription_updated"))
        await render_subscription_detail_screen(callback.message, user, subscription_id)

    @router.callback_query(F.data.startswith("subscription:delete:"))
    async def delete_subscription_callback(callback: CallbackQuery) -> None:
        if not await ensure_callback_private(callback):
            return
        user = await ensure_registered_from_callback(callback)
        if user is None or callback.message is None:
            return
        subscription_id = int(callback.data.rsplit(":", 1)[-1])
        await service.delete_subscription(callback.from_user.id, subscription_id)
        await callback.answer(t(user.language, "subscription_deleted"))
        await render_subscriptions_screen(callback.message, user, t(user.language, "subscription_deleted"))

    @router.callback_query(F.data.startswith("subscription:rename:"))
    async def rename_subscription_callback(callback: CallbackQuery, state: FSMContext) -> None:
        if not await ensure_callback_private(callback):
            return
        user = await ensure_registered_from_callback(callback)
        if user is None or callback.message is None:
            return
        subscription_id = int(callback.data.rsplit(":", 1)[-1])
        await state.set_state(BotStates.awaiting_rename_subscription)
        prompt_message = await callback.message.answer(t(user.language, "rename_subscription_prompt"))
        await state.update_data(prompt_message_id=prompt_message.message_id, subscription_id=subscription_id)
        await callback.answer()

    @router.message(BotStates.awaiting_rename_subscription)
    async def rename_subscription_message(message: Message, state: FSMContext) -> None:
        if not await ensure_private(message):
            return
        user = await ensure_registered(message)
        if user is None:
            return
        data = await state.get_data()
        subscription_id = int(data["subscription_id"])
        await service.rename_subscription(message.from_user.id, subscription_id, message.text or "")
        await cleanup_flow_messages(message, state)
        await state.clear()
        await render_subscription_detail_screen(message, user, subscription_id, t(user.language, "subscription_updated"))

    @router.callback_query(F.data.regexp(r"^subscription:frequency:\d+$"))
    async def frequency_screen_callback(callback: CallbackQuery) -> None:
        if not await ensure_callback_private(callback):
            return
        user = await ensure_registered_from_callback(callback)
        if user is None or callback.message is None:
            return
        subscription_id = int(callback.data.rsplit(":", 1)[-1])
        subscription = await service.get_subscription(user.telegram_user_id, subscription_id)
        if subscription is None:
            return
        await callback.answer()
        await callback.message.edit_text(format_subscription_detail_text(subscription, user), reply_markup=_frequency_keyboard(subscription, user.language))

    @router.callback_query(F.data.regexp(r"^subscription:frequency:set:\d+:(hourly|daily)$"))
    async def frequency_set_callback(callback: CallbackQuery) -> None:
        if not await ensure_callback_private(callback):
            return
        _, _, _, subscription_id, value = callback.data.split(":")
        subscription = await service.update_subscription_frequency(callback.from_user.id, int(subscription_id), DeliveryFrequency(value))
        user = await service.get_user(callback.from_user.id)
        if user is None or callback.message is None:
            return
        await callback.answer(t(user.language, "subscription_updated"))
        await callback.message.edit_text(format_subscription_detail_text(subscription, user), reply_markup=_subscription_detail_keyboard(subscription, user.language))

    @router.callback_query(F.data.regexp(r"^subscription:format:\d+$"))
    async def format_screen_callback(callback: CallbackQuery) -> None:
        if not await ensure_callback_private(callback):
            return
        user = await ensure_registered_from_callback(callback)
        if user is None or callback.message is None:
            return
        subscription_id = int(callback.data.rsplit(":", 1)[-1])
        subscription = await service.get_subscription(user.telegram_user_id, subscription_id)
        if subscription is None:
            return
        await callback.answer()
        await callback.message.edit_text(
            format_digest_prompt_settings_text(subscription, user),
            reply_markup=_format_keyboard(subscription, user.language),
            parse_mode=ParseMode.HTML,
        )

    @router.callback_query(F.data.regexp(r"^subscription:processing_log:\d+$"))
    async def processing_log_callback(callback: CallbackQuery, state: FSMContext) -> None:
        if not await ensure_callback_private(callback):
            return
        user = await ensure_registered_from_callback(callback)
        if user is None or callback.message is None:
            return
        subscription_id = int(callback.data.rsplit(":", 1)[-1])
        subscription = await service.get_subscription(user.telegram_user_id, subscription_id)
        if subscription is None:
            await callback.answer(t(user.language, "subscription_deleted"), show_alert=True)
            return
        period_end = datetime.now(timezone.utc)
        period_start = period_end - timedelta(hours=24)
        stats = await service.get_digest_processing_stats(
            user.telegram_user_id,
            subscription_id,
            period_start,
            period_end,
        )
        await state.clear()
        await callback.answer()
        await callback.message.edit_text(
            format_digest_processing_stats(subscription, user, stats, period_start, period_end),
            reply_markup=_processing_log_keyboard(subscription, user.language),
            parse_mode=ParseMode.HTML,
        )

    @router.callback_query(F.data.regexp(r"^subscription:format:set:\d+:short$"))
    async def format_set_callback(callback: CallbackQuery) -> None:
        if not await ensure_callback_private(callback):
            return
        _, _, _, subscription_id, value = callback.data.split(":")
        subscription = await service.update_subscription_digest_format(callback.from_user.id, int(subscription_id), DigestFormat(value))
        user = await service.get_user(callback.from_user.id)
        if user is None or callback.message is None:
            return
        await callback.answer(t(user.language, "subscription_updated"))
        await callback.message.edit_text(format_subscription_detail_text(subscription, user), reply_markup=_subscription_detail_keyboard(subscription, user.language))

    @router.callback_query(F.data.regexp(r"^subscription:filter_prompt:\d+$"))
    async def filter_prompt_callback(callback: CallbackQuery, state: FSMContext) -> None:
        if not await ensure_callback_private(callback):
            return
        user = await ensure_registered_from_callback(callback)
        if user is None or callback.message is None:
            return
        subscription_id = int(callback.data.rsplit(":", 1)[-1])
        await state.set_state(BotStates.awaiting_filter_prompt)
        prompt_message = await callback.message.answer(t(user.language, "filter_prompt_prompt"))
        await state.update_data(prompt_message_id=prompt_message.message_id, subscription_id=subscription_id)
        await callback.answer()

    @router.callback_query(F.data.regexp(r"^subscription:summary_prompt:\d+$"))
    async def summary_prompt_callback(callback: CallbackQuery, state: FSMContext) -> None:
        if not await ensure_callback_private(callback):
            return
        user = await ensure_registered_from_callback(callback)
        if user is None or callback.message is None:
            return
        subscription_id = int(callback.data.rsplit(":", 1)[-1])
        await state.set_state(BotStates.awaiting_custom_prompt)
        prompt_message = await callback.message.answer(t(user.language, "summary_prompt_prompt"))
        await state.update_data(prompt_message_id=prompt_message.message_id, subscription_id=subscription_id)
        await callback.answer()

    @router.callback_query(F.data.regexp(r"^subscription:prompts:reset:\d+$"))
    async def prompts_reset_callback(callback: CallbackQuery) -> None:
        if not await ensure_callback_private(callback):
            return
        user = await ensure_registered_from_callback(callback)
        if user is None or callback.message is None:
            return
        subscription_id = int(callback.data.rsplit(":", 1)[-1])
        subscription = await service.reset_subscription_prompts(callback.from_user.id, subscription_id)
        await callback.answer(t(user.language, "prompts_reset"))
        await callback.message.edit_text(
            format_digest_prompt_settings_text(subscription, user),
            reply_markup=_format_keyboard(subscription, user.language),
            parse_mode=ParseMode.HTML,
        )

    @router.callback_query(F.data.regexp(r"^subscription:summary:\d+$"))
    async def summary_screen_callback(callback: CallbackQuery) -> None:
        if not await ensure_callback_private(callback):
            return
        user = await ensure_registered_from_callback(callback)
        if user is None or callback.message is None:
            return
        parts = callback.data.split(":")
        subscription_id = int(parts[2])
        subscription = await service.get_subscription(user.telegram_user_id, subscription_id)
        if subscription is None:
            return
        await callback.answer()
        await callback.message.edit_text(format_subscription_detail_text(subscription, user), reply_markup=_summary_keyboard(subscription, user.language))

    @router.callback_query(F.data.regexp(r"^subscription:summary:set:\d+:(brief|detailed|custom)$"))
    async def summary_set_callback(callback: CallbackQuery, state: FSMContext) -> None:
        if not await ensure_callback_private(callback):
            return
        _, _, _, subscription_id, value = callback.data.split(":")
        user = await service.get_user(callback.from_user.id)
        if user is None or callback.message is None:
            return
        if value == SummaryMode.CUSTOM.value:
            await state.set_state(BotStates.awaiting_custom_prompt)
            prompt_message = await callback.message.answer(t(user.language, "custom_prompt_prompt"))
            await state.update_data(prompt_message_id=prompt_message.message_id, subscription_id=int(subscription_id))
            await callback.answer()
            return
        subscription = await service.update_subscription_summary_mode(callback.from_user.id, int(subscription_id), SummaryMode(value))
        await callback.answer(t(user.language, "subscription_updated"))
        await callback.message.edit_text(format_subscription_detail_text(subscription, user), reply_markup=_subscription_detail_keyboard(subscription, user.language))

    @router.message(BotStates.awaiting_custom_prompt)
    async def custom_prompt_message(message: Message, state: FSMContext) -> None:
        if not await ensure_private(message):
            return
        user = await ensure_registered(message)
        if user is None:
            return
        data = await state.get_data()
        subscription_id = int(data["subscription_id"])
        try:
            subscription = await service.update_subscription_custom_prompt(message.from_user.id, subscription_id, message.text or "")
        except ValueError as exc:
            await message.answer(str(exc))
            return
        await cleanup_flow_messages(message, state)
        await state.clear()
        await render_screen(
            message.chat.id,
            message.bot,
            format_digest_prompt_settings_text(subscription, user),
            _format_keyboard(subscription, user.language),
            parse_mode=ParseMode.HTML,
        )

    @router.message(BotStates.awaiting_filter_prompt)
    async def filter_prompt_message(message: Message, state: FSMContext) -> None:
        if not await ensure_private(message):
            return
        user = await ensure_registered(message)
        if user is None:
            return
        data = await state.get_data()
        subscription_id = int(data["subscription_id"])
        try:
            subscription = await service.update_subscription_filter_prompt(message.from_user.id, subscription_id, message.text or "")
        except ValueError as exc:
            await message.answer(str(exc))
            return
        await cleanup_flow_messages(message, state)
        await state.clear()
        await render_screen(
            message.chat.id,
            message.bot,
            format_digest_prompt_settings_text(subscription, user),
            _format_keyboard(subscription, user.language),
            parse_mode=ParseMode.HTML,
        )

    return router


class BotRuntime:
    """Owns aiogram bot lifecycle inside the main process."""

    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker,
        scraper_client: TelegramClient,
        model_pool: OpenRouterModelPool | None = None,
        memory_service: AssistantMemoryService | None = None,
    ) -> None:
        self._settings = settings
        self._service = BotService(session_factory, scraper_client, settings.bot)
        self._memory_service = memory_service or AssistantMemoryService(settings.memory, settings.llm)
        self._assistant_service = (
            AssistantAgentService(
                settings=settings.assistant,
                llm_settings=settings.llm,
                session_factory=session_factory,
                scraper_client=scraper_client,
                bot_service=self._service,
                memory_service=self._memory_service,
                model_pool=model_pool,
            )
            if settings.assistant.enabled
            else None
        )
        self._bot = Bot(token=settings.bot.token)
        self._dispatcher = Dispatcher(storage=MemoryStorage())
        self._dispatcher.include_router(
            build_router(self._service, settings.bot.e2e_allowed_chat_id, self._assistant_service)
        )
        self._polling_task: asyncio.Task | None = None

    async def start(self) -> None:
        if self._settings.bot.set_commands_on_startup:
            await self._bot.set_my_commands(
                [
                    BotCommand(command="start", description="Open bot"),
                    BotCommand(command="help", description="Show help"),
                    BotCommand(command="settings", description="Open settings"),
                    BotCommand(command="subscriptions", description="Manage subscriptions"),
                    BotCommand(command="cancel", description="Cancel current input"),
                ]
            )

        self._polling_task = asyncio.create_task(
            self._dispatcher.start_polling(
                self._bot,
                handle_signals=False,
                allowed_updates=self._dispatcher.resolve_used_update_types(),
                drop_pending_updates=self._settings.bot.drop_pending_updates,
            )
        )
        logger.info("Telegram bot polling started")

    async def shutdown(self) -> None:
        if self._polling_task is not None and not self._polling_task.done():
            self._polling_task.cancel()
            await asyncio.gather(self._polling_task, return_exceptions=True)
        await self._bot.session.close()
        logger.info("Telegram bot polling stopped")
