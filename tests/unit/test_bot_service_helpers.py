"""Unit tests for bot input normalization helpers."""

from datetime import timezone

import pytest

from src.bot.service import (
    InvalidChannelReferenceError,
    InvalidTimezoneError,
    normalize_channel_reference,
    split_channel_references,
    normalize_timezone,
    timezone_info,
)


def test_normalize_channel_reference_accepts_username_forms() -> None:
    assert normalize_channel_reference("@durov") == "durov"
    assert normalize_channel_reference("https://t.me/telegram") == "telegram"
    assert normalize_channel_reference("t.me/example_chan/") == "example_chan"
    assert normalize_channel_reference("channel") == "channel"


def test_split_channel_references_supports_commas_and_newlines() -> None:
    assert split_channel_references("@a, https://t.me/b\nchannel") == ["@a", "https://t.me/b", "channel"]


def test_normalize_channel_reference_rejects_invalid_value() -> None:
    with pytest.raises(InvalidChannelReferenceError):
        normalize_channel_reference("not a channel")


def test_normalize_timezone_accepts_utc_and_iana() -> None:
    assert normalize_timezone("utc") == "UTC"
    assert normalize_timezone("Europe/Berlin") == "Europe/Berlin"
    assert normalize_timezone("+5") == "UTC+5"
    assert normalize_timezone("UTC-3") == "UTC-3"


def test_timezone_info_supports_fixed_utc_offsets() -> None:
    assert timezone_info("+5").utcoffset(None).total_seconds() == 5 * 3600
    assert timezone_info("UTC-3").utcoffset(None).total_seconds() == -3 * 3600
    assert timezone_info("UTC") is timezone.utc


def test_normalize_timezone_rejects_invalid_timezone() -> None:
    with pytest.raises(InvalidTimezoneError):
        normalize_timezone("Mars/Olympus")
