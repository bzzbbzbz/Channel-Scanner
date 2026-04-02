"""HTML-to-Markdown conversion and view/reaction/link-preview parsers."""

from __future__ import annotations

import re
from typing import Optional

from bs4 import Tag
from markdownify import markdownify as md

from src.scraper.selectors import SELECTORS


def html_to_markdown(html_string: str) -> str:
    """Convert Telegram HTML to Markdown using markdownify."""
    result = md(html_string, strip=["img"]).strip()
    return result


def parse_views(views_str: str) -> int:
    """Parse Telegram views string like '234', '1.5K', '2.3M' to integer."""
    if not views_str:
        return 0

    s = views_str.strip().replace(",", "")
    s_upper = s.upper()

    if "K" in s_upper:
        return int(float(s_upper.replace("K", "")) * 1_000)
    if "M" in s_upper:
        return int(float(s_upper.replace("M", "")) * 1_000_000)
    return int(s)


def parse_reactions(reactions_div: Optional[Tag]) -> Optional[dict[str, int]]:
    """Extract emoji → count mapping from the reactions container.

    Returns ``None`` when the reactions div is missing (no reactions on post).
    """
    if reactions_div is None:
        return None

    reactions: dict[str, int] = {}
    for span in reactions_div.select(SELECTORS["reaction_span"]):
        # Emoji lives in <i class="emoji"><b>🔥</b></i>
        emoji_tag = span.find("i", class_="emoji")
        if emoji_tag:
            b_tag = emoji_tag.find("b")
            emoji = b_tag.get_text() if b_tag else emoji_tag.get_text()
        else:
            continue

        # The full text of the span is emoji + count, e.g. "🔥16"
        full_text = span.get_text()
        # Remove emoji characters to get the count portion
        count_str = re.sub(r"[^\d]", "", full_text)
        count = int(count_str) if count_str else 0

        if emoji and count > 0:
            reactions[emoji] = count

    return reactions if reactions else None


def parse_link_preview(preview_element: Optional[Tag]) -> Optional[dict[str, str]]:
    """Extract link preview fields from the preview anchor element.

    Returns ``None`` when there is no link preview on the post.
    """
    if preview_element is None:
        return None

    result: dict[str, str] = {}

    title = preview_element.find(class_="link_preview_title")
    if title:
        result["title"] = title.get_text(strip=True)

    site_name = preview_element.find(class_="link_preview_site_name")
    if site_name:
        result["site_name"] = site_name.get_text(strip=True)

    description = preview_element.find(class_="link_preview_description")
    if description:
        result["description"] = description.get_text(strip=True)

    href = preview_element.get("href")
    if href:
        result["url"] = href

    return result if result else None
