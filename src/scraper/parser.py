"""HTML post parser — extracts structured data from t.me/s/* pages.

Uses centralized selectors from selectors.py and converter helpers from converters.py.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from bs4 import BeautifulSoup

from src.scraper.converters import (
    html_to_markdown,
    parse_link_preview,
    parse_reactions,
    parse_views,
)
from src.scraper.selectors import SELECTORS


@dataclass
class ParsedPost:
    """Structured representation of a parsed Telegram post."""

    post_id: int
    channel_username: str
    content: str  # Markdown
    datetime: str  # ISO 8601
    views: Optional[int] = None
    reactions: Optional[dict[str, int]] = None
    author: Optional[str] = None
    link_preview: Optional[dict[str, str]] = None


def parse_post(post_element: "bs4.element.Tag") -> Optional[ParsedPost]:
    """Parse a single ``tgme_widget_message`` div into a :class:`ParsedPost`.

    Returns ``None`` when essential fields (post_id, datetime) are missing.
    """
    # --- post_id & channel_username from data-post attribute ---
    data_post = post_element.get(SELECTORS["post_id_attr"], "")
    if not data_post or "/" not in str(data_post):
        return None

    parts = str(data_post).split("/", 1)
    channel_username = parts[0]
    try:
        post_id = int(parts[1])
    except (ValueError, IndexError):
        return None

    # --- datetime ---
    time_elem = post_element.find("time")
    if not time_elem:
        return None
    datetime_str = time_elem.get(SELECTORS["datetime_attr"], "")
    if not datetime_str:
        return None

    # --- content (HTML → Markdown) ---
    content_div = post_element.select_one(SELECTORS["content"])
    if content_div:
        content = html_to_markdown(str(content_div))
    else:
        content = ""

    # --- views ---
    views_elem = post_element.select_one(SELECTORS["views"])
    views: Optional[int] = None
    if views_elem:
        try:
            views = parse_views(views_elem.get_text(strip=True))
        except (ValueError, TypeError):
            views = None

    # --- reactions ---
    reactions_div = post_element.select_one(SELECTORS["reactions_div"])
    reactions = parse_reactions(reactions_div)

    # --- author ---
    author_elem = post_element.select_one(SELECTORS["author"])
    author = author_elem.get_text(strip=True) if author_elem else None

    # --- link preview ---
    preview_elem = post_element.select_one(SELECTORS["link_preview"])
    link_preview = parse_link_preview(preview_elem)

    return ParsedPost(
        post_id=post_id,
        channel_username=channel_username,
        content=content,
        datetime=datetime_str,
        views=views,
        reactions=reactions,
        author=author,
        link_preview=link_preview,
    )


def parse_page(html: str) -> tuple[list[ParsedPost], Optional[str]]:
    """Parse an HTML page from ``t.me/s/*`` into posts + optional next-page URL.

    Returns ``(posts, next_url)`` where *next_url* is the pagination link
    (``?before=XX``) or ``None`` when there are no older posts.
    """
    soup = BeautifulSoup(html, "html.parser")

    post_elements = soup.select(SELECTORS["post"])

    posts: list[ParsedPost] = []
    for el in post_elements:
        parsed = parse_post(el)
        if parsed is not None:
            posts.append(parsed)

    # --- pagination ---
    next_url: Optional[str] = None
    pagination_el = soup.find("a", class_="tme_messages_more")
    if pagination_el:
        href = pagination_el.get("href")
        if href:
            next_url = str(href)

    return posts, next_url
