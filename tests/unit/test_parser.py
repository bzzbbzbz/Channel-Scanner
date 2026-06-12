"""Unit tests for src.scraper.parser — HTML post parsing and page parsing."""

from __future__ import annotations

from src.scraper.parser import ParsedPost, parse_page, parse_post

# ---------------------------------------------------------------------------
# HTML fixtures matching real t.me/s/* structure
# ---------------------------------------------------------------------------

COMPLETE_POST_HTML = """
<div class="tgme_widget_message" data-post="testchannel/12345">
  <a class="tgme_widget_message_owner_name">Test Author</a>
  <div class="tgme_widget_message_text"><b>Hello</b> <i>world</i></div>
  <time datetime="2024-06-15T12:00:00+00:00">12:00</time>
  <span class="tgme_widget_message_views">1.5K</span>
  <div class="tgme_widget_message_reactions">
    <span class="tgme_reaction"><i class="emoji"><b>🔥</b></i>16</span>
    <span class="tgme_reaction"><i class="emoji"><b>👍</b></i>42</span>
  </div>
  <a class="tgme_widget_message_link_preview" href="https://example.com/art">
    <div class="link_preview_title">Some Article</div>
    <div class="link_preview_site_name">Example</div>
    <div class="link_preview_description">Desc</div>
  </a>
  <a class="tgme_widget_message_date" href="https://t.me/testchannel/12345">12:00</a>
</div>
"""

POST_NO_REACTIONS_HTML = """
<div class="tgme_widget_message" data-post="testchannel/12346">
  <a class="tgme_widget_message_owner_name">Test Author</a>
  <div class="tgme_widget_message_text">Simple text</div>
  <time datetime="2024-06-15T13:00:00+00:00">13:00</time>
  <span class="tgme_widget_message_views">234</span>
  <a class="tgme_widget_message_date" href="https://t.me/testchannel/12346">13:00</a>
</div>
"""

POST_NO_LINK_PREVIEW_HTML = """
<div class="tgme_widget_message" data-post="testchannel/12347">
  <a class="tgme_widget_message_owner_name">Test Author</a>
  <div class="tgme_widget_message_text">Text without preview</div>
  <time datetime="2024-06-15T14:00:00+00:00">14:00</time>
  <span class="tgme_widget_message_views">500</span>
  <div class="tgme_widget_message_reactions">
    <span class="tgme_reaction"><i class="emoji"><b>❤️</b></i>8</span>
  </div>
  <a class="tgme_widget_message_date" href="https://t.me/testchannel/12347">14:00</a>
</div>
"""

POST_NO_VIEWS_HTML = """
<div class="tgme_widget_message" data-post="testchannel/12348">
  <a class="tgme_widget_message_owner_name">Test Author</a>
  <div class="tgme_widget_message_text">No views post</div>
  <time datetime="2024-06-15T15:00:00+00:00">15:00</time>
  <a class="tgme_widget_message_date" href="https://t.me/testchannel/12348">15:00</a>
</div>
"""

POST_MISSING_DATETIME_HTML = """
<div class="tgme_widget_message" data-post="testchannel/99999">
  <div class="tgme_widget_message_text">Missing datetime</div>
  <a class="tgme_widget_message_date" href="https://t.me/testchannel/99999">15:00</a>
</div>
"""

POST_MISSING_DATA_POST_HTML = """
<div class="tgme_widget_message">
  <div class="tgme_widget_message_text">No data-post attribute</div>
  <time datetime="2024-06-15T16:00:00+00:00">16:00</time>
  <a class="tgme_widget_message_date" href="https://t.me/testchannel/99998">16:00</a>
</div>
"""

POST_WITH_REPLY_HTML = """
<div class="tgme_widget_message" data-post="bankrollo/61957">
  <div class="tgme_widget_message_bubble">
    <a class="tgme_widget_message_reply user-color-default" href="https://t.me/bankrollo/61954">
      <div class="tgme_widget_message_text js-message_reply_text" dir="auto">
        Трамп объявил о сделке между США и Ираном.
      </div>
    </a>
    <div class="tgme_widget_message_text js-message_text" dir="auto">
      Израиль и Иран отрицают существование мирной сделки.
      <a href="https://t.me/bankrollo" target="_blank">@bankrollo</a>
    </div>
    <time datetime="2026-06-11T20:06:00+00:00">20:06</time>
    <a class="tgme_widget_message_date" href="https://t.me/bankrollo/61957">20:06</a>
  </div>
</div>
"""

PAGE_MULTI_POST_HTML = """
<html><body>
<div class="tgme_widget_message" data-post="chan/100">
  <a class="tgme_widget_message_owner_name">Chan</a>
  <div class="tgme_widget_message_text">First</div>
  <time datetime="2024-06-15T10:00:00+00:00">10:00</time>
  <span class="tgme_widget_message_views">10</span>
  <a class="tgme_widget_message_date" href="https://t.me/chan/100">10:00</a>
</div>
<div class="tgme_widget_message" data-post="chan/99">
  <a class="tgme_widget_message_owner_name">Chan</a>
  <div class="tgme_widget_message_text">Second</div>
  <time datetime="2024-06-15T09:00:00+00:00">09:00</time>
  <span class="tgme_widget_message_views">5</span>
  <a class="tgme_widget_message_date" href="https://t.me/chan/99">09:00</a>
</div>
<a class="tme_messages_more" href="https://t.me/s/chan?before=99">Load more</a>
</body></html>
"""

PAGE_NO_POSTS_HTML = """
<html><body>
<div class="tgme_channel_header">Channel Header</div>
</body></html>
"""

PAGE_NO_PAGINATION_HTML = """
<html><body>
<div class="tgme_widget_message" data-post="chan/50">
  <a class="tgme_widget_message_owner_name">Chan</a>
  <div class="tgme_widget_message_text">Only post</div>
  <time datetime="2024-06-15T08:00:00+00:00">08:00</time>
  <a class="tgme_widget_message_date" href="https://t.me/chan/50">08:00</a>
</div>
</body></html>
"""


# ---------------------------------------------------------------------------
# parse_post tests
# ---------------------------------------------------------------------------


def _parse_fixture(html: str) -> "bs4.element.Tag":
    """Parse HTML string and return the first tgme_widget_message div."""
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    return soup.find("div", class_="tgme_widget_message")


class TestParsePost:
    """Tests for parse_post with various HTML fixtures."""

    def test_complete_post_all_fields(self) -> None:
        el = _parse_fixture(COMPLETE_POST_HTML)
        result = parse_post(el)
        assert result is not None
        assert isinstance(result, ParsedPost)
        assert result.post_id == 12345
        assert result.channel_username == "testchannel"
        assert "**Hello**" in result.content
        assert "*world*" in result.content
        assert result.datetime == "2024-06-15T12:00:00+00:00"
        assert result.views == 1500
        assert result.reactions == {"🔥": 16, "👍": 42}
        assert result.author == "Test Author"
        assert result.link_preview is not None
        assert result.link_preview["title"] == "Some Article"

    def test_post_no_reactions(self) -> None:
        el = _parse_fixture(POST_NO_REACTIONS_HTML)
        result = parse_post(el)
        assert result is not None
        assert result.reactions is None
        assert result.views == 234

    def test_post_no_link_preview(self) -> None:
        el = _parse_fixture(POST_NO_LINK_PREVIEW_HTML)
        result = parse_post(el)
        assert result is not None
        assert result.link_preview is None
        assert result.reactions == {"❤️": 8}

    def test_post_no_views(self) -> None:
        el = _parse_fixture(POST_NO_VIEWS_HTML)
        result = parse_post(el)
        assert result is not None
        assert result.views is None

    def test_missing_datetime_returns_none(self) -> None:
        el = _parse_fixture(POST_MISSING_DATETIME_HTML)
        result = parse_post(el)
        assert result is None

    def test_missing_data_post_returns_none(self) -> None:
        el = _parse_fixture(POST_MISSING_DATA_POST_HTML)
        result = parse_post(el)
        assert result is None

    def test_data_post_attribute_parsing(self) -> None:
        """Verify 'channel_name/12345' → post_id=12345, channel_username='channel_name'."""
        html = """
        <div class="tgme_widget_message" data-post="my_channel/99999">
          <div class="tgme_widget_message_text">content</div>
          <time datetime="2024-01-01T00:00:00+00:00">00:00</time>
          <a class="tgme_widget_message_date" href="https://t.me/my_channel/99999">00:00</a>
        </div>
        """
        el = _parse_fixture(html)
        result = parse_post(el)
        assert result is not None
        assert result.post_id == 99999
        assert result.channel_username == "my_channel"

    def test_reply_context_is_excluded_from_content(self) -> None:
        el = _parse_fixture(POST_WITH_REPLY_HTML)
        result = parse_post(el)
        assert result is not None
        assert "Израиль и Иран отрицают" in result.content
        assert "Трамп объявил" not in result.content


class TestParsePage:
    """Tests for parse_page — multi-post pages with pagination."""

    def test_multiple_posts_with_pagination(self) -> None:
        posts, next_url = parse_page(PAGE_MULTI_POST_HTML)
        assert len(posts) == 2
        assert posts[0].post_id == 100
        assert posts[1].post_id == 99
        assert next_url == "https://t.me/s/chan?before=99"

    def test_no_posts_returns_empty(self) -> None:
        posts, next_url = parse_page(PAGE_NO_POSTS_HTML)
        assert posts == []
        assert next_url is None

    def test_no_pagination(self) -> None:
        posts, next_url = parse_page(PAGE_NO_PAGINATION_HTML)
        assert len(posts) == 1
        assert posts[0].post_id == 50
        assert next_url is None

    def test_empty_html(self) -> None:
        posts, next_url = parse_page("")
        assert posts == []
        assert next_url is None
