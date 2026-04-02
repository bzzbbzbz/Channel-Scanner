"""Unit tests for src.scraper.converters — HTML-to-Markdown, views, reactions, link previews."""

from __future__ import annotations

from bs4 import BeautifulSoup

from src.scraper.converters import (
    html_to_markdown,
    parse_link_preview,
    parse_reactions,
    parse_views,
)


class TestHtmlToMarkdown:
    """Tests for html_to_markdown converter."""

    def test_bold(self) -> None:
        html = "<b>hello world</b>"
        assert html_to_markdown(html) == "**hello world**"

    def test_italic(self) -> None:
        html = "<i>emphasis</i>"
        assert html_to_markdown(html) == "*emphasis*"

    def test_link(self) -> None:
        html = '<a href="https://example.com">click here</a>'
        result = html_to_markdown(html)
        assert "[click here](https://example.com)" in result

    def test_code(self) -> None:
        html = "<code>print('hi')</code>"
        result = html_to_markdown(html)
        assert "`print('hi')`" in result

    def test_pre_code_block(self) -> None:
        html = "<pre><code>def foo():\n    pass\n</code></pre>"
        result = html_to_markdown(html)
        assert "foo" in result

    def test_combined_formatting(self) -> None:
        html = "<b>bold</b> and <i>italic</i> and <a href='http://x.com'>link</a>"
        result = html_to_markdown(html)
        assert "**bold**" in result
        assert "*italic*" in result
        assert "[link](http://x.com)" in result

    def test_strips_outer_whitespace(self) -> None:
        html = "  <b>hello</b>  "
        assert html_to_markdown(html) == "**hello**"

    def test_empty_string(self) -> None:
        assert html_to_markdown("") == ""


class TestParseViews:
    """Tests for parse_views — handles K/M suffixes."""

    def test_plain_number(self) -> None:
        assert parse_views("234") == 234

    def test_k_suffix(self) -> None:
        assert parse_views("1.5K") == 1500

    def test_k_suffix_integer(self) -> None:
        assert parse_views("12K") == 12000

    def test_m_suffix(self) -> None:
        assert parse_views("2.3M") == 2_300_000

    def test_m_suffix_small(self) -> None:
        assert parse_views("1.1M") == 1_100_000

    def test_empty_string(self) -> None:
        assert parse_views("") == 0

    def test_with_commas(self) -> None:
        assert parse_views("1,234") == 1234

    def test_whitespace(self) -> None:
        assert parse_views("  1.5K  ") == 1500


class TestParseReactions:
    """Tests for parse_reactions — emoji → count extraction."""

    def _make_reactions_html(self, emoji: str, count: int) -> str:
        return f'<span class="tgme_reaction"><i class="emoji"><b>{emoji}</b></i>{count}</span>'

    def test_multiple_reactions(self) -> None:
        html = (
            '<div class="tgme_widget_message_reactions">'
            + self._make_reactions_html("🔥", 16)
            + self._make_reactions_html("👍", 42)
            + "</div>"
        )
        soup = BeautifulSoup(html, "html.parser")
        div = soup.find("div")
        result = parse_reactions(div)
        assert result == {"🔥": 16, "👍": 42}

    def test_none_input(self) -> None:
        assert parse_reactions(None) is None

    def test_empty_div(self) -> None:
        html = '<div class="tgme_widget_message_reactions"></div>'
        soup = BeautifulSoup(html, "html.parser")
        div = soup.find("div")
        assert parse_reactions(div) is None

    def test_single_reaction(self) -> None:
        html = (
            '<div class="tgme_widget_message_reactions">'
            + self._make_reactions_html("❤️", 100)
            + "</div>"
        )
        soup = BeautifulSoup(html, "html.parser")
        div = soup.find("div")
        assert parse_reactions(div) == {"❤️": 100}


class TestParseLinkPreview:
    """Tests for parse_link_preview — title, site, description extraction."""

    def test_all_fields(self) -> None:
        html = (
            '<a class="tgme_widget_message_link_preview" href="https://example.com/article">'
            '<div class="link_preview_title">Article Title</div>'
            '<div class="link_preview_site_name">Example Site</div>'
            '<div class="link_preview_description">A brief description</div>'
            "</a>"
        )
        soup = BeautifulSoup(html, "html.parser")
        el = soup.find("a")
        result = parse_link_preview(el)
        assert result is not None
        assert result["title"] == "Article Title"
        assert result["site_name"] == "Example Site"
        assert result["description"] == "A brief description"
        assert result["url"] == "https://example.com/article"

    def test_missing_optional_fields(self) -> None:
        html = (
            '<a class="tgme_widget_message_link_preview" href="https://example.com">'
            '<div class="link_preview_site_name">Example Site</div>'
            "</a>"
        )
        soup = BeautifulSoup(html, "html.parser")
        el = soup.find("a")
        result = parse_link_preview(el)
        assert result is not None
        assert "title" not in result
        assert result["site_name"] == "Example Site"
        assert "description" not in result

    def test_none_input(self) -> None:
        assert parse_link_preview(None) is None

    def test_empty_anchor(self) -> None:
        html = '<a class="tgme_widget_message_link_preview" href="https://example.com"></a>'
        soup = BeautifulSoup(html, "html.parser")
        el = soup.find("a")
        result = parse_link_preview(el)
        assert result is not None
        assert result["url"] == "https://example.com"
