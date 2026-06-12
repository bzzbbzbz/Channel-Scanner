"""Centralized CSS selectors for t.me/s/* HTML parsing.

All selectors in one place so they can be updated if Telegram changes their HTML structure.
"""

SELECTORS = {
    "post": "div.tgme_widget_message",
    "post_id_attr": "data-post",
    "date_link": "a.tgme_widget_message_date",
    "author": "a.tgme_widget_message_owner_name",
    "content": "div.tgme_widget_message_text",
    "reply": "a.tgme_widget_message_reply",
    "reply_text_class": "js-message_reply_text",
    "time": "time",
    "datetime_attr": "datetime",
    "views": "span.tgme_widget_message_views",
    "reactions_div": "div.tgme_widget_message_reactions",
    "reaction_span": "span.tgme_reaction",
    "emoji": "i.emoji",
    "link_preview": "a.tgme_widget_message_link_preview",
    "link_preview_title": "div.link_preview_title",
    "link_preview_site": "div.link_preview_site_name",
    "link_preview_description": "div.link_preview_description",
    "pagination": "a.tme_messages_more",
}
