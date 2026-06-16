"""Built-in editable task prompts for digest AI steps."""

from __future__ import annotations


def default_filter_task_prompt(language: str) -> str:
    """Return the user-editable default filter task prompt."""
    if language == "ru":
        return (
            "Исключай рекламу, промо, низкосигнальные репосты и шаблонные сообщения. "
            "Оставляй пост, если сомневаешься в его полезности."
        )
    return (
        "Exclude ads, promos, low-signal reposts, and boilerplate posts. "
        "Include the post if you are unsure whether it is useful."
    )


def default_summary_task_prompt(language: str) -> str:
    """Return the user-editable default summary task prompt."""
    if language == "ru":
        return (
            "Сделай краткие тезисы дайджеста на русском языке. "
            "Для каждого пункта передай только суть: что произошло, кто/что задействован, "
            "почему это важно или какой вывод следует. "
            "Пиши 1-2 короткими предложениями без вступления, оценок и воды. "
            "Сохраняй ключевые факты, числа, имена и причинно-следственные связи. "
            "Не добавляй информацию, которой нет в постах."
        )
    return (
        "Write brief digest notes in English. "
        "For each item, capture only the essence: what happened, who or what is involved, "
        "why it matters, or what conclusion follows. "
        "Use 1-2 short sentences with no intro, opinions, or filler. "
        "Keep key facts, numbers, names, and causal links. "
        "Do not add information that is not present in the posts."
    )
