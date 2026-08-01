"""Public-channel knowledge catalog, retrieval, and indexing services."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.knowledge.service import KnowledgeService

__all__ = ["KnowledgeService"]


def __getattr__(name: str):
    if name == "KnowledgeService":
        from src.knowledge.service import KnowledgeService

        return KnowledgeService
    raise AttributeError(name)
