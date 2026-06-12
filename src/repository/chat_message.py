"""Repository for persisted assistant chat context."""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.chat_message import ChatMessage


class ChatMessageRepository:
    """Read and write recent chat messages for assistant context."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add_message(
        self,
        *,
        user_id: int,
        chat_id: int,
        role: str,
        text: str,
        metadata: dict | None = None,
    ) -> ChatMessage:
        message = ChatMessage(
            user_id=user_id,
            chat_id=chat_id,
            role=role,
            text=text,
            message_metadata=metadata,
        )
        self._session.add(message)
        await self._session.flush()
        return message

    async def list_recent_for_user(self, user_id: int, limit: int = 30) -> list[ChatMessage]:
        stmt = (
            select(ChatMessage)
            .where(ChatMessage.user_id == user_id)
            .order_by(ChatMessage.created_at.desc(), ChatMessage.id.desc())
            .limit(limit)
        )
        result = await self._session.execute(stmt)
        return list(reversed(result.scalars().all()))

    async def list_recent_digests(self, user_id: int, limit: int = 10) -> list[ChatMessage]:
        stmt = (
            select(ChatMessage)
            .where(ChatMessage.user_id == user_id, ChatMessage.role == "digest")
            .order_by(ChatMessage.created_at.desc(), ChatMessage.id.desc())
            .limit(limit)
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())
