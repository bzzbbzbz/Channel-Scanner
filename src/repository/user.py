"""Repository for Telegram bot users and their top-level settings."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.user import User


class UserRepository:
    """Manage persisted Telegram users."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_by_telegram_user_id(self, telegram_user_id: int) -> Optional[User]:
        stmt = select(User).where(User.telegram_user_id == telegram_user_id)
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_by_id(self, user_id: int) -> Optional[User]:
        stmt = select(User).where(User.id == user_id)
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def list_users(self) -> list[User]:
        stmt = select(User).order_by(User.id.asc())
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def upsert_user(
        self,
        *,
        telegram_user_id: int,
        chat_id: int,
        chat_type: str,
        username: Optional[str],
        first_name: Optional[str],
        last_name: Optional[str],
        default_timezone: str,
        default_language: str,
    ) -> User:
        user = await self.get_by_telegram_user_id(telegram_user_id)
        if user is None:
            user = User(
                telegram_user_id=telegram_user_id,
                chat_id=chat_id,
                chat_type=chat_type,
                username=username,
                first_name=first_name,
                last_name=last_name,
                timezone=default_timezone,
                language=default_language,
            )
            self._session.add(user)
            await self._session.flush()
            return user

        user.chat_id = chat_id
        user.chat_type = chat_type
        user.username = username
        user.first_name = first_name
        user.last_name = last_name
        user.updated_at = datetime.now(timezone.utc)
        await self._session.flush()
        return user

    async def update_timezone(self, user: User, timezone_name: str) -> User:
        user.timezone = timezone_name
        user.updated_at = datetime.now(timezone.utc)
        await self._session.flush()
        return user

    async def update_language(self, user: User, language: str) -> User:
        user.language = language
        user.updated_at = datetime.now(timezone.utc)
        await self._session.flush()
        return user
