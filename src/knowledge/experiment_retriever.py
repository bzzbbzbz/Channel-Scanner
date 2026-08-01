"""Clone-only canonical-post lexical candidates for BL-21 experiments."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from time import monotonic

from sqlalchemy import bindparam, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.knowledge.experiments import ExperimentError
from src.knowledge.repository import normalize_username
from src.models.channel import Channel
from src.models.knowledge import KnowledgeChannel, KnowledgeChannelState
from src.models.post import Post


class LexicalCandidateMode(str, Enum):
    TOKEN_ILIKE = "token_ilike"
    RUSSIAN_FTS = "russian_fts"
    EXACT_SHORT_CIRCUIT = "exact_short_circuit"


@dataclass(frozen=True, slots=True)
class LexicalCandidateResult:
    """Only canonical Telegram parent IDs leave the retriever."""

    telegram_post_ids: tuple[int, ...]
    lexical_ms: float


class CanonicalLexicalCandidateRetriever:
    """Read only canonical ``Post.content`` in one ready catalog channel.

    This adapter deliberately has no access to documents, representations, vector
    clients, provider clients, or write operations. The resolved channel scope is
    repeated in each query so a candidate cannot escape the approved catalog row.
    """

    def __init__(self, session: AsyncSession, *, channel_username: str, result_limit: int = 5, pool_limit: int = 30) -> None:
        self._session = session
        self._username = normalize_username(channel_username)
        if not self._username:
            raise ExperimentError("channel username must not be empty")
        if result_limit < 1 or pool_limit < result_limit:
            raise ExperimentError("lexical candidate limits are invalid")
        self._result_limit = result_limit
        self._pool_limit = pool_limit
        self._channel_id: int | None = None

    async def resolve_channel(self) -> int:
        """Resolve exactly one ready catalog entry before any retrieval."""
        statement = (
            select(KnowledgeChannel.channel_id)
            .join(Channel, Channel.id == KnowledgeChannel.channel_id)
            .where(
                Channel.username == self._username,
                KnowledgeChannel.state == KnowledgeChannelState.READY,
            )
        )
        channel_id = (await self._session.execute(statement)).scalar_one_or_none()
        if channel_id is None:
            raise ExperimentError("approved ready catalog channel was not found")
        self._channel_id = int(channel_id)
        return self._channel_id

    async def retrieve(self, *, mode: LexicalCandidateMode, query: str) -> LexicalCandidateResult:
        if not isinstance(mode, LexicalCandidateMode):
            raise ExperimentError("candidate mode is not declared safe")
        if mode == LexicalCandidateMode.RUSSIAN_FTS and (self._session.bind.dialect.name if self._session.bind else "") != "postgresql":
            raise ExperimentError("Russian FTS candidate requires PostgreSQL")
        if self._channel_id is None:
            await self.resolve_channel()
        assert self._channel_id is not None
        started = monotonic()
        if mode == LexicalCandidateMode.TOKEN_ILIKE:
            rows = await self._token_ilike(query)
        elif mode == LexicalCandidateMode.RUSSIAN_FTS:
            rows = await self._russian_fts(query)
        else:
            rows = await self._exact_short_circuit(query)
        # Each query starts at the unique Post row; preserve that parent-only form.
        return LexicalCandidateResult(tuple(int(row) for row in rows), (monotonic() - started) * 1000)

    def _scope_statement(self):
        assert self._channel_id is not None
        return (
            select(Post.post_id, Post.content, Post.datetime, Post.id)
            .join(KnowledgeChannel, KnowledgeChannel.channel_id == Post.channel_id)
            .join(Channel, Channel.id == Post.channel_id)
            .where(
                Post.channel_id == self._channel_id,
                Channel.username == self._username,
                KnowledgeChannel.state == KnowledgeChannelState.READY,
            )
        )

    async def _token_ilike(self, query: str) -> list[int]:
        """Match the production token-ILIKE candidate pool and deterministic rank."""
        terms = [term for term in query.split() if len(term) > 1][:8]
        pool = self._scope_statement()
        if terms:
            pool = pool.where(or_(*(Post.content.ilike(f"%{term}%") for term in terms)))
        pool = pool.order_by(Post.datetime.desc(), Post.id.desc()).limit(self._pool_limit).subquery()
        lowered_query = query.lower()
        if lowered_query:
            occurrences = (
                func.length(func.lower(pool.c.content))
                - func.length(func.replace(func.lower(pool.c.content), bindparam("lexical_query", lowered_query), ""))
            ) / len(lowered_query)
        else:
            occurrences = 0
        statement = (
            select(pool.c.post_id)
            .order_by(occurrences.desc(), pool.c.datetime.desc(), pool.c.id.desc())
            .limit(self._result_limit)
        )
        return list((await self._session.execute(statement)).scalars())

    async def _exact_short_circuit(self, query: str) -> list[int]:
        if query.strip():
            statement = (
                self._scope_statement()
                .where(Post.content.ilike(f"%{query}%"))
                .order_by(Post.datetime.desc(), Post.id.desc())
                .limit(self._result_limit)
            )
            exact = list((await self._session.execute(statement)).scalars())
            if exact:
                return [int(post_id) for post_id in exact]
        return await self._token_ilike(query)

    async def _russian_fts(self, query: str) -> list[int]:
        dialect = self._session.bind.dialect.name if self._session.bind else ""
        if dialect != "postgresql":
            raise ExperimentError("Russian FTS candidate requires PostgreSQL")
        if not query.strip():
            return []
        statement = russian_fts_statement(
            channel_id=self._channel_id,
            username=self._username,
            query=query,
            limit=self._result_limit,
        )
        return list((await self._session.execute(statement)).scalars())


def russian_fts_statement(*, channel_id: int, username: str, query: str, limit: int):
    """Build a parameterized PostgreSQL Russian FTS query without index changes."""
    if channel_id < 1 or limit < 1 or not username or not query:
        raise ExperimentError("Russian FTS statement inputs are invalid")
    tsquery = func.plainto_tsquery("russian", bindparam("fts_query", query))
    vector = func.to_tsvector("russian", Post.content)
    rank = func.ts_rank_cd(vector, tsquery)
    return (
        select(Post.post_id)
        .join(KnowledgeChannel, KnowledgeChannel.channel_id == Post.channel_id)
        .join(Channel, Channel.id == Post.channel_id)
        .where(
            Post.channel_id == channel_id,
            Channel.username == bindparam("channel_username", username),
            KnowledgeChannel.state == KnowledgeChannelState.READY,
            vector.op("@@")(tsquery),
        )
        .order_by(rank.desc(), Post.datetime.desc(), Post.id.desc())
        .limit(limit)
    )
