"""Focused coverage for clone-only lexical BL-21 candidates."""

from datetime import datetime, timezone

import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import async_sessionmaker

from src.knowledge.experiment_retriever import (
    CanonicalLexicalCandidateRetriever,
    LexicalCandidateMode,
    russian_fts_statement,
)
from src.knowledge.experiments import ExperimentError
from src.models.channel import Channel
from src.models.knowledge import KnowledgeChannel, KnowledgeChannelState
from src.models.post import Post


@pytest.mark.asyncio
async def test_lexical_candidates_use_only_one_ready_canonical_catalog_channel_and_parent_ids(engine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(timezone.utc)
    async with session_factory() as session:
        catalog = Channel(username="catalog")
        outside = Channel(username="outside")
        session.add_all([catalog, outside])
        await session.flush()
        session.add_all([
            KnowledgeChannel(channel_id=catalog.id, state=KnowledgeChannelState.READY),
            Post(channel_id=catalog.id, post_id=10, content="alpha", datetime=now),
            Post(channel_id=catalog.id, post_id=11, content="alpha alpha exact phrase", datetime=now),
            Post(channel_id=outside.id, post_id=12, content="alpha alpha alpha exact phrase", datetime=now),
        ])
        await session.commit()

    async with session_factory() as session:
        retriever = CanonicalLexicalCandidateRetriever(session, channel_username="@catalog")
        assert await retriever.resolve_channel() == catalog.id
        baseline = await retriever.retrieve(mode=LexicalCandidateMode.TOKEN_ILIKE, query="alpha")
        exact = await retriever.retrieve(mode=LexicalCandidateMode.EXACT_SHORT_CIRCUIT, query="exact phrase")

    assert baseline.telegram_post_ids == (11, 10)
    assert exact.telegram_post_ids == (11,)
    assert isinstance(baseline.telegram_post_ids[0], int)
    assert "alpha" not in repr(baseline)


@pytest.mark.asyncio
async def test_russian_fts_fails_closed_on_sqlite(engine) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as session:
        retriever = CanonicalLexicalCandidateRetriever(session, channel_username="catalog")
        with pytest.raises(ExperimentError, match="requires PostgreSQL"):
            await retriever.retrieve(mode=LexicalCandidateMode.RUSSIAN_FTS, query="поиск")


def test_russian_fts_sql_is_parameterized_parent_scoped_and_deterministic() -> None:
    statement = russian_fts_statement(channel_id=7, username="catalog", query="русский поиск", limit=5)
    compiled = statement.compile(dialect=postgresql.dialect())
    sql = str(compiled)

    assert "to_tsvector" in sql
    assert "plainto_tsquery" in sql
    assert "ts_rank_cd" in sql
    assert "knowledge_representations" not in sql
    assert "knowledge_documents" not in sql
    assert "русский поиск" not in sql
    assert compiled.params["fts_query"] == "русский поиск"
    assert "posts.datetime DESC, posts.id DESC" in sql
