from sqlalchemy.dialects.postgresql import dialect
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.models.channel import Channel
from src.models.post import Post
from src.models.user import DigestFormat, SummaryMode
from src.repository.post import PostRepository
from src.scraper.parser import ParsedPost


def test_channel_status_enum_uses_postgres_safe_values() -> None:
    assert Channel.__table__.c.status.type.enums == ["active", "error", "paused"]


def test_postgresql_upsert_compiles_on_conflict_clause() -> None:
    post = ParsedPost(
        post_id=1,
        channel_username="testchannel",
        content="hello",
        datetime="2026-01-15T12:00:00+00:00",
    )
    row = PostRepository._post_to_dict(1, post)

    compiled = str(
        pg_insert(Post)
        .values([row])
        .on_conflict_do_nothing(index_elements=["channel_id", "post_id"])
        .compile(dialect=dialect())
    )

    assert "ON CONFLICT (channel_id, post_id) DO NOTHING" in compiled


def test_user_enums_match_expected_values() -> None:
    assert list(DigestFormat) == [DigestFormat.SHORT, DigestFormat.SUMMARY]
    assert list(SummaryMode) == [SummaryMode.BRIEF, SummaryMode.DETAILED, SummaryMode.CUSTOM]
