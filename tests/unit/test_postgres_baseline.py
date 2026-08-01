from sqlalchemy.dialects.postgresql import dialect
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.knowledge.experiments import CampaignState, CandidateState, PromotionDecision
from src.models.channel import Channel
from src.models.knowledge import ExperimentCampaign, ExperimentCandidate
from src.models.post import Post
from src.models.user import DigestFormat, SummaryMode
from src.repository.post import PostRepository
from src.scraper.parser import ParsedPost


def test_channel_status_enum_uses_postgres_safe_values() -> None:
    assert Channel.__table__.c.status.type.enums == ["active", "error", "paused"]


def test_experiment_enums_bind_existing_postgres_lowercase_labels() -> None:
    cases = (
        (ExperimentCampaign.__table__.c.status.type, CampaignState.DRAFT, "draft"),
        (ExperimentCandidate.__table__.c.status.type, CandidateState.PLANNED, "planned"),
        (ExperimentCandidate.__table__.c.promotion_decision.type, PromotionDecision.PASSING_FOR_REVIEW, "passing_for_review"),
    )

    for enum_type, member, expected in cases:
        bind = enum_type.bind_processor(dialect())
        assert bind is not None
        assert bind(member) == expected


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
