"""BL-21 isolated experiment control plane.

Revision ID: 0016_experiment_control_plane
Revises: 0015_knowledge_retry_state
"""

from alembic import op
import sqlalchemy as sa


revision = "0016_experiment_control_plane"
down_revision = "0015_knowledge_retry_state"
branch_labels = None
depends_on = None


def _enum(name: str, *values: str) -> sa.Enum:
    return sa.Enum(*values, name=name)


def upgrade() -> None:
    campaign_status = _enum("experiment_campaign_status", "draft", "ready", "running", "completed", "failed", "cancelled")
    candidate_status = _enum("experiment_candidate_status", "planned", "running", "evaluated", "failed", "skipped")
    decision = _enum("experiment_promotion_decision", "insufficient_evidence", "failing", "passing_for_review", "promoted")
    op.create_table(
        "experiment_campaigns",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("campaign_key", sa.String(length=128), nullable=False),
        sa.Column("channel_sha256", sa.String(length=64), nullable=False),
        sa.Column("dataset_sha256", sa.String(length=64), nullable=False),
        sa.Column("source_snapshot_sha256", sa.String(length=64), nullable=False),
        sa.Column("source_snapshot_table_count", sa.Integer(), nullable=False),
        sa.Column("config_sha256", sa.String(length=64), nullable=False),
        sa.Column("policy_sha256", sa.String(length=64), nullable=False),
        sa.Column("resume_key", sa.String(length=64), nullable=False),
        sa.Column("budget_usd", sa.Numeric(precision=14, scale=6), nullable=False),
        sa.Column("status", campaign_status, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("campaign_key", name="uq_experiment_campaign_key"),
    )
    op.create_index("ix_experiment_campaign_channel_status", "experiment_campaigns", ["channel_sha256", "status"])
    op.create_table(
        "experiment_campaign_locks",
        sa.Column("channel_sha256", sa.String(length=64), primary_key=True),
        sa.Column("campaign_id", sa.Integer(), sa.ForeignKey("experiment_campaigns.id", ondelete="CASCADE"), nullable=False, unique=True),
        sa.Column("acquired_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_table(
        "experiment_candidates",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("campaign_id", sa.Integer(), sa.ForeignKey("experiment_campaigns.id", ondelete="CASCADE"), nullable=False),
        sa.Column("hypothesis_id", sa.String(length=128), nullable=False),
        sa.Column("config_sha256", sa.String(length=64), nullable=False),
        sa.Column("index_label", sa.String(length=128), nullable=False),
        sa.Column("status", candidate_status, nullable=False),
        sa.Column("failure_reason", sa.String(length=128)),
        sa.Column("dev_metrics", sa.JSON()),
        sa.Column("holdout_metrics", sa.JSON()),
        sa.Column("phase_percentiles", sa.JSON()),
        sa.Column("projected_cost_usd", sa.Numeric(precision=14, scale=6)),
        sa.Column("actual_cost_usd", sa.Numeric(precision=14, scale=6)),
        sa.Column("promotion_decision", decision),
        sa.Column("claimed_at", sa.DateTime(timezone=True)),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("campaign_id", "config_sha256", name="uq_experiment_candidate_campaign_config"),
    )
    op.create_index("ix_experiment_candidate_campaign_status", "experiment_candidates", ["campaign_id", "status"])


def downgrade() -> None:
    op.drop_index("ix_experiment_candidate_campaign_status", table_name="experiment_candidates")
    op.drop_table("experiment_candidates")
    op.drop_table("experiment_campaign_locks")
    op.drop_index("ix_experiment_campaign_channel_status", table_name="experiment_campaigns")
    op.drop_table("experiment_campaigns")
    for name, values in (
        ("experiment_promotion_decision", ("insufficient_evidence", "failing", "passing_for_review", "promoted")),
        ("experiment_candidate_status", ("planned", "running", "evaluated", "failed", "skipped")),
        ("experiment_campaign_status", ("draft", "ready", "running", "completed", "failed", "cancelled")),
    ):
        _enum(name, *values).drop(op.get_bind(), checkfirst=True)
