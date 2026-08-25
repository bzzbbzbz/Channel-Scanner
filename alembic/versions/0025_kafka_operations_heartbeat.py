"""add reliability role heartbeats

Revision ID: 0025_kafka_operations_heartbeat
Revises: 0024_reliable_delete_cascades
"""

from alembic import op
import sqlalchemy as sa


revision = "0025_kafka_operations_heartbeat"
down_revision = "0024_reliable_delete_cascades"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "reliability_role_heartbeats",
        sa.Column("role", sa.String(32), nullable=False),
        sa.Column("instance_id", sa.String(128), nullable=False),
        sa.Column("state", sa.String(16), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("stopped_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error_code", sa.String(128), nullable=True),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "role IN ('scheduler', 'outbox-relay', 'digest-worker', 'telegram-delivery-worker')",
            name="ck_reliability_role_heartbeats_role",
        ),
        sa.CheckConstraint(
            "state IN ('starting', 'ready', 'stopped', 'failed')",
            name="ck_reliability_role_heartbeats_state",
        ),
        sa.CheckConstraint(
            "length(instance_id) BETWEEN 1 AND 128",
            name="ck_reliability_role_heartbeats_instance_length",
        ),
        sa.CheckConstraint(
            "last_error_code IS NULL OR length(last_error_code) BETWEEN 1 AND 128",
            name="ck_reliability_role_heartbeats_error_length",
        ),
        sa.PrimaryKeyConstraint("role", name="pk_reliability_role_heartbeats"),
    )


def downgrade() -> None:
    op.drop_table("reliability_role_heartbeats")
