"""cascade reliable run state when a subscription is deleted

Revision ID: 0024_reliable_delete_cascades
Revises: 0023_dead_letters_and_replay
"""

from alembic import op


revision = "0024_reliable_delete_cascades"
down_revision = "0023_dead_letters_and_replay"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint(
        "fk_digest_outbox_messages_run_id_digest_runs",
        "digest_outbox_messages",
        type_="foreignkey",
    )
    op.create_foreign_key(
        "fk_digest_outbox_messages_run_id_digest_runs",
        "digest_outbox_messages",
        "digest_runs",
        ["run_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.drop_constraint(
        "fk_digest_runs_subscription_id_subscriptions",
        "digest_runs",
        type_="foreignkey",
    )
    op.create_foreign_key(
        "fk_digest_runs_subscription_id_subscriptions",
        "digest_runs",
        "subscriptions",
        ["subscription_id"],
        ["id"],
        ondelete="CASCADE",
    )


def downgrade() -> None:
    op.drop_constraint(
        "fk_digest_runs_subscription_id_subscriptions",
        "digest_runs",
        type_="foreignkey",
    )
    op.create_foreign_key(
        "fk_digest_runs_subscription_id_subscriptions",
        "digest_runs",
        "subscriptions",
        ["subscription_id"],
        ["id"],
    )
    op.drop_constraint(
        "fk_digest_outbox_messages_run_id_digest_runs",
        "digest_outbox_messages",
        type_="foreignkey",
    )
    op.create_foreign_key(
        "fk_digest_outbox_messages_run_id_digest_runs",
        "digest_outbox_messages",
        "digest_runs",
        ["run_id"],
        ["id"],
    )
