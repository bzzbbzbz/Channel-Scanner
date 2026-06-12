"""user language and subscription baseline

Revision ID: 0004_user_language_and_subscription_baseline
Revises: 0003_digest_delivery_v1
Create Date: 2026-04-27 00:10:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0004_user_language_and_subscription_baseline"
down_revision: Union[str, Sequence[str], None] = "0003_digest_delivery_v1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("users", sa.Column("language", sa.String(length=8), server_default="ru", nullable=False))
    op.add_column(
        "subscriptions",
        sa.Column("subscribed_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
    )
    op.execute("UPDATE subscriptions SET subscribed_at = created_at WHERE created_at IS NOT NULL")


def downgrade() -> None:
    op.drop_column("subscriptions", "subscribed_at")
    op.drop_column("users", "language")
