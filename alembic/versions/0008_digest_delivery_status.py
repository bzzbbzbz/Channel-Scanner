"""digest delivery status

Revision ID: 0008_digest_delivery_status
Revises: 0007_assistant_cron_and_chat_history
Create Date: 2026-06-11 00:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0008_digest_delivery_status"
down_revision: Union[str, Sequence[str], None] = "0007_assistant_cron_and_chat_history"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "digest_deliveries",
        sa.Column("status", sa.String(length=32), server_default="delivered", nullable=False),
    )
    op.add_column("digest_deliveries", sa.Column("skip_reason", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("digest_deliveries", "skip_reason")
    op.drop_column("digest_deliveries", "status")
