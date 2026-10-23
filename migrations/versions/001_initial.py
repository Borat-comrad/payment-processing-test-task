"""Payments and transactional outbox."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "payments",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("amount", sa.Numeric(18, 2), nullable=False),
        sa.Column("currency", sa.String(3), nullable=False),
        sa.Column("description", sa.String(1000), nullable=False),
        sa.Column("metadata", postgresql.JSONB(), nullable=False),
        sa.Column("status", sa.String(10), nullable=False, server_default="pending"),
        sa.Column("idempotency_key", sa.String(200), nullable=False, unique=True),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("webhook_url", sa.String(2048), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("processed_at", sa.DateTime(timezone=True)),
        sa.Column("webhook_delivered_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint("amount > 0", name="positive_amount"),
        sa.CheckConstraint("currency IN ('RUB', 'USD', 'EUR')", name="known_currency"),
        sa.CheckConstraint("status IN ('pending', 'succeeded', 'failed')", name="known_status"),
    )
    op.create_table(
        "outbox",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("payment_id", sa.Uuid(), sa.ForeignKey("payments.id"), nullable=False),
        sa.Column("kind", sa.String(10), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column(
            "available_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("published_at", sa.DateTime(timezone=True)),
        sa.Column("handled_at", sa.DateTime(timezone=True)),
        sa.Column("error", sa.String(200)),
        sa.CheckConstraint("kind IN ('process', 'dead')", name="known_event_kind"),
        sa.CheckConstraint("attempt BETWEEN 1 AND 3", name="valid_attempt"),
        sa.UniqueConstraint("payment_id", "kind", "attempt", name="unique_payment_attempt"),
    )
    op.create_index(
        "outbox_ready", "outbox", ["available_at"], postgresql_where=sa.text("published_at IS NULL")
    )


def downgrade():
    op.drop_table("outbox")
    op.drop_table("payments")
