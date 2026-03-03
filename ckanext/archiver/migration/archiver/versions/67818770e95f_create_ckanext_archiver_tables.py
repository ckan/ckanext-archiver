"""Create ckanext-archiver tables

Revision ID: 67818770e95f
Revises:
Create Date: 2026-01-09 13:19:51.948465

"""

from alembic import op
import sqlalchemy as sa
from datetime import datetime
from uuid import uuid4


# revision identifiers, used by Alembic.
revision = "67818770e95f"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    engine = op.get_bind()
    inspector = sa.inspect(engine)
    tables = inspector.get_table_names()

    if "archival" not in tables:
        op.create_table(
            "archival",
            sa.Column("id", sa.UnicodeText, primary_key=True, default=uuid4),
            sa.Column("package_id", sa.UnicodeText, nullable=False, index=True),
            sa.Column("resource_id", sa.UnicodeText, nullable=False, index=True),
            sa.Column("resource_timestamp", sa.DateTime),  # key to resource_revision
            # Details of the latest archival attempt
            sa.Column("status_id", sa.Integer),
            sa.Column("is_broken", sa.Boolean),  # Based on status_id. None = not sure
            sa.Column("reason", sa.UnicodeText),  # Extra detail explaining the status (cannot be translated)
            sa.Column("url_redirected_to", sa.UnicodeText),
            # Details of last successful archival
            sa.Column("cache_filepath", sa.UnicodeText),
            sa.Column("cache_url", sa.UnicodeText),
            sa.Column("size", sa.BigInteger, default=0),
            sa.Column("mimetype", sa.UnicodeText),
            sa.Column("hash", sa.UnicodeText),
            sa.Column("etag", sa.UnicodeText),
            sa.Column("last_modified", sa.UnicodeText),
            # History
            sa.Column("first_failure", sa.DateTime),
            sa.Column("last_success", sa.DateTime),
            sa.Column("failure_count", sa.Integer, default=0),
            sa.Column("created", sa.DateTime, default=datetime.now),
            sa.Column("updated", sa.DateTime),
        )


def downgrade():
    op.drop_table("archival")
