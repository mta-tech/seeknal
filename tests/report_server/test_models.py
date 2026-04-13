from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest
from sqlalchemy.engine import Engine

from seeknal.report_server.models import Publish, _report_server_metadata, get_engine, get_session


def test_metadata_isolated_from_seeknal_models() -> None:
    import seeknal.models as seeknal_models

    assert _report_server_metadata is not seeknal_models.metadata, (
        "report_server metadata must not be shared with seeknal.models singleton"
    )


def test_publish_table_round_trip(test_engine: Engine) -> None:
    with get_session(test_engine) as session:
        pub = Publish(
            slug="ab1234",
            report_name="My Report",
            report_title="Title Here",
            owner_secret_hash="deadbeef" * 8,
            owner_secret_salt="cafe" * 4,
            created_at=datetime.now(timezone.utc),
            asset_relpath="assets/ab1234",
            byte_size=1024,
        )
        session.add(pub)
        session.commit()

    with get_session(test_engine) as session:
        retrieved = session.get(Publish, "ab1234")
        assert retrieved is not None
        assert retrieved.report_name == "My Report"
        assert retrieved.report_title == "Title Here"
        assert retrieved.byte_size == 1024
        assert retrieved.asset_relpath == "assets/ab1234"
        assert retrieved.revoked_at is None


def test_get_session_context_manager(test_engine: Engine) -> None:
    with get_session(test_engine) as session:
        pub = Publish(
            slug="xyz999",
            report_name="ctx test",
            report_title=None,
            owner_secret_hash="aa" * 32,
            owner_secret_salt="bb" * 8,
            created_at=datetime.now(timezone.utc),
            asset_relpath="assets/xyz999",
            byte_size=512,
        )
        session.add(pub)
        session.commit()
        retrieved = session.get(Publish, "xyz999")
        assert retrieved is not None
