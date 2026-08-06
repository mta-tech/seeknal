"""Tests for the Seeknal-owned Atlas online materialization path."""

from types import SimpleNamespace

import pytest

from seeknal.workflow.materialization.atlas_online import (
    AtlasOnlineMaterializationError,
    AtlasOnlineMaterializer,
)
from seeknal.workflow.materialization.atlas_online_config import (
    AtlasOnlineMaterializationConfig,
)


class _DuckResult:
    def __init__(self, *, rows=None, batches=None):
        self.rows = rows or []
        self.batches = list(batches or [])

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchmany(self, _size):
        return self.batches.pop(0) if self.batches else []


class _DuckConnection:
    def execute(self, query):
        if query.startswith("DESCRIBE"):
            return _DuckResult(
                rows=[
                    ("customer_id", "VARCHAR"),
                    ("age", "INTEGER"),
                    ("observed_at", "TIMESTAMP"),
                ]
            )
        if query.startswith("SELECT MIN"):
            return _DuckResult(rows=[(None, None)])
        if query.startswith("SELECT *"):
            return _DuckResult(
                batches=[
                    [("c-1", 42, None), ("c-2", 35, None)],
                    [],
                ]
            )
        raise AssertionError(query)


class _PostgresCursor:
    def __init__(self):
        self.statements = []
        self._fetchone = (False,)

    def execute(self, query, params=None):
        rendered = str(query)
        self.statements.append((rendered, params))
        if "SELECT EXISTS" in rendered:
            self._fetchone = (False,)

    def fetchone(self):
        return self._fetchone


class _PostgresConnection:
    def __init__(self):
        self.autocommit = True
        self.cursor_instance = _PostgresCursor()
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def cursor(self):
        return self.cursor_instance

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


def _config(**overrides):
    values = {
        "type": "atlas_online",
        "connection": "atlas_feature_store",
        "table": "customer_profile",
        "entity_keys": ["customer_id"],
        "event_time_column": "observed_at",
        "revision": "a" * 64,
        "definition_sha": "b" * 64,
        "schema_sha": "c" * 64,
        "publish_run_id": "run-1",
    }
    values.update(overrides)
    return AtlasOnlineMaterializationConfig.model_validate(values)


def test_config_rejects_schema_qualified_table():
    with pytest.raises(ValueError, match="SQL identifier"):
        _config(table="feature_online.customer_profile")


def test_materializer_swaps_live_table_and_records_ledger(monkeypatch):
    postgres = _PostgresConnection()
    copied = []
    monkeypatch.setattr(
        "psycopg2.extras.execute_values",
        lambda cursor, query, values, page_size: copied.extend(values),
    )
    pg_config = SimpleNamespace(
        host="localhost",
        port=5432,
        database="atlas",
        user="seeknal_feature_materializer",
        password="secret",
        sslmode="require",
        connect_timeout=10,
    )
    materializer = AtlasOnlineMaterializer(
        pg_config,
        _config(),
        connect=lambda **_kwargs: postgres,
    )

    result = materializer.materialize(
        _DuckConnection(), "feature_group.customer_profile"
    )

    assert result.success is True
    assert result.row_count == 2
    assert result.revision == "a" * 64
    assert len(copied) == 2
    assert postgres.committed is True
    assert postgres.rolled_back is False
    assert postgres.closed is True
    statements = "\n".join(item[0] for item in postgres.cursor_instance.statements)
    assert "pg_advisory_xact_lock" in statements
    assert "CREATE TABLE" in statements
    assert "ALTER TABLE" in statements
    assert "_online_publications" in statements


def test_materializer_rejects_reserved_source_columns():
    class ReservedDuck(_DuckConnection):
        def execute(self, query):
            if query.startswith("DESCRIBE"):
                return _DuckResult(
                    rows=[
                        ("customer_id", "VARCHAR"),
                        ("definition_sha", "VARCHAR"),
                    ]
                )
            return super().execute(query)

    pg_config = SimpleNamespace()
    with pytest.raises(
        AtlasOnlineMaterializationError,
        match="reserved metadata column",
    ):
        AtlasOnlineMaterializer(
            pg_config,
            _config(event_time_column=None),
            connect=lambda **_kwargs: None,
        ).materialize(ReservedDuck(), "feature_group.customer_profile")
