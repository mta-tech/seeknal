"""
Tests for SourceExecutor Iceberg incremental reads with partition pruning.

Tests cover:
- Full scan on first run (no time_column / no watermark)
- Incremental read with partition-pruned WHERE clause
- Custom query skips pruning even with time_column + watermark
- Invalid time_column raises ExecutorExecutionError
- Iceberg metadata (snapshot_id, last_watermark) flows into result.metadata
- NULL handling in WHERE clause
"""

from __future__ import annotations

import json
import pytest
from typing import Any, Dict
from unittest.mock import patch, MagicMock

from seeknal.dag.manifest import Node, NodeType
from seeknal.workflow.executors.base import (
    ExecutionContext,
    ExecutorExecutionError,
)
from seeknal.workflow.executors.source_executor import SourceExecutor


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

ICEBERG_ENV = {
    "KEYCLOAK_TOKEN_URL": "http://keycloak/token",
    "KEYCLOAK_CLIENT_ID": "seeknal",
    "KEYCLOAK_CLIENT_SECRET": "secret",
    "AWS_ENDPOINT_URL": "http://minio:9000",
    "AWS_ACCESS_KEY_ID": "minioadmin",
    "AWS_SECRET_ACCESS_KEY": "password",
    "AWS_REGION": "us-east-1",
    "LAKEKEEPER_URL": "",
}

TOKEN_RESPONSE = json.dumps({"access_token": "test-token"}).encode()


@pytest.fixture
def execution_context(tmp_path):
    """Create a minimal execution context for tests."""
    return ExecutionContext(
        project_name="test_project",
        workspace_path=tmp_path,
        target_path=tmp_path / "target",
        dry_run=False,
        verbose=False,
    )


def _make_iceberg_node(
    table="atlas.ns.tbl",
    params=None,
    freshness=None,
):
    """Create a stub Iceberg source Node."""
    config: Dict[str, Any] = {
        "source": "iceberg",
        "table": table,
    }
    if params is not None:
        config["params"] = params
    else:
        config["params"] = {"catalog_uri": "http://lakekeeper:8181", "warehouse": "wh"}
    if freshness is not None:
        config["freshness"] = freshness
    return Node(
        id="source.my_table",
        name="my_table",
        node_type=NodeType.SOURCE,
        config=config,
        description="Test iceberg source",
        tags=["iceberg"],
    )


def _mock_urlopen():
    """Return a mock for urllib.request.urlopen that returns a valid OAuth token."""
    mock_response = MagicMock()
    mock_response.read.return_value = TOKEN_RESPONSE
    return mock_response


def _make_mock_con(row_count=100, describe_cols=None, max_watermark=None, snapshot_id=None):
    """
    Build a mock DuckDB connection that routes responses based on SQL patterns.

    Args:
        row_count: Value returned for COUNT(*) queries
        describe_cols: List of (col_name, col_type) tuples for DESCRIBE
        max_watermark: Value returned for MAX(time_column) queries
        snapshot_id: Value returned for iceberg_snapshots queries
    """
    mock_con = MagicMock()

    def execute_side_effect(sql):
        result = MagicMock()
        sql_upper = sql.upper().strip()

        if "DESCRIBE" in sql_upper:
            if describe_cols is not None:
                result.fetchall.return_value = describe_cols
            else:
                result.fetchall.return_value = [
                    ("id", "INTEGER"), ("event_time", "TIMESTAMP"), ("value", "DOUBLE")
                ]
            return result

        if "COUNT(*)" in sql_upper:
            result.fetchone.return_value = (row_count,)
            return result

        if "MAX(" in sql_upper:
            result.fetchone.return_value = (max_watermark,)
            return result

        if "ICEBERG_SNAPSHOTS" in sql_upper:
            if snapshot_id is not None:
                result.fetchone.return_value = (snapshot_id,)
            else:
                result.fetchone.return_value = None
            return result

        if "ATTACH" in sql_upper:
            return result

        # Default: return a mock result
        result.fetchone.return_value = None
        result.fetchall.return_value = []
        return result

    mock_con.execute.side_effect = execute_side_effect
    return mock_con


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestIcebergFullScanFirstRun:
    """No time_column / no watermark -> full SELECT * without WHERE."""

    def test_iceberg_full_scan_first_run(self, execution_context):
        """First run with no freshness config produces a full scan."""
        node = _make_iceberg_node(table="atlas.ns.tbl")
        executor = SourceExecutor(node, execution_context)

        mock_con = _make_mock_con(row_count=50)
        mock_resp = _mock_urlopen()

        with patch.dict("os.environ", ICEBERG_ENV, clear=True), \
             patch("urllib.request.urlopen", return_value=mock_resp):
            row_count = executor._load_iceberg(mock_con, "atlas.ns.tbl", {"catalog_uri": "http://lakekeeper:8181"})

        assert row_count == 50

        # Verify the CREATE VIEW SQL does NOT contain WHERE
        calls = [str(c) for c in mock_con.execute.call_args_list]
        view_calls = [c for c in calls if "CREATE OR REPLACE VIEW" in c]
        assert len(view_calls) == 1
        assert "WHERE" not in view_calls[0]

        # No watermark metadata should be set (no time_column)
        assert hasattr(executor, '_iceberg_result_metadata')
        assert executor._iceberg_result_metadata["last_watermark"] is None


class TestIcebergIncrementalWithWatermark:
    """time_column + last_watermark -> WHERE clause injected."""

    def test_iceberg_incremental_with_watermark(self, execution_context):
        """Incremental read includes partition-pruning WHERE clause."""
        node = _make_iceberg_node(
            table="atlas.ns.tbl",
            freshness={"time_column": "event_time"},
        )
        executor = SourceExecutor(node, execution_context)

        mock_con = _make_mock_con(
            row_count=25,
            max_watermark="2026-01-15 12:00:00",
            snapshot_id=9999,
        )
        mock_resp = _mock_urlopen()

        with patch.dict("os.environ", ICEBERG_ENV, clear=True), \
             patch("urllib.request.urlopen", return_value=mock_resp):
            row_count = executor._load_iceberg(
                mock_con, "atlas.ns.tbl",
                {"catalog_uri": "http://lakekeeper:8181"},
                time_column="event_time",
                last_watermark="2026-01-01 00:00:00",
            )

        assert row_count == 25

        # Verify WHERE clause was injected
        # Extract the actual SQL string from the call args (not the repr)
        view_calls = [
            c.args[0] for c in mock_con.execute.call_args_list
            if c.args and "CREATE OR REPLACE VIEW" in str(c.args[0])
        ]
        assert len(view_calls) == 1
        view_sql = view_calls[0]
        assert "WHERE" in view_sql
        assert "CAST('2026-01-01 00:00:00' AS TIMESTAMP)" in view_sql
        assert '"event_time"' in view_sql


class TestIcebergCustomQuerySkipsPruning:
    """params.query set -> no WHERE injection even with time_column + watermark."""

    def test_iceberg_custom_query_skips_pruning(self, execution_context):
        """Custom query in params skips partition pruning."""
        node = _make_iceberg_node(
            table="atlas.ns.tbl",
            params={
                "catalog_uri": "http://lakekeeper:8181",
                "query": "SELECT id FROM atlas.ns.tbl WHERE id > 10",
            },
            freshness={"time_column": "event_time"},
        )
        executor = SourceExecutor(node, execution_context)

        mock_con = _make_mock_con(row_count=5)
        mock_resp = _mock_urlopen()

        with patch.dict("os.environ", ICEBERG_ENV, clear=True), \
             patch("urllib.request.urlopen", return_value=mock_resp):
            row_count = executor._load_iceberg(
                mock_con, "atlas.ns.tbl",
                {
                    "catalog_uri": "http://lakekeeper:8181",
                    "query": "SELECT id FROM atlas.ns.tbl WHERE id > 10",
                },
                time_column="event_time",
                last_watermark="2026-01-01 00:00:00",
            )

        assert row_count == 5

        # Verify the view SQL is a full scan (no extra WHERE for pruning)
        calls = [str(c) for c in mock_con.execute.call_args_list]
        view_calls = [c for c in calls if "CREATE OR REPLACE VIEW" in c]
        assert len(view_calls) == 1
        # Should NOT contain the incremental WHERE clause
        assert 'CAST(' not in view_calls[0]


class TestIcebergInvalidTimeColumn:
    """time_column not in DESCRIBE results -> ExecutorExecutionError."""

    def test_iceberg_invalid_time_column(self, execution_context):
        """Non-existent time_column raises ExecutorExecutionError."""
        node = _make_iceberg_node(
            table="atlas.ns.tbl",
            freshness={"time_column": "nonexistent_col"},
        )
        executor = SourceExecutor(node, execution_context)

        # DESCRIBE returns columns that do NOT include "nonexistent_col"
        mock_con = _make_mock_con(
            describe_cols=[("id", "INTEGER"), ("event_time", "TIMESTAMP"), ("value", "DOUBLE")],
        )
        mock_resp = _mock_urlopen()

        with patch.dict("os.environ", ICEBERG_ENV, clear=True), \
             patch("urllib.request.urlopen", return_value=mock_resp):
            with pytest.raises(ExecutorExecutionError, match="nonexistent_col.*not found"):
                executor._load_iceberg(
                    mock_con, "atlas.ns.tbl",
                    {"catalog_uri": "http://lakekeeper:8181"},
                    time_column="nonexistent_col",
                )


class TestIcebergMetadataInResult:
    """After execution, result.metadata contains iceberg_snapshot_id and last_watermark."""

    def test_iceberg_metadata_in_result(self, execution_context):
        """Full execute() flow produces Iceberg metadata in result."""
        node = _make_iceberg_node(
            table="atlas.ns.tbl",
            freshness={"time_column": "event_time"},
        )
        # Inject last watermark into execution context config
        execution_context.config["_iceberg_last_watermark"] = "2026-01-01 00:00:00"

        # Inject mock DuckDB connection directly (ExecutionContext is a dataclass)
        mock_con = _make_mock_con(
            row_count=30,
            max_watermark="2026-01-15 12:00:00",
            snapshot_id=7777,
        )
        execution_context.duckdb_connection = mock_con

        executor = SourceExecutor(node, execution_context)
        mock_resp = _mock_urlopen()

        with patch.dict("os.environ", ICEBERG_ENV, clear=True), \
             patch("urllib.request.urlopen", return_value=mock_resp):
            result = executor.execute()

        assert result.status.value == "success"
        assert result.metadata["iceberg_snapshot_id"] == "7777"
        assert result.metadata["last_watermark"] == "2026-01-15 12:00:00"
        assert result.metadata["iceberg_table_ref"] == "atlas.ns.tbl"
        assert result.metadata["source"] == "iceberg"


class TestIcebergNullHandling:
    """WHERE clause includes OR time_column IS NULL."""

    def test_iceberg_null_handling(self, execution_context):
        """Incremental WHERE clause includes IS NULL clause for safety."""
        node = _make_iceberg_node(
            table="atlas.ns.tbl",
            freshness={"time_column": "event_time"},
        )
        executor = SourceExecutor(node, execution_context)

        mock_con = _make_mock_con(row_count=10)
        mock_resp = _mock_urlopen()

        with patch.dict("os.environ", ICEBERG_ENV, clear=True), \
             patch("urllib.request.urlopen", return_value=mock_resp):
            executor._load_iceberg(
                mock_con, "atlas.ns.tbl",
                {"catalog_uri": "http://lakekeeper:8181"},
                time_column="event_time",
                last_watermark="2026-01-01 00:00:00",
            )

        # Verify IS NULL clause
        calls = [str(c) for c in mock_con.execute.call_args_list]
        view_calls = [c for c in calls if "CREATE OR REPLACE VIEW" in c]
        assert len(view_calls) == 1
        assert '"event_time" IS NULL' in view_calls[0]


class TestIcebergWatermarkFallback:
    """When MAX() returns None, last_watermark is preserved as fallback."""

    def test_watermark_falls_back_to_last(self, execution_context):
        """If new watermark is None, stored last_watermark is used."""
        node = _make_iceberg_node(table="atlas.ns.tbl")
        executor = SourceExecutor(node, execution_context)

        mock_con = _make_mock_con(row_count=0, max_watermark=None)
        mock_resp = _mock_urlopen()

        with patch.dict("os.environ", ICEBERG_ENV, clear=True), \
             patch("urllib.request.urlopen", return_value=mock_resp):
            executor._load_iceberg(
                mock_con, "atlas.ns.tbl",
                {"catalog_uri": "http://lakekeeper:8181"},
                time_column="event_time",
                last_watermark="2026-01-01 00:00:00",
            )

        # new_watermark is None (MAX returned None), so fallback to last_watermark
        assert executor._iceberg_result_metadata["last_watermark"] == "2026-01-01 00:00:00"
