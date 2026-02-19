"""
Tests for SourceExecutor iceberg source type.

Tests the _load_iceberg method for loading data from Iceberg tables
via Lakekeeper REST catalog.
"""

import json
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

from seeknal.workflow.executors.base import (
    ExecutionContext,
    ExecutorExecutionError,
    ExecutorValidationError,
)
from seeknal.workflow.executors.source_executor import SourceExecutor
from seeknal.dag.manifest import Node, NodeType


@pytest.fixture
def execution_context(tmp_path):
    """Create an execution context."""
    return ExecutionContext(
        project_name="test_project",
        workspace_path=tmp_path,
        target_path=tmp_path / "target",
        dry_run=False,
        verbose=False,
    )


def _make_iceberg_node(table="atlas.my_ns.my_table", params=None):
    """Helper to create an iceberg source node."""
    config = {
        "source": "iceberg",
        "table": table,
    }
    if params:
        config["params"] = params
    return Node(
        id="source.my_table",
        name="my_table",
        node_type=NodeType.SOURCE,
        config=config,
        description="Test iceberg source",
        tags=["iceberg"],
    )


class TestIcebergSourceValidation:
    """Test validation for iceberg source type."""

    def test_iceberg_in_supported_sources(self, execution_context):
        """Test that 'iceberg' is a supported source type."""
        node = _make_iceberg_node()
        executor = SourceExecutor(node, execution_context)
        # validate() should not raise for iceberg type
        executor.validate()

    def test_iceberg_skips_file_path_validation(self, execution_context):
        """Test that iceberg sources don't validate file paths."""
        node = _make_iceberg_node(table="atlas.ns.tbl")
        executor = SourceExecutor(node, execution_context)
        # Should not raise even though table is not a valid file path
        executor.validate()


class TestIcebergTableNameValidation:
    """Test table name validation for iceberg sources."""

    def test_rejects_1_part_table_name(self, execution_context):
        """Test that single-part table name is rejected."""
        node = _make_iceberg_node(table="just_a_table")
        executor = SourceExecutor(node, execution_context)

        with pytest.raises(ExecutorExecutionError, match="3-part format"):
            executor.execute()

    def test_rejects_2_part_table_name(self, execution_context):
        """Test that 2-part table name is rejected."""
        node = _make_iceberg_node(table="ns.table")
        executor = SourceExecutor(node, execution_context)

        with pytest.raises(ExecutorExecutionError, match="3-part format"):
            executor.execute()

    def test_rejects_4_part_table_name(self, execution_context):
        """Test that 4-part table name is rejected."""
        node = _make_iceberg_node(table="a.b.c.d")
        executor = SourceExecutor(node, execution_context)

        with pytest.raises(ExecutorExecutionError, match="3-part format"):
            executor.execute()


class TestIcebergConnectionConfig:
    """Test connection configuration for iceberg sources."""

    def test_requires_catalog_uri(self, execution_context):
        """Test that catalog_uri is required."""
        node = _make_iceberg_node(
            table="atlas.ns.tbl",
            params={}  # No catalog_uri
        )
        executor = SourceExecutor(node, execution_context)

        with patch.dict("os.environ", {}, clear=True):
            with pytest.raises(ExecutorExecutionError, match="catalog_uri"):
                executor.execute()

    def test_catalog_uri_from_params(self, execution_context):
        """Test that catalog_uri can come from params."""
        node = _make_iceberg_node(
            table="atlas.ns.tbl",
            params={"catalog_uri": "http://localhost:8181"}
        )
        executor = SourceExecutor(node, execution_context)

        # Will fail at OAuth step but proves catalog_uri was accepted
        env = {
            "LAKEKEEPER_URL": "",
            "KEYCLOAK_TOKEN_URL": "",
            "KEYCLOAK_CLIENT_ID": "",
            "KEYCLOAK_CLIENT_SECRET": "",
        }
        with patch.dict("os.environ", env):
            with pytest.raises(ExecutorExecutionError, match="OAuth2"):
                executor.execute()

    def test_catalog_uri_from_env(self, execution_context):
        """Test that catalog_uri falls back to LAKEKEEPER_URL env var."""
        node = _make_iceberg_node(
            table="atlas.ns.tbl",
            params={}  # No catalog_uri in params
        )
        executor = SourceExecutor(node, execution_context)

        env = {
            "LAKEKEEPER_URL": "http://lakekeeper:8181",
            "KEYCLOAK_TOKEN_URL": "",
            "KEYCLOAK_CLIENT_ID": "",
            "KEYCLOAK_CLIENT_SECRET": "",
        }
        with patch.dict("os.environ", env):
            # Will fail at OAuth step but proves env var was read
            with pytest.raises(ExecutorExecutionError, match="OAuth2"):
                executor.execute()

    def test_requires_oauth_credentials(self, execution_context):
        """Test that OAuth2 credentials are required."""
        node = _make_iceberg_node(
            table="atlas.ns.tbl",
            params={"catalog_uri": "http://localhost:8181"}
        )
        executor = SourceExecutor(node, execution_context)

        env = {
            "KEYCLOAK_TOKEN_URL": "",
            "KEYCLOAK_CLIENT_ID": "",
            "KEYCLOAK_CLIENT_SECRET": "",
            "AWS_ENDPOINT_URL": "",
        }
        with patch.dict("os.environ", env):
            with pytest.raises(ExecutorExecutionError, match="OAuth2"):
                executor.execute()

    def test_default_warehouse_name(self, execution_context):
        """Test that warehouse defaults to seeknal-warehouse."""
        node = _make_iceberg_node(
            table="atlas.ns.tbl",
            params={"catalog_uri": "http://localhost:8181"}
        )
        executor = SourceExecutor(node, execution_context)

        mock_con = MagicMock()
        mock_execute = MagicMock()
        mock_execute.fetchone.return_value = (10,)
        mock_con.execute.return_value = mock_execute

        # Mock urllib to return a token
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps(
            {"access_token": "test-token"}
        ).encode()

        env = {
            "KEYCLOAK_TOKEN_URL": "http://keycloak/token",
            "KEYCLOAK_CLIENT_ID": "client",
            "KEYCLOAK_CLIENT_SECRET": "secret",
            "AWS_ENDPOINT_URL": "http://minio:9000",
            "AWS_ACCESS_KEY_ID": "key",
            "AWS_SECRET_ACCESS_KEY": "secret",
            "AWS_REGION": "us-east-1",
        }

        with patch.dict("os.environ", env):
            with patch("urllib.request.urlopen", return_value=mock_response):
                # Call _load_iceberg directly with mock connection
                row_count = executor._load_iceberg(
                    mock_con, "atlas.ns.tbl",
                    {"catalog_uri": "http://localhost:8181"}
                )

                # Check that ATTACH used 'seeknal-warehouse'
                attach_calls = [
                    str(c) for c in mock_con.execute.call_args_list
                    if "ATTACH" in str(c)
                ]
                assert any(
                    "seeknal-warehouse" in c for c in attach_calls
                ), f"Expected 'seeknal-warehouse' in ATTACH calls: {attach_calls}"


class TestIcebergLoadMocked:
    """Test _load_iceberg with mocked DuckDB and OAuth."""

    def test_successful_load(self, execution_context):
        """Test successful iceberg table load with all mocks."""
        node = _make_iceberg_node(
            table="atlas.test_ns.orders",
            params={"catalog_uri": "http://lakekeeper:8181"}
        )
        executor = SourceExecutor(node, execution_context)

        mock_con = MagicMock()
        # Make execute().fetchone() return row count
        mock_fetchone = MagicMock(return_value=(42,))
        mock_execute = MagicMock()
        mock_execute.fetchone = mock_fetchone
        mock_con.execute.return_value = mock_execute

        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps(
            {"access_token": "my-token"}
        ).encode()

        env = {
            "KEYCLOAK_TOKEN_URL": "http://keycloak/token",
            "KEYCLOAK_CLIENT_ID": "client",
            "KEYCLOAK_CLIENT_SECRET": "secret",
            "AWS_ENDPOINT_URL": "http://minio:9000",
            "AWS_ACCESS_KEY_ID": "minioadmin",
            "AWS_SECRET_ACCESS_KEY": "password",
            "AWS_REGION": "us-east-1",
        }

        with patch.dict("os.environ", env, clear=True):
            with patch("urllib.request.urlopen", return_value=mock_response):
                row_count = executor._load_iceberg(mock_con, "atlas.test_ns.orders", {"catalog_uri": "http://lakekeeper:8181"})

        assert row_count == 42

        # Verify key SQL calls were made
        calls = [str(c) for c in mock_con.execute.call_args_list]
        assert any("INSTALL httpfs" in c for c in calls)
        assert any("INSTALL iceberg" in c for c in calls)
        assert any("s3_endpoint" in c for c in calls)
        assert any("ATTACH" in c for c in calls)
        assert any("CREATE SCHEMA" in c for c in calls)
        assert any("CREATE OR REPLACE VIEW" in c for c in calls)
        assert any("test_ns.orders" in c for c in calls)

    def test_catalog_url_appends_catalog_path(self, execution_context):
        """Test that /catalog is appended to base URL."""
        node = _make_iceberg_node(
            table="atlas.ns.tbl",
            params={"catalog_uri": "http://lakekeeper:8181"}
        )
        executor = SourceExecutor(node, execution_context)

        mock_con = MagicMock()
        mock_execute = MagicMock()
        mock_execute.fetchone.return_value = (5,)
        mock_con.execute.return_value = mock_execute

        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps(
            {"access_token": "token"}
        ).encode()

        env = {
            "KEYCLOAK_TOKEN_URL": "http://keycloak/token",
            "KEYCLOAK_CLIENT_ID": "c",
            "KEYCLOAK_CLIENT_SECRET": "s",
            "AWS_ENDPOINT_URL": "http://minio:9000",
            "AWS_ACCESS_KEY_ID": "k",
            "AWS_SECRET_ACCESS_KEY": "s",
            "AWS_REGION": "us-east-1",
        }

        with patch.dict("os.environ", env, clear=True):
            with patch("urllib.request.urlopen", return_value=mock_response):
                executor._load_iceberg(mock_con, "atlas.ns.tbl", {"catalog_uri": "http://lakekeeper:8181"})

        # Check ATTACH used /catalog endpoint
        attach_calls = [
            str(c) for c in mock_con.execute.call_args_list
            if "ATTACH" in str(c)
        ]
        assert any("lakekeeper:8181/catalog" in c for c in attach_calls)

    def test_skips_s3_config_when_no_endpoint(self, execution_context):
        """Test that S3 config is skipped when AWS_ENDPOINT_URL is empty."""
        node = _make_iceberg_node(
            table="atlas.ns.tbl",
            params={"catalog_uri": "http://lakekeeper:8181"}
        )
        executor = SourceExecutor(node, execution_context)

        mock_con = MagicMock()
        mock_execute = MagicMock()
        mock_execute.fetchone.return_value = (3,)
        mock_con.execute.return_value = mock_execute

        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps(
            {"access_token": "token"}
        ).encode()

        env = {
            "KEYCLOAK_TOKEN_URL": "http://keycloak/token",
            "KEYCLOAK_CLIENT_ID": "c",
            "KEYCLOAK_CLIENT_SECRET": "s",
            "AWS_ENDPOINT_URL": "",  # No S3 endpoint
            "AWS_ACCESS_KEY_ID": "",
            "AWS_SECRET_ACCESS_KEY": "",
            "AWS_REGION": "us-east-1",
        }

        with patch.dict("os.environ", env, clear=True):
            with patch("urllib.request.urlopen", return_value=mock_response):
                executor._load_iceberg(mock_con, "atlas.ns.tbl", {"catalog_uri": "http://lakekeeper:8181"})

        # S3 config calls should NOT be present
        calls = [str(c) for c in mock_con.execute.call_args_list]
        assert not any("s3_endpoint" in c for c in calls)

    def test_handles_already_attached_catalog(self, execution_context):
        """Test that already-attached catalog is handled gracefully."""
        node = _make_iceberg_node(
            table="atlas.ns.tbl",
            params={"catalog_uri": "http://lakekeeper:8181"}
        )
        executor = SourceExecutor(node, execution_context)

        call_count = 0

        def mock_execute_side_effect(sql):
            nonlocal call_count
            call_count += 1
            mock_result = MagicMock()
            mock_result.fetchone.return_value = (10,)
            if "ATTACH" in sql:
                raise Exception("Catalog 'atlas' already exists")
            return mock_result

        mock_con = MagicMock()
        mock_con.execute.side_effect = mock_execute_side_effect

        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps(
            {"access_token": "token"}
        ).encode()

        env = {
            "KEYCLOAK_TOKEN_URL": "http://keycloak/token",
            "KEYCLOAK_CLIENT_ID": "c",
            "KEYCLOAK_CLIENT_SECRET": "s",
            "AWS_ENDPOINT_URL": "http://minio:9000",
            "AWS_ACCESS_KEY_ID": "k",
            "AWS_SECRET_ACCESS_KEY": "s",
            "AWS_REGION": "us-east-1",
        }

        with patch.dict("os.environ", env, clear=True):
            with patch("urllib.request.urlopen", return_value=mock_response):
                row_count = executor._load_iceberg(
                    mock_con, "atlas.ns.tbl",
                    {"catalog_uri": "http://lakekeeper:8181"}
                )

        # Should succeed despite ATTACH error
        assert row_count == 10

    def test_custom_warehouse_from_params(self, execution_context):
        """Test custom warehouse name from params."""
        node = _make_iceberg_node(
            table="atlas.ns.tbl",
            params={
                "catalog_uri": "http://lakekeeper:8181",
                "warehouse": "my-custom-warehouse",
            }
        )
        executor = SourceExecutor(node, execution_context)

        mock_con = MagicMock()
        mock_execute = MagicMock()
        mock_execute.fetchone.return_value = (7,)
        mock_con.execute.return_value = mock_execute

        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps(
            {"access_token": "token"}
        ).encode()

        env = {
            "KEYCLOAK_TOKEN_URL": "http://keycloak/token",
            "KEYCLOAK_CLIENT_ID": "c",
            "KEYCLOAK_CLIENT_SECRET": "s",
            "AWS_ENDPOINT_URL": "http://minio:9000",
            "AWS_ACCESS_KEY_ID": "k",
            "AWS_SECRET_ACCESS_KEY": "s",
            "AWS_REGION": "us-east-1",
        }

        with patch.dict("os.environ", env, clear=True):
            with patch("urllib.request.urlopen", return_value=mock_response):
                executor._load_iceberg(
                    mock_con, "atlas.ns.tbl",
                    {"catalog_uri": "http://lakekeeper:8181", "warehouse": "my-custom-warehouse"}
                )

        attach_calls = [
            str(c) for c in mock_con.execute.call_args_list
            if "ATTACH" in str(c)
        ]
        assert any("my-custom-warehouse" in c for c in attach_calls)


class TestIcebergDryRun:
    """Test dry-run mode for iceberg sources."""

    def test_dry_run_returns_early(self, execution_context):
        """Test that dry-run skips actual loading."""
        execution_context.dry_run = True
        node = _make_iceberg_node(table="atlas.ns.tbl")
        executor = SourceExecutor(node, execution_context)

        result = executor.execute()
        assert result.is_dry_run
        assert result.row_count == 0
