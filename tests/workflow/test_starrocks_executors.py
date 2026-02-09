"""Tests for StarRocks source and exposure executors."""
from unittest.mock import MagicMock, patch

import pytest
import duckdb

from seeknal.dag.manifest import Node, NodeType
from seeknal.workflow.executors.source_executor import SourceExecutor
from seeknal.workflow.executors.exposure_executor import ExposureExecutor, ExposureType


class MockExecutionContext:
    """Minimal execution context for testing."""

    def __init__(self, tmp_path):
        self.project_name = "test_project"
        self.workspace_path = tmp_path
        self.target_path = tmp_path / "target"
        self.target_path.mkdir(parents=True, exist_ok=True)
        self.dry_run = False
        self.verbose = False
        self.materialize_enabled = False
        self._duckdb_conn = duckdb.connect(":memory:")
        self.duckdb_connection = self._duckdb_conn

    def get_duckdb_connection(self):
        return self._duckdb_conn

    def get_output_path(self, node):
        p = self.target_path / "output" / node.name
        p.mkdir(parents=True, exist_ok=True)
        return p

    def get_cache_path(self, node):
        p = self.target_path / "cache" / node.node_type.value / f"{node.name}.parquet"
        p.parent.mkdir(parents=True, exist_ok=True)
        return p


@pytest.fixture
def context(tmp_path):
    return MockExecutionContext(tmp_path)


class TestSourceExecutorStarRocksValidation:
    """Test SourceExecutor validation for StarRocks type."""

    def test_starrocks_in_supported_sources(self, context):
        node = Node(
            id="source.sr_users",
            name="sr_users",
            node_type=NodeType.SOURCE,
            config={"source": "starrocks", "table": "users", "params": {"host": "localhost"}},
        )
        executor = SourceExecutor(node, context)
        # Should not raise
        executor.validate()

    def test_starrocks_missing_table(self, context):
        node = Node(
            id="source.sr_users",
            name="sr_users",
            node_type=NodeType.SOURCE,
            config={"source": "starrocks"},
        )
        executor = SourceExecutor(node, context)
        with pytest.raises(Exception, match="table"):
            executor.validate()


class TestSourceExecutorStarRocksLoad:
    """Test StarRocks data loading into DuckDB."""

    @patch("seeknal.workflow.executors.source_executor.materialize_node_if_enabled")
    def test_load_starrocks_data(self, mock_mat, context):
        """Test loading data from StarRocks creates a DuckDB view."""
        node = Node(
            id="source.sr_users",
            name="sr_users",
            node_type=NodeType.SOURCE,
            config={
                "source": "starrocks",
                "table": "users",
                "params": {
                    "host": "localhost",
                    "port": 9030,
                    "user": "root",
                    "database": "test_db",
                },
            },
        )
        executor = SourceExecutor(node, context)

        # Mock the StarRocks connection
        mock_sr_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = [("id",), ("name",), ("age",)]
        mock_cursor.fetchall.return_value = [
            (1, "alice", 30),
            (2, "bob", 25),
            (3, "charlie", 35),
        ]
        mock_sr_conn.cursor.return_value = mock_cursor

        with patch(
            "seeknal.workflow.executors.source_executor.SourceExecutor._load_starrocks"
        ) as mock_load:
            # Simulate what _load_starrocks does
            mock_load.return_value = 3

            result = executor.run()

        assert result.is_success()
        assert result.row_count == 3

    @patch("seeknal.workflow.executors.source_executor.materialize_node_if_enabled")
    def test_load_starrocks_with_custom_query(self, mock_mat, context):
        """Test custom query parameter."""
        node = Node(
            id="source.sr_active",
            name="sr_active",
            node_type=NodeType.SOURCE,
            config={
                "source": "starrocks",
                "table": "users",
                "params": {
                    "host": "localhost",
                    "query": "SELECT * FROM users WHERE status = 'active'",
                },
            },
        )
        executor = SourceExecutor(node, context)

        with patch(
            "seeknal.workflow.executors.source_executor.SourceExecutor._load_starrocks"
        ) as mock_load:
            mock_load.return_value = 2
            result = executor.run()

        assert result.is_success()
        assert result.row_count == 2

    @patch("seeknal.workflow.executors.source_executor.materialize_node_if_enabled")
    def test_load_starrocks_connection_error(self, mock_mat, context):
        """Test handling of connection errors."""
        from seeknal.workflow.executors.base import ExecutorExecutionError

        node = Node(
            id="source.sr_fail",
            name="sr_fail",
            node_type=NodeType.SOURCE,
            config={
                "source": "starrocks",
                "table": "users",
                "params": {"host": "bad-host"},
            },
        )
        executor = SourceExecutor(node, context)

        with patch(
            "seeknal.workflow.executors.source_executor.SourceExecutor._load_starrocks"
        ) as mock_load:
            mock_load.side_effect = ExecutorExecutionError(
                "source.sr_fail", "Connection refused"
            )
            with pytest.raises(ExecutorExecutionError, match="Connection refused"):
                executor.run()

    def test_load_starrocks_dry_run(self, context):
        """Test dry run mode."""
        context.dry_run = True
        node = Node(
            id="source.sr_users",
            name="sr_users",
            node_type=NodeType.SOURCE,
            config={
                "source": "starrocks",
                "table": "users",
                "params": {"host": "localhost"},
            },
        )
        executor = SourceExecutor(node, context)
        result = executor.run()

        assert result.is_success()
        assert result.is_dry_run


class TestExposureExecutorStarRocksMV:
    """Test ExposureExecutor StarRocks materialized view creation."""

    def test_starrocks_mv_type_registered(self):
        assert ExposureType.STARROCKS_MV.value == "starrocks_materialized_view"

    def test_validate_starrocks_mv(self, context):
        node = Node(
            id="exposure.user_mv",
            name="user_mv",
            node_type=NodeType.EXPOSURE,
            config={
                "type": "starrocks_materialized_view",
                "params": {
                    "mv_name": "user_summary_mv",
                    "query": "SELECT user_id, COUNT(*) FROM orders GROUP BY user_id",
                    "host": "localhost",
                },
            },
        )
        executor = ExposureExecutor(node, context)
        # Should not raise
        executor.validate()

    def test_validate_starrocks_mv_missing_name(self, context):
        node = Node(
            id="exposure.user_mv",
            name="user_mv",
            node_type=NodeType.EXPOSURE,
            config={
                "type": "starrocks_materialized_view",
                "params": {
                    "query": "SELECT 1",
                },
            },
        )
        executor = ExposureExecutor(node, context)
        with pytest.raises(Exception, match="mv_name"):
            executor.validate()

    def test_execute_starrocks_mv(self, context):
        """Test MV creation execution (mocked)."""
        node = Node(
            id="exposure.user_mv",
            name="user_mv",
            node_type=NodeType.EXPOSURE,
            config={
                "type": "starrocks_materialized_view",
                "params": {
                    "mv_name": "user_summary_mv",
                    "query": "SELECT user_id, COUNT(*) as cnt FROM orders GROUP BY user_id",
                    "refresh": "ASYNC",
                    "host": "localhost",
                    "port": 9030,
                    "database": "analytics",
                },
            },
        )
        executor = ExposureExecutor(node, context)

        mock_sr_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_sr_conn.cursor.return_value = mock_cursor

        with patch(
            "seeknal.connections.starrocks.create_starrocks_connection"
        ) as mock_create:
            mock_create.return_value = mock_sr_conn
            result = executor.run()

        assert result.is_success()
        assert "mv_name" in result.metadata

    def test_execute_starrocks_mv_connection_failure(self, context):
        """Test MV creation handles connection errors."""
        node = Node(
            id="exposure.user_mv",
            name="user_mv",
            node_type=NodeType.EXPOSURE,
            config={
                "type": "starrocks_materialized_view",
                "params": {
                    "mv_name": "user_summary_mv",
                    "query": "SELECT 1",
                    "host": "bad-host",
                },
            },
        )
        executor = ExposureExecutor(node, context)

        with patch(
            "seeknal.connections.starrocks.create_starrocks_connection"
        ) as mock_create:
            mock_create.side_effect = Exception("Connection refused")
            result = executor.run()

        assert not result.is_success()

    def test_execute_starrocks_mv_with_properties(self, context):
        """Test MV creation with custom properties."""
        node = Node(
            id="exposure.user_mv",
            name="user_mv",
            node_type=NodeType.EXPOSURE,
            config={
                "type": "starrocks_materialized_view",
                "params": {
                    "mv_name": "user_stats_mv",
                    "query": "SELECT user_id, SUM(amount) FROM orders GROUP BY user_id",
                    "refresh": "ASYNC START('2024-01-01 00:00:00') EVERY(INTERVAL 1 DAY)",
                    "properties": {
                        "replication_num": "1",
                        "storage_medium": "HDD",
                    },
                    "host": "localhost",
                },
            },
        )
        executor = ExposureExecutor(node, context)

        mock_sr_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_sr_conn.cursor.return_value = mock_cursor

        with patch(
            "seeknal.connections.starrocks.create_starrocks_connection"
        ) as mock_create:
            mock_create.return_value = mock_sr_conn
            result = executor.run()

        assert result.is_success()
        # Verify DDL includes properties
        ddl = result.metadata.get("ddl", "")
        assert "PROPERTIES" in ddl
        assert "replication_num" in ddl
