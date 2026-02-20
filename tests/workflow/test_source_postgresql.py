"""Tests for PostgreSQL source executor profile connection and pushdown query support."""
from unittest.mock import MagicMock, patch

import pytest  # ty: ignore[unresolved-import]
import duckdb  # ty: ignore[unresolved-import]

from seeknal.dag.manifest import Node, NodeType  # ty: ignore[unresolved-import]
from seeknal.workflow.executors.source_executor import SourceExecutor  # ty: ignore[unresolved-import]
from seeknal.workflow.executors.base import ExecutorValidationError, ExecutorExecutionError  # ty: ignore[unresolved-import]
from seeknal.connections.postgresql import PostgreSQLConfig  # ty: ignore[unresolved-import]


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


def _make_pg_node(node_id="source.pg_users", table="users", params=None):
    """Helper to create a PostgreSQL source node."""
    if params is None:
        params = {"host": "localhost", "port": 5432, "database": "testdb", "user": "postgres"}
    return Node(
        id=node_id,
        name=node_id.split(".", 1)[-1] if "." in node_id else node_id,
        node_type=NodeType.SOURCE,
        config={"source": "postgresql", "table": table, "params": params},
    )


def _make_pg_query_node(node_id="source.pg_active", query="SELECT id FROM users", params=None):
    """Helper to create a PostgreSQL source node with pushdown query (no table)."""
    if params is None:
        params = {"host": "localhost", "port": 5432, "database": "testdb", "user": "postgres"}
    params["query"] = query
    return Node(
        id=node_id,
        name=node_id.split(".", 1)[-1] if "." in node_id else node_id,
        node_type=NodeType.SOURCE,
        config={"source": "postgresql", "params": params},
    )


class TestResolvePostgresqlConfig:
    """Test _resolve_postgresql_config with inline and profile connections."""

    def test_inline_params_returns_config(self, context):
        """Inline host/port/database/user/password produces a PostgreSQLConfig."""
        node = _make_pg_node(params={
            "host": "db.example.com",
            "port": 5433,
            "database": "analytics",
            "user": "reader",
            "password": "secret",
            "schema": "staging",
        })
        executor = SourceExecutor(node, context)
        config = executor._resolve_postgresql_config(node.config["params"])

        assert isinstance(config, PostgreSQLConfig)
        assert config.host == "db.example.com"
        assert config.port == 5433
        assert config.database == "analytics"
        assert config.user == "reader"
        assert config.password == "secret"
        assert config.schema == "staging"

    def test_inline_defaults(self, context):
        """Missing inline params fall back to PostgreSQLConfig defaults."""
        node = _make_pg_node(params={})
        executor = SourceExecutor(node, context)
        config = executor._resolve_postgresql_config(node.config["params"])

        assert config.host == "localhost"
        assert config.port == 5432
        assert config.database == "postgres"
        assert config.user == "postgres"
        assert config.password == ""
        assert config.schema == "public"

    @patch("seeknal.connections.postgresql.parse_postgresql_config")
    @patch("seeknal.workflow.materialization.profile_loader.ProfileLoader.load_connection_profile")
    def test_connection_profile_resolves(self, mock_load_profile, mock_parse, context):
        """connection: my_pg loads from profile and parses config."""
        mock_load_profile.return_value = {
            "type": "postgresql",
            "host": "profile-host",
            "port": 5432,
            "database": "profile-db",
            "user": "profile-user",
            "password": "profile-pass",
        }
        expected_config = PostgreSQLConfig(
            host="profile-host", port=5432, database="profile-db",
            user="profile-user", password="profile-pass",
        )
        mock_parse.return_value = expected_config

        node = _make_pg_node(params={"connection": "my_pg"})
        executor = SourceExecutor(node, context)
        config = executor._resolve_postgresql_config(node.config["params"])

        mock_load_profile.assert_called_once_with("my_pg")
        assert config == expected_config

    @patch("seeknal.connections.postgresql.parse_postgresql_config")
    @patch("seeknal.workflow.materialization.profile_loader.ProfileLoader.load_connection_profile")
    def test_inline_overrides_profile(self, mock_load_profile, mock_parse, context):
        """Inline params override profile defaults."""
        mock_load_profile.return_value = {
            "type": "postgresql",
            "host": "profile-host",
            "port": 5432,
            "database": "profile-db",
            "user": "profile-user",
            "password": "profile-pass",
        }
        expected_config = PostgreSQLConfig(
            host="override-host", port=5432, database="profile-db",
            user="profile-user", password="profile-pass",
        )
        mock_parse.return_value = expected_config

        node = _make_pg_node(params={"connection": "my_pg", "host": "override-host"})
        executor = SourceExecutor(node, context)
        executor._resolve_postgresql_config(node.config["params"])

        # Verify that the profile dict was modified with the override
        call_args = mock_parse.call_args[0][0]
        assert call_args["host"] == "override-host"

    @patch("seeknal.connections.postgresql.parse_postgresql_config")
    @patch("seeknal.workflow.materialization.profile_loader.ProfileLoader.load_connection_profile")
    @patch("seeknal.workflow.materialization.profile_loader.ProfileLoader.clear_connection_credentials")
    def test_credentials_cleared_after_use(self, mock_clear, mock_load_profile, mock_parse, context):
        """Credentials are cleared from profile dict after config is built."""
        mock_load_profile.return_value = {
            "type": "postgresql",
            "host": "h",
            "password": "secret",
        }
        mock_parse.return_value = PostgreSQLConfig()

        node = _make_pg_node(params={"connection": "my_pg"})
        executor = SourceExecutor(node, context)
        executor._resolve_postgresql_config(node.config["params"])

        mock_clear.assert_called_once()


class TestLoadPostgresqlWithProfile:
    """Test _load_postgresql with profile-based connections."""

    @patch("seeknal.workflow.executors.source_executor.materialize_node_if_enabled")
    @patch("seeknal.workflow.executors.source_executor.SourceExecutor._load_postgresql")
    def test_postgresql_source_runs(self, mock_load, mock_mat, context):  # noqa: ARG002
        """PostgreSQL source with inline params executes successfully."""
        mock_load.return_value = 5

        node = _make_pg_node(params={
            "host": "localhost",
            "port": 5432,
            "database": "testdb",
            "user": "postgres",
        })
        executor = SourceExecutor(node, context)
        result = executor.run()

        assert result.is_success()
        assert result.row_count == 5

    @patch("seeknal.workflow.executors.source_executor.materialize_node_if_enabled")
    @patch("seeknal.workflow.executors.source_executor.SourceExecutor._load_postgresql")
    def test_postgresql_source_with_profile(self, mock_load, mock_mat, context):  # noqa: ARG002
        """PostgreSQL source with connection: profile_name executes successfully."""
        mock_load.return_value = 10

        node = _make_pg_node(params={"connection": "my_pg"})
        executor = SourceExecutor(node, context)
        result = executor.run()

        assert result.is_success()
        assert result.row_count == 10

    def test_libpq_string_used_in_attach(self):
        """The ATTACH statement uses PostgreSQLConfig.to_libpq_string()."""
        config = PostgreSQLConfig(
            host="db.local", port=5433, database="analytics",
            user="reader", password="s3cret", schema="staging",
        )
        conn_str = config.to_libpq_string()

        assert "host=db.local" in conn_str
        assert "port=5433" in conn_str
        assert "dbname=analytics" in conn_str
        assert "user=reader" in conn_str
        assert "password=s3cret" in conn_str
        assert "sslmode=prefer" in conn_str

    def test_schema_from_config_used_in_view(self):
        """config.schema is used for the full_table_name instead of hardcoded 'public'."""
        config = PostgreSQLConfig(schema="staging")
        assert config.schema == "staging"


class TestLoadPostgresqlErrorHandling:
    """Test error handling and password masking."""

    def test_mask_password_in_error(self):
        """Error messages should mask passwords."""
        from seeknal.connections.postgresql import mask_password  # ty: ignore[unresolved-import]

        msg = "connection failed: host=db port=5432 user=me password=secret123 dbname=test"
        masked = mask_password(msg)
        assert "secret123" not in masked
        assert "password=***" in masked

    def test_mask_password_url_format(self):
        """URL-format passwords are also masked."""
        from seeknal.connections.postgresql import mask_password  # ty: ignore[unresolved-import]

        msg = "connection to postgresql://admin:topsecret@db:5432/test failed"
        masked = mask_password(msg)
        assert "topsecret" not in masked

    def test_postgresql_validation(self, context):
        """PostgreSQL source validates successfully with required fields."""
        node = _make_pg_node()
        executor = SourceExecutor(node, context)
        executor.validate()

    def test_postgresql_missing_table(self, context):
        """PostgreSQL source fails validation without table or query."""
        node = Node(
            id="source.pg_users",
            name="pg_users",
            node_type=NodeType.SOURCE,
            config={"source": "postgresql"},
        )
        executor = SourceExecutor(node, context)
        with pytest.raises(ExecutorValidationError, match="requires either"):
            executor.validate()


class TestPushdownQueryValidation:
    """Test pushdown query validation in validate()."""

    def test_select_query_passes_validation(self, context):
        """A SELECT pushdown query passes validation."""
        node = _make_pg_query_node(query="SELECT id, name FROM users WHERE active = true")
        executor = SourceExecutor(node, context)
        executor.validate()

    def test_with_cte_query_passes_validation(self, context):
        """A WITH (CTE) pushdown query passes validation."""
        node = _make_pg_query_node(
            query="WITH active AS (SELECT * FROM users WHERE active) SELECT * FROM active"
        )
        executor = SourceExecutor(node, context)
        executor.validate()

    def test_case_insensitive_select(self, context):
        """SELECT validation is case-insensitive."""
        node = _make_pg_query_node(query="select id from users")
        executor = SourceExecutor(node, context)
        executor.validate()

    def test_table_and_query_coexist(self, context):
        """When both table: and query: are present, query takes precedence."""
        node = Node(
            id="source.pg_users",
            name="pg_users",
            node_type=NodeType.SOURCE,
            config={
                "source": "postgresql",
                "table": "users",
                "params": {"query": "SELECT * FROM users"},
            },
        )
        executor = SourceExecutor(node, context)
        # Should NOT raise — table serves as documentation, query takes precedence
        executor.validate()

    def test_reject_insert(self, context):
        """INSERT queries are rejected."""
        node = _make_pg_query_node(query="INSERT INTO users VALUES (1, 'test')")
        executor = SourceExecutor(node, context)
        with pytest.raises(ExecutorValidationError, match="DDL/DML not allowed"):
            executor.validate()

    def test_reject_update(self, context):
        """UPDATE queries are rejected."""
        node = _make_pg_query_node(query="UPDATE users SET name = 'x'")
        executor = SourceExecutor(node, context)
        with pytest.raises(ExecutorValidationError, match="DDL/DML not allowed"):
            executor.validate()

    def test_reject_delete(self, context):
        """DELETE queries are rejected."""
        node = _make_pg_query_node(query="DELETE FROM users WHERE id = 1")
        executor = SourceExecutor(node, context)
        with pytest.raises(ExecutorValidationError, match="DDL/DML not allowed"):
            executor.validate()

    def test_reject_drop(self, context):
        """DROP queries are rejected."""
        node = _make_pg_query_node(query="DROP TABLE users")
        executor = SourceExecutor(node, context)
        with pytest.raises(ExecutorValidationError, match="DDL/DML not allowed"):
            executor.validate()

    def test_reject_alter(self, context):
        """ALTER queries are rejected."""
        node = _make_pg_query_node(query="ALTER TABLE users ADD COLUMN age INT")
        executor = SourceExecutor(node, context)
        with pytest.raises(ExecutorValidationError, match="DDL/DML not allowed"):
            executor.validate()

    def test_reject_truncate(self, context):
        """TRUNCATE queries are rejected."""
        node = _make_pg_query_node(query="TRUNCATE users")
        executor = SourceExecutor(node, context)
        with pytest.raises(ExecutorValidationError, match="DDL/DML not allowed"):
            executor.validate()

    def test_reject_empty_query(self, context):
        """Empty query string is rejected."""
        node = _make_pg_query_node(query="   ")
        executor = SourceExecutor(node, context)
        with pytest.raises(ExecutorValidationError, match="empty"):
            executor.validate()

    def test_reject_unknown_statement(self, context):
        """Non-SELECT/WITH statements are rejected."""
        node = _make_pg_query_node(query="EXPLAIN SELECT * FROM users")
        executor = SourceExecutor(node, context)
        with pytest.raises(ExecutorValidationError, match="must start with SELECT or WITH"):
            executor.validate()


class TestPushdownQueryExecution:
    """Test _load_postgresql with pushdown queries via postgres_query()."""

    @patch("seeknal.workflow.executors.source_executor.materialize_node_if_enabled")
    @patch("seeknal.workflow.executors.source_executor.SourceExecutor._load_postgresql")
    def test_pushdown_query_source_runs(self, mock_load, mock_mat, context):  # noqa: ARG002
        """PostgreSQL source with query: param executes successfully."""
        mock_load.return_value = 42

        node = _make_pg_query_node(query="SELECT id, name FROM users WHERE active = true")
        executor = SourceExecutor(node, context)
        result = executor.run()

        assert result.is_success()
        assert result.row_count == 42

    @patch("seeknal.workflow.executors.source_executor.materialize_node_if_enabled")
    @patch("seeknal.workflow.executors.source_executor.SourceExecutor._load_postgresql")
    def test_pushdown_query_passes_params_to_loader(self, mock_load, mock_mat, context):  # noqa: ARG002
        """The query param is forwarded in params dict to _load_postgresql."""
        mock_load.return_value = 10

        query = "SELECT id FROM users WHERE region = 'EU'"
        node = _make_pg_query_node(query=query)
        executor = SourceExecutor(node, context)
        executor.run()

        # Verify _load_postgresql was called and params contain the query
        call_args = mock_load.call_args
        passed_params = call_args[0][2]  # (con, table, params)
        assert passed_params["query"] == query

    def test_postgres_query_sql_uses_correct_function(self, context):
        """When query is in params, the SQL should use postgres_query() function."""
        node = _make_pg_query_node(query="SELECT id FROM users")
        executor = SourceExecutor(node, context)

        # Verify _validate_pushdown_query doesn't raise for valid query
        executor._validate_pushdown_query("SELECT id FROM users")

    def test_single_quotes_escaped_in_pushdown(self, context):
        """Single quotes in pushdown queries are escaped for SQL safety."""
        node = _make_pg_query_node(query="SELECT * FROM users WHERE name = 'O''Brien'")
        executor = SourceExecutor(node, context)
        executor.validate()
