"""
Tests for TransformExecutor.

Tests the transform node executor for SQL transformations.
"""

import pytest
from pathlib import Path
from seeknal.workflow.executors.base import (
    ExecutionContext,
    ExecutionStatus,
    ExecutorValidationError,
)
from seeknal.workflow.executors.transform_executor import TransformExecutor
from seeknal.dag.manifest import Node, NodeType


@pytest.fixture
def sample_transform_node():
    """Create a sample transform node."""
    return Node(
        id="transform.clean_users",
        name="clean_users",
        node_type=NodeType.TRANSFORM,
        config={
            "transform": "SELECT * FROM users WHERE age >= 18",
            "inputs": {
                "users": "ref:source.raw_users"
            }
        },
        description="Clean user data",
        tags=["transform", "cleaning"],
    )


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


class TestTransformExecutorValidation:
    """Test TransformExecutor validation."""

    def test_validate_success(self, sample_transform_node, execution_context):
        """Test validation succeeds with valid config."""
        executor = TransformExecutor(sample_transform_node, execution_context)

        # Should not raise
        executor.validate()

    def test_validate_missing_transform(self, execution_context):
        """Test validation fails when transform field is missing."""
        node = Node(
            id="transform.invalid",
            name="invalid",
            node_type=NodeType.TRANSFORM,
            config={},  # Missing 'transform' field
        )

        executor = TransformExecutor(node, execution_context)

        with pytest.raises(ExecutorValidationError) as exc_info:
            executor.validate()

        assert "Missing or invalid 'transform' field" in str(exc_info.value)

    def test_validate_empty_transform(self, execution_context):
        """Test validation fails when transform is empty."""
        node = Node(
            id="transform.empty",
            name="empty",
            node_type=NodeType.TRANSFORM,
            config={"transform": "   -- Just a comment   "},
        )

        executor = TransformExecutor(node, execution_context)

        with pytest.raises(ExecutorValidationError) as exc_info:
            executor.validate()

        assert "empty or contains only comments" in str(exc_info.value)

    def test_validate_invalid_inputs(self, execution_context):
        """Test validation fails when inputs is not a dict."""
        node = Node(
            id="transform.invalid_inputs",
            name="invalid_inputs",
            node_type=NodeType.TRANSFORM,
            config={
                "transform": "SELECT 1",
                "inputs": "not_a_dict"
            },
        )

        executor = TransformExecutor(node, execution_context)

        with pytest.raises(ExecutorValidationError) as exc_info:
            executor.validate()

        assert "must be a dictionary" in str(exc_info.value)


class TestTransformExecutorDryRun:
    """Test TransformExecutor dry-run mode."""

    def test_execute_dry_run(self, sample_transform_node, execution_context):
        """Test dry-run execution returns success without executing SQL."""
        # Enable dry-run
        execution_context.dry_run = True

        executor = TransformExecutor(sample_transform_node, execution_context)
        result = executor.run()

        assert result.is_success()
        assert result.is_dry_run
        assert result.duration_seconds < 0.1  # Should be very fast
        assert result.row_count == 0
        assert result.metadata["engine"] == "duckdb"
        assert result.metadata["dry_run"] is True


class TestTransformExecutorSQLHandling:
    """Test SQL handling in TransformExecutor."""

    def test_split_single_statement(self, sample_transform_node, execution_context):
        """Test splitting a single SQL statement."""
        executor = TransformExecutor(sample_transform_node, execution_context)

        sql = "SELECT * FROM users"
        statements = executor._split_sql_statements(sql)

        assert len(statements) == 1
        assert statements[0] == sql

    def test_split_multiple_statements(self, sample_transform_node, execution_context):
        """Test splitting multiple SQL statements."""
        executor = TransformExecutor(sample_transform_node, execution_context)

        sql = """
            SELECT * FROM users;
            INSERT INTO cleaned_users SELECT * FROM users;
            SELECT COUNT(*) FROM cleaned_users;
        """
        statements = executor._split_sql_statements(sql)

        assert len(statements) == 3
        assert "SELECT * FROM users" in statements[0]
        assert "INSERT INTO cleaned_users" in statements[1]
        assert "COUNT(*) FROM cleaned_users" in statements[2]

    def test_split_preserves_strings_with_semicolons(self, sample_transform_node, execution_context):
        """Test that semicolons in string literals are not used as delimiters."""
        executor = TransformExecutor(sample_transform_node, execution_context)

        sql = "SELECT 'hello;world' AS greeting"
        statements = executor._split_sql_statements(sql)

        assert len(statements) == 1
        assert "hello;world" in statements[0]

    def test_is_select_statement(self, sample_transform_node, execution_context):
        """Test detecting SELECT statements."""
        executor = TransformExecutor(sample_transform_node, execution_context)

        assert executor._is_select_statement("SELECT * FROM users") is True
        assert executor._is_select_statement("  SELECT 1  ") is True
        assert executor._is_select_statement("WITH cte AS (SELECT 1) SELECT * FROM cte") is True
        assert executor._is_select_statement("CREATE TABLE test AS SELECT 1") is False
        assert executor._is_select_statement("INSERT INTO users VALUES (1)") is False
        assert executor._is_select_statement("DROP TABLE users") is False

    def test_clean_sql_removes_comments(self, sample_transform_node, execution_context):
        """Test removing SQL comments."""
        executor = TransformExecutor(sample_transform_node, execution_context)

        sql = """
            -- This is a comment
            SELECT * FROM users;  -- Inline comment
            /* Multi-line
               comment */
            SELECT 1;
        """
        cleaned = executor._clean_sql(sql)

        assert "--" not in cleaned
        assert "/*" not in cleaned
        assert "SELECT * FROM users" in cleaned
        assert "SELECT 1" in cleaned

    def test_validate_sql_syntax(self, sample_transform_node, execution_context):
        """Test basic SQL syntax validation."""
        executor = TransformExecutor(sample_transform_node, execution_context)

        # Valid syntax
        assert executor._validate_sql_syntax("SELECT * FROM users WHERE name = 'test'") is True
        assert executor._validate_sql_syntax("SELECT (1 + 2) AS sum") is True

        # Invalid syntax
        assert executor._validate_sql_syntax("SELECT * FROM users WHERE name = 'test") is False  # Unclosed quote
        assert executor._validate_sql_syntax("SELECT (1 + 2 AS sum") is False  # Unbalanced paren


class TestTransformExecutorRefResolution:
    """Test input ref resolution."""

    def test_resolve_no_refs(self, sample_transform_node, execution_context):
        """Test SQL with no refs is unchanged."""
        executor = TransformExecutor(sample_transform_node, execution_context)

        sql = "SELECT * FROM users"
        resolved = executor._resolve_input_refs(sql, None)

        assert resolved == sql

    def test_resolve_ref_with_braces(self, sample_transform_node, execution_context):
        """Test resolving refs with {{ref}} syntax."""
        node = Node(
            id="transform.test",
            name="test",
            node_type=NodeType.TRANSFORM,
            config={
                "transform": "SELECT * FROM {{users}}",
                "inputs": {
                    "users": "ref:source.raw_users"
                }
            },
        )

        executor = TransformExecutor(node, execution_context)
        resolved = executor._resolve_input_refs("SELECT * FROM {{users}}", None)

        assert "{{users}}" not in resolved
        assert "source_raw_users" in resolved or "source_raw_users" in resolved

    def test_resolve_ref_with_dollar(self, sample_transform_node, execution_context):
        """Test resolving refs with $ref syntax."""
        node = Node(
            id="transform.test",
            name="test",
            node_type=NodeType.TRANSFORM,
            config={
                "transform": "SELECT * FROM $users",
                "inputs": {
                    "users": "ref:source.raw_users"
                }
            },
        )

        executor = TransformExecutor(node, execution_context)
        resolved = executor._resolve_input_refs("SELECT * FROM $users", None)

        assert "$users" not in resolved
        assert "source_raw_users" in resolved or "source_raw_users" in resolved


class TestTransformExecutorPostExecute:
    """Test post-execution metadata enhancement."""

    def test_post_execute_adds_metadata(self, sample_transform_node, execution_context):
        """Test that post_execute adds additional metadata."""
        executor = TransformExecutor(sample_transform_node, execution_context)

        from seeknal.workflow.executors.base import ExecutorResult

        result = ExecutorResult(
            node_id="test",
            status=ExecutionStatus.SUCCESS,
            duration_seconds=1.0,
            row_count=100,
        )

        enhanced = executor.post_execute(result)

        assert "executor_version" in enhanced.metadata
        assert "sql_preview" in enhanced.metadata
        assert enhanced.metadata["executor_version"] == "1.0.0"


class TestTransformExecutorIntegration:
    """Integration tests with actual DuckDB execution."""

    def test_execute_simple_select(self, execution_context):
        """Test executing a simple SELECT query."""
        node = Node(
            id="transform.simple",
            name="simple",
            node_type=NodeType.TRANSFORM,
            config={
                "transform": "SELECT 1 AS num, 'test' AS str"
            },
        )

        executor = TransformExecutor(node, execution_context)
        result = executor.run()

        assert result.is_success()
        assert result.row_count == 1
        assert result.duration_seconds > 0
        assert result.metadata["engine"] == "duckdb"
        assert result.metadata["statements_executed"] == 1

    def test_execute_multi_statement(self, execution_context):
        """Test executing multiple SQL statements."""
        node = Node(
            id="transform.multi",
            name="multi",
            node_type=NodeType.TRANSFORM,
            config={
                "transform": """
                    CREATE OR REPLACE TABLE temp AS SELECT 1 AS num;
                    SELECT * FROM temp;
                    DROP TABLE temp;
                """
            },
        )

        executor = TransformExecutor(node, execution_context)
        result = executor.run()

        assert result.is_success()
        assert result.metadata["statements_executed"] == 3

    def test_execute_with_cte(self, execution_context):
        """Test executing a query with CTE (WITH clause)."""
        node = Node(
            id="transform.cte",
            name="cte",
            node_type=NodeType.TRANSFORM,
            config={
                "transform": """
                    WITH numbers AS (
                        SELECT 1 AS n
                        UNION ALL
                        SELECT 2
                        UNION ALL
                        SELECT 3
                    )
                    SELECT SUM(n) AS total FROM numbers
                """
            },
        )

        executor = TransformExecutor(node, execution_context)
        result = executor.run()

        assert result.is_success()
        assert result.row_count == 1  # Single row with SUM result

    def test_execute_with_error(self, execution_context):
        """Test that SQL errors are properly reported."""
        from seeknal.workflow.executors.base import ExecutorExecutionError

        node = Node(
            id="transform.error",
            name="error",
            node_type=NodeType.TRANSFORM,
            config={
                "transform": "SELECT * FROM nonexistent_table"
            },
        )

        executor = TransformExecutor(node, execution_context)

        # ExecutorExecutionError should be raised
        with pytest.raises(ExecutorExecutionError) as exc_info:
            executor.run()

        # Verify error message contains useful info
        assert "nonexistent_table" in str(exc_info.value) or "not found" in str(exc_info.value).lower()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
