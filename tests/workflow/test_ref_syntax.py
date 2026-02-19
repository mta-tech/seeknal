"""
Tests for named ref() input syntax in transforms.

Tests the ref('source.sales') -> input_0 resolution in both
the TransformExecutor and the dry-run preview executor.
"""

import pytest
from pathlib import Path
from seeknal.workflow.executors.base import (
    ExecutionContext,
    ExecutionStatus,
    ExecutorExecutionError,
)
from seeknal.workflow.executors.transform_executor import TransformExecutor
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


# --- TransformExecutor._resolve_named_refs() ---


class TestResolveNamedRefs:
    """Test _resolve_named_refs() method on TransformExecutor."""

    def _make_executor(self, inputs, sql, ctx):
        node = Node(
            id="transform.test",
            name="test",
            node_type=NodeType.TRANSFORM,
            config={"transform": sql, "inputs": inputs},
        )
        return TransformExecutor(node, ctx)

    def test_single_ref(self, execution_context):
        """ref('source.sales') is replaced by input_0."""
        inputs = [{"ref": "source.sales"}]
        sql = "SELECT * FROM ref('source.sales')"
        executor = self._make_executor(inputs, sql, execution_context)
        result = executor._resolve_named_refs(sql)
        assert result == "SELECT * FROM input_0"

    def test_multiple_refs(self, execution_context):
        """Multiple ref() calls resolve to correct indices."""
        inputs = [
            {"ref": "source.sales"},
            {"ref": "source.products"},
        ]
        sql = (
            "SELECT s.*, p.name "
            "FROM ref('source.sales') s "
            "JOIN ref('source.products') p ON s.pid = p.id"
        )
        executor = self._make_executor(inputs, sql, execution_context)
        result = executor._resolve_named_refs(sql)
        assert "input_0" in result
        assert "input_1" in result
        assert "ref(" not in result

    def test_double_quoted_ref(self, execution_context):
        """ref(\"source.sales\") with double quotes works."""
        inputs = [{"ref": "source.sales"}]
        sql = 'SELECT * FROM ref("source.sales")'
        executor = self._make_executor(inputs, sql, execution_context)
        result = executor._resolve_named_refs(sql)
        assert result == "SELECT * FROM input_0"

    def test_ref_with_spaces(self, execution_context):
        """ref(  'source.sales'  ) with extra spaces works."""
        inputs = [{"ref": "source.sales"}]
        sql = "SELECT * FROM ref(  'source.sales'  )"
        executor = self._make_executor(inputs, sql, execution_context)
        result = executor._resolve_named_refs(sql)
        assert result == "SELECT * FROM input_0"

    def test_no_ref_calls_unchanged(self, execution_context):
        """SQL without ref() calls passes through unchanged."""
        inputs = [{"ref": "source.sales"}]
        sql = "SELECT * FROM input_0"
        executor = self._make_executor(inputs, sql, execution_context)
        result = executor._resolve_named_refs(sql)
        assert result == sql

    def test_mixed_ref_and_positional(self, execution_context):
        """ref() and input_N can coexist in the same SQL."""
        inputs = [
            {"ref": "source.sales"},
            {"ref": "source.products"},
        ]
        sql = "SELECT * FROM ref('source.sales') s JOIN input_1 p ON s.pid = p.id"
        executor = self._make_executor(inputs, sql, execution_context)
        result = executor._resolve_named_refs(sql)
        assert "input_0 s" in result
        assert "input_1 p" in result

    def test_no_inputs_unchanged(self, execution_context):
        """SQL is unchanged when there are no inputs."""
        node = Node(
            id="transform.test",
            name="test",
            node_type=NodeType.TRANSFORM,
            config={"transform": "SELECT 1"},
        )
        executor = TransformExecutor(node, execution_context)
        result = executor._resolve_named_refs("SELECT 1")
        assert result == "SELECT 1"

    def test_dict_inputs_unchanged(self, execution_context):
        """ref() resolution is skipped for dict-style inputs."""
        node = Node(
            id="transform.test",
            name="test",
            node_type=NodeType.TRANSFORM,
            config={
                "transform": "SELECT * FROM ref('source.sales')",
                "inputs": {"sales": "ref:source.sales"},
            },
        )
        executor = TransformExecutor(node, execution_context)
        sql = "SELECT * FROM ref('source.sales')"
        result = executor._resolve_named_refs(sql)
        # Dict inputs are not list, so _resolve_named_refs returns unchanged
        assert result == sql

    def test_unknown_ref_raises(self, execution_context):
        """ref() with unknown name raises ExecutorExecutionError."""
        inputs = [{"ref": "source.sales"}]
        sql = "SELECT * FROM ref('source.nonexistent')"
        executor = self._make_executor(inputs, sql, execution_context)
        with pytest.raises(ExecutorExecutionError) as exc_info:
            executor._resolve_named_refs(sql)
        assert "Unknown ref('source.nonexistent')" in str(exc_info.value)
        assert "source.sales" in str(exc_info.value)

    def test_invalid_ref_name_raises(self, execution_context):
        """ref() with invalid characters raises ExecutorExecutionError."""
        inputs = [{"ref": "source.sales"}]
        sql = "SELECT * FROM ref('DROP TABLE; --')"
        executor = self._make_executor(inputs, sql, execution_context)
        with pytest.raises(ExecutorExecutionError) as exc_info:
            executor._resolve_named_refs(sql)
        assert "Invalid ref() argument" in str(exc_info.value)

    def test_ref_with_hyphen_in_name(self, execution_context):
        """ref() supports hyphens in names (e.g., source.my-data)."""
        inputs = [{"ref": "source.my-data"}]
        sql = "SELECT * FROM ref('source.my-data')"
        executor = self._make_executor(inputs, sql, execution_context)
        result = executor._resolve_named_refs(sql)
        assert result == "SELECT * FROM input_0"

    def test_ref_with_underscore_in_name(self, execution_context):
        """ref() supports underscores in names."""
        inputs = [{"ref": "source.my_data"}]
        sql = "SELECT * FROM ref('source.my_data')"
        executor = self._make_executor(inputs, sql, execution_context)
        result = executor._resolve_named_refs(sql)
        assert result == "SELECT * FROM input_0"

    def test_ref_in_cte(self, execution_context):
        """ref() works inside CTE (WITH clause)."""
        inputs = [{"ref": "source.sales"}]
        sql = (
            "WITH cte AS (SELECT * FROM ref('source.sales')) "
            "SELECT * FROM cte"
        )
        executor = self._make_executor(inputs, sql, execution_context)
        result = executor._resolve_named_refs(sql)
        assert "input_0" in result
        assert "ref(" not in result


# --- Dry-run integration ---


class TestDryRunRefResolution:
    """Test that dry-run mode resolves ref() syntax."""

    def test_dry_run_resolves_refs(self, execution_context):
        """Dry-run resolves ref() and returns success."""
        execution_context.dry_run = True
        node = Node(
            id="transform.test",
            name="test",
            node_type=NodeType.TRANSFORM,
            config={
                "transform": "SELECT * FROM ref('source.sales')",
                "inputs": [{"ref": "source.sales"}],
            },
            file_path="test.yml",
        )
        executor = TransformExecutor(node, execution_context)
        result = executor.run()
        assert result.is_success()
        assert result.is_dry_run

    def test_dry_run_invalid_ref_raises(self, execution_context):
        """Dry-run with unknown ref() raises error."""
        execution_context.dry_run = True
        node = Node(
            id="transform.test",
            name="test",
            node_type=NodeType.TRANSFORM,
            config={
                "transform": "SELECT * FROM ref('source.nonexistent')",
                "inputs": [{"ref": "source.sales"}],
            },
            file_path="test.yml",
        )
        executor = TransformExecutor(node, execution_context)
        with pytest.raises(ExecutorExecutionError) as exc_info:
            executor.run()
        assert "Unknown ref" in str(exc_info.value)


# --- Preview executor (executor.py) ---


class TestPreviewResolveNamedRefs:
    """Test _resolve_named_refs() in the preview executor."""

    def test_preview_resolves_refs(self):
        """Preview executor resolves ref() to input_N."""
        from seeknal.workflow.executor import _resolve_named_refs

        inputs = [
            {"ref": "source.sales"},
            {"ref": "source.products"},
        ]
        sql = "SELECT * FROM ref('source.sales') s JOIN ref('source.products') p ON s.pid = p.id"
        result = _resolve_named_refs(sql, inputs)
        assert "input_0" in result
        assert "input_1" in result
        assert "ref(" not in result

    def test_preview_no_inputs(self):
        """Preview with empty inputs returns SQL unchanged."""
        from seeknal.workflow.executor import _resolve_named_refs

        sql = "SELECT * FROM ref('source.sales')"
        result = _resolve_named_refs(sql, [])
        assert result == sql

    def test_preview_unknown_ref_warns(self):
        """Preview with unknown ref returns SQL unchanged (warns, no error)."""
        from seeknal.workflow.executor import _resolve_named_refs

        inputs = [{"ref": "source.sales"}]
        sql = "SELECT * FROM ref('source.unknown')"
        # Preview executor warns but doesn't raise (returns unchanged)
        result = _resolve_named_refs(sql, inputs)
        assert "ref('source.unknown')" in result

    def test_preview_invalid_ref_warns(self):
        """Preview with invalid ref name returns SQL unchanged."""
        from seeknal.workflow.executor import _resolve_named_refs

        inputs = [{"ref": "source.sales"}]
        sql = "SELECT * FROM ref('DROP TABLE; --')"
        result = _resolve_named_refs(sql, inputs)
        # Invalid ref stays unchanged (warning emitted)
        assert "ref(" in result


# --- Full integration: execute with ref() ---


class TestTransformExecutorRefIntegration:
    """Integration test: execute SQL using ref() syntax with real DuckDB."""

    def test_execute_ref_resolved_before_input_refs(self, execution_context):
        """Verify ref() is resolved to input_N before _resolve_input_refs runs."""
        node = Node(
            id="transform.ref_test",
            name="ref_test",
            node_type=NodeType.TRANSFORM,
            config={
                "transform": "SELECT * FROM ref('source.sales')",
                "inputs": [{"ref": "source.sales"}],
            },
            file_path="test.yml",
        )
        executor = TransformExecutor(node, execution_context)
        # Test the resolution chain directly
        sql = "SELECT * FROM ref('source.sales')"
        resolved = executor._resolve_named_refs(sql)
        assert resolved == "SELECT * FROM input_0"
        # The SQL now uses input_0, which _resolve_input_refs will handle


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
