"""Tests for Python SOA integration: DAG builder, executor routing, and two-phase execution.

Tests cover:
- DAG builder: _parse_python_file() SOA branch populates yaml_data correctly
- Executor routing: Python SOA nodes go to SecondOrderAggregationExecutor
- Two-phase execution: _is_python_node(), _execute_python_preprocessing()
- dry_run.py: extract_decorators() recognizes second_order_aggregation
"""

import ast
import pytest  # ty: ignore[unresolved-import]
from unittest.mock import MagicMock, patch

from seeknal.dag.manifest import Node, NodeType  # ty: ignore[unresolved-import]
from seeknal.workflow.executors.base import (  # ty: ignore[unresolved-import]
    ExecutionContext,
    ExecutionStatus,
    ExecutorResult,
    ExecutorExecutionError,
)
from seeknal.workflow.executors.second_order_aggregation_executor import (  # ty: ignore[unresolved-import]
    SecondOrderAggregationExecutor,
)


# =============================================================================
# extract_decorators() tests
# =============================================================================


class TestExtractDecoratorsSOA:
    """Test that extract_decorators() recognizes @second_order_aggregation."""

    def test_recognizes_soa_decorator(self):
        """extract_decorators() finds @second_order_aggregation in Python AST."""
        from seeknal.workflow.dry_run import extract_decorators  # ty: ignore[unresolved-import]

        source_code = '''
from seeknal.pipeline import second_order_aggregation

@second_order_aggregation(
    name="region_metrics",
    id_col="region",
    feature_date_col="order_date",
)
def region_metrics(ctx):
    return ctx.ref("transform.customer_daily_agg")
'''
        tree = ast.parse(source_code)
        result = extract_decorators(tree)
        assert result != {}
        assert result["kind"] == "second_order_aggregation"
        assert result["name"] == "region_metrics"
        assert result["function_name"] == "region_metrics"


# =============================================================================
# Executor routing tests
# =============================================================================


class TestExecutorRouting:
    """Test that Python SOA nodes are routed to SecondOrderAggregationExecutor."""

    def test_python_soa_not_routed_to_python_executor(self):
        """Python SOA nodes should NOT go to PythonExecutor."""
        from seeknal.workflow.executors import get_executor  # ty: ignore[unresolved-import]

        node = MagicMock()
        node.file_path = "/path/to/seeknal/pipelines/region_metrics.py"
        node.node_type = NodeType.SECOND_ORDER_AGGREGATION
        node.id = "second_order_aggregation.region_metrics"
        node.name = "region_metrics"
        node.config = {
            "id_col": "region",
            "feature_date_col": "order_date",
            "source": "transform.customer_daily_agg",
            "features": {"daily_amount": {"basic": ["sum"]}},
        }

        context = MagicMock()
        executor = get_executor(node, context)

        # Should be SecondOrderAggregationExecutor, not PythonExecutor
        assert isinstance(executor, SecondOrderAggregationExecutor)

    def test_python_transform_still_routed_to_python_executor(self):
        """Python transform nodes should still go to PythonExecutor."""
        from seeknal.workflow.executors import get_executor  # ty: ignore[unresolved-import]
        from seeknal.workflow.executors.python_executor import PythonExecutor  # ty: ignore[unresolved-import]

        node = MagicMock()
        node.file_path = "/path/to/seeknal/pipelines/my_transform.py"
        node.node_type = NodeType.TRANSFORM
        node.id = "transform.my_transform"
        node.name = "my_transform"
        node.config = {}

        context = MagicMock()
        executor = get_executor(node, context)

        assert isinstance(executor, PythonExecutor)


# =============================================================================
# _is_python_node() tests
# =============================================================================


class TestIsPythonNode:
    """Test _is_python_node() helper on SecondOrderAggregationExecutor."""

    def _make_executor(self, file_path=None):
        node = Node(
            id="second_order_aggregation.test",
            name="test",
            node_type=NodeType.SECOND_ORDER_AGGREGATION,
            config={
                "id_col": "id",
                "feature_date_col": "date",
                "source": "transform.upstream",
                "features": {"col": {"basic": ["sum"]}},
            },
        )
        if file_path is not None:
            node.file_path = file_path
        context = MagicMock()
        return SecondOrderAggregationExecutor(node, context)

    def test_python_file_detected(self):
        """Node from .py file is detected as Python node."""
        executor = self._make_executor("/path/to/seeknal/pipelines/soa.py")
        assert executor._is_python_node() is True

    def test_yaml_file_not_python(self):
        """Node from .yml file is not a Python node."""
        executor = self._make_executor("/path/to/seeknal/soa/region.yml")
        assert executor._is_python_node() is False

    def test_no_file_path_not_python(self):
        """Node without file_path is not a Python node."""
        executor = self._make_executor(None)
        assert executor._is_python_node() is False


# =============================================================================
# Two-phase execution tests
# =============================================================================


class TestPythonSOAExecution:
    """Test two-phase execution: Python preprocessing + SOA engine."""

    @pytest.fixture
    def soa_node_python(self):
        """Create a Python SOA node."""
        node = Node(
            id="second_order_aggregation.region_metrics",
            name="region_metrics",
            node_type=NodeType.SECOND_ORDER_AGGREGATION,
            config={
                "id_col": "region",
                "feature_date_col": "order_date",
                "application_date_col": "application_date",
                "source": "transform.customer_daily_agg",
                "features": {
                    "daily_amount": {"basic": ["sum", "mean"]},
                },
            },
        )
        node.file_path = "/path/to/seeknal/pipelines/region_metrics.py"
        return node

    @pytest.fixture
    def execution_context(self, tmp_path):
        """Create an execution context with target directory."""
        ctx = ExecutionContext(
            project_name="test_project",
            workspace_path=tmp_path,
            target_path=tmp_path / "target",
            dry_run=False,
            verbose=False,
        )
        return ctx

    def test_preprocessing_calls_python_executor(self, soa_node_python, execution_context, tmp_path):
        """_execute_python_preprocessing() delegates to PythonExecutor."""
        executor = SecondOrderAggregationExecutor(soa_node_python, execution_context)

        # Create the intermediate parquet that PythonExecutor would produce
        intermediate_dir = tmp_path / "target" / "intermediate"
        intermediate_dir.mkdir(parents=True)
        parquet_path = intermediate_dir / "second_order_aggregation_region_metrics.parquet"

        import pandas as pd  # ty: ignore[unresolved-import]
        df = pd.DataFrame({
            "region": ["north", "south"],
            "order_date": ["2026-01-15", "2026-01-16"],
            "application_date": ["2026-01-21", "2026-01-21"],
            "daily_amount": [100.0, 200.0],
        })
        df.to_parquet(parquet_path)

        # Mock PythonExecutor.execute() to return success
        mock_result = ExecutorResult(
            node_id=soa_node_python.id,
            status=ExecutionStatus.SUCCESS,
            duration_seconds=1.0,
            row_count=2,
        )

        # Patch the lazy import inside _execute_python_preprocessing
        mock_py_executor = MagicMock()
        mock_py_executor.execute.return_value = mock_result

        with patch(
            "seeknal.workflow.executors.python_executor.PythonExecutor",
            return_value=mock_py_executor,
        ):
            result_path = executor._execute_python_preprocessing()

        assert result_path == parquet_path

    def test_preprocessing_raises_on_failure(self, soa_node_python, execution_context):
        """_execute_python_preprocessing() raises on PythonExecutor failure."""
        executor = SecondOrderAggregationExecutor(soa_node_python, execution_context)

        mock_result = ExecutorResult(
            node_id=soa_node_python.id,
            status=ExecutionStatus.FAILED,
            duration_seconds=1.0,
            row_count=0,
            metadata={"stderr": "import error"},
        )

        mock_py_executor = MagicMock()
        mock_py_executor.execute.return_value = mock_result

        with patch(
            "seeknal.workflow.executors.python_executor.PythonExecutor",
            return_value=mock_py_executor,
        ):
            with pytest.raises(ExecutorExecutionError, match="Python preprocessing failed"):
                executor._execute_python_preprocessing()
