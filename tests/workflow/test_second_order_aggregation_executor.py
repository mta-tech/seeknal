"""
Tests for SecondOrderAggregationExecutor.

Tests the second-order aggregation node executor for aggregations of aggregations.
"""

import pytest
from pathlib import Path
from seeknal.workflow.executors.base import (
    ExecutionContext,
    ExecutionStatus,
    ExecutorValidationError,
    ExecutorExecutionError,
)
from seeknal.workflow.executors.second_order_aggregation_executor import SecondOrderAggregationExecutor
from seeknal.dag.manifest import Node, NodeType


@pytest.fixture
def sample_second_order_aggregation_node():
    """Create a sample second-order aggregation node."""
    return Node(
        id="second_order_aggregation.region_user_metrics",
        name="region_user_metrics",
        node_type=NodeType.SECOND_ORDER_AGGREGATION,
        config={
            "id_col": "region_id",
            "feature_date_col": "date",
            "application_date_col": "application_date",
            "source": "aggregation.user_daily_features",
            "features": {
                "total_users": {
                    "basic": ["count"]
                },
                "avg_user_spend_30d": {
                    "basic": ["mean", "stddev"],
                    "source_feature": "total_spend_30d"
                },
                "weekly_total": {
                    "window": [7, 7],
                    "basic": ["sum"],
                    "source_feature": "daily_volume"
                }
            }
        },
        description="Aggregate user features to region level",
        owner="data-science",
        tags=["second-order", "analytics"],
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


class TestSecondOrderAggregationExecutorValidation:
    """Test SecondOrderAggregationExecutor validation."""

    def test_validate_success(self, sample_second_order_aggregation_node, execution_context):
        """Test validation succeeds with valid config."""
        executor = SecondOrderAggregationExecutor(
            sample_second_order_aggregation_node,
            execution_context
        )

        # Should not raise
        executor.validate()

    def test_validate_missing_id_col(self, execution_context):
        """Test validation fails when id_col is missing."""
        node = Node(
            id="second_order_aggregation.invalid",
            name="invalid",
            node_type=NodeType.SECOND_ORDER_AGGREGATION,
            config={
                # Missing id_col
                "feature_date_col": "date",
                "source": "aggregation.user_metrics",
                "features": {}
            },
        )

        executor = SecondOrderAggregationExecutor(node, execution_context)

        with pytest.raises(ExecutorValidationError) as exc_info:
            executor.validate()

        assert "Missing required field: id_col" in str(exc_info.value)

    def test_validate_missing_feature_date_col(self, execution_context):
        """Test validation fails when feature_date_col is missing."""
        node = Node(
            id="second_order_aggregation.invalid",
            name="invalid",
            node_type=NodeType.SECOND_ORDER_AGGREGATION,
            config={
                "id_col": "region_id",
                # Missing feature_date_col
                "source": "aggregation.user_metrics",
                "features": {}
            },
        )

        executor = SecondOrderAggregationExecutor(node, execution_context)

        with pytest.raises(ExecutorValidationError) as exc_info:
            executor.validate()

        assert "Missing required field: feature_date_col" in str(exc_info.value)

    def test_validate_missing_source(self, execution_context):
        """Test validation fails when source is missing."""
        node = Node(
            id="second_order_aggregation.invalid",
            name="invalid",
            node_type=NodeType.SECOND_ORDER_AGGREGATION,
            config={
                "id_col": "region_id",
                "feature_date_col": "date",
                # Missing source
                "features": {}
            },
        )

        executor = SecondOrderAggregationExecutor(node, execution_context)

        with pytest.raises(ExecutorValidationError) as exc_info:
            executor.validate()

        assert "Missing required field: source" in str(exc_info.value)

    def test_validate_invalid_source_format(self, execution_context):
        """Test validation fails with invalid source reference format."""
        node = Node(
            id="second_order_aggregation.invalid",
            name="invalid",
            node_type=NodeType.SECOND_ORDER_AGGREGATION,
            config={
                "id_col": "region_id",
                "feature_date_col": "date",
                "source": "invalid_format",  # No dot separator
                "features": {}
            },
        )

        executor = SecondOrderAggregationExecutor(node, execution_context)

        with pytest.raises(ExecutorValidationError) as exc_info:
            executor.validate()

        assert "Invalid source reference" in str(exc_info.value)
        assert "kind.name" in str(exc_info.value)

    def test_validate_empty_features(self, execution_context):
        """Test validation fails when features is empty."""
        node = Node(
            id="second_order_aggregation.invalid",
            name="invalid",
            node_type=NodeType.SECOND_ORDER_AGGREGATION,
            config={
                "id_col": "region_id",
                "feature_date_col": "date",
                "source": "aggregation.user_metrics",
                "features": {}  # Empty features
            },
        )

        executor = SecondOrderAggregationExecutor(node, execution_context)

        with pytest.raises(ExecutorValidationError) as exc_info:
            executor.validate()

        assert "Features must be a non-empty dictionary" in str(exc_info.value)

    def test_validate_feature_without_aggregation_type(self, execution_context):
        """Test validation fails when feature has no aggregation type."""
        node = Node(
            id="second_order_aggregation.invalid",
            name="invalid",
            node_type=NodeType.SECOND_ORDER_AGGREGATION,
            config={
                "id_col": "region_id",
                "feature_date_col": "date",
                "source": "aggregation.user_metrics",
                "features": {
                    "some_feature": {
                        # No aggregation type (basic, window, ratio, etc.)
                        "description": "test"
                    }
                }
            },
        )

        executor = SecondOrderAggregationExecutor(node, execution_context)

        with pytest.raises(ExecutorValidationError) as exc_info:
            executor.validate()

        assert "must have at least one aggregation type" in str(exc_info.value)

    def test_validate_invalid_window_format(self, execution_context):
        """Test validation fails with invalid window format during execution."""
        from seeknal.workflow.executors.base import ExecutorExecutionError

        node = Node(
            id="second_order_aggregation.invalid",
            name="invalid",
            node_type=NodeType.SECOND_ORDER_AGGREGATION,
            config={
                "id_col": "region_id",
                "feature_date_col": "date",
                "source": "aggregation.user_metrics",
                "features": {
                    "weekly_total": {
                        "window": "invalid",  # Not a list
                        "basic": ["sum"]
                    }
                }
            },
        )

        executor = SecondOrderAggregationExecutor(node, execution_context)

        # Window validation happens during execution, not in validate()
        with pytest.raises(ExecutorExecutionError) as exc_info:
            executor._build_aggregation_specs(node.config)

        assert "window must be a list of two integers" in str(exc_info.value)

    def test_validate_invalid_ratio_format(self, execution_context):
        """Test validation fails with invalid ratio format during execution."""
        from seeknal.workflow.executors.base import ExecutorExecutionError

        node = Node(
            id="second_order_aggregation.invalid",
            name="invalid",
            node_type=NodeType.SECOND_ORDER_AGGREGATION,
            config={
                "id_col": "region_id",
                "feature_date_col": "date",
                "source": "aggregation.user_metrics",
                "features": {
                    "ratio_feature": {
                        "ratio": {
                            "numerator": "invalid",  # Not a list
                            "denominator": [8, 30],
                            "aggs": ["sum"]
                        }
                    }
                }
            },
        )

        executor = SecondOrderAggregationExecutor(node, execution_context)

        # Ratio validation happens during execution, not in validate()
        with pytest.raises(ExecutorExecutionError) as exc_info:
            executor._build_aggregation_specs(node.config)

        assert "numerator must be a list of two integers" in str(exc_info.value)


class TestSecondOrderAggregationExecutorExecution:
    """Test SecondOrderAggregationExecutor execution."""

    def test_execute_dry_run(self, sample_second_order_aggregation_node, execution_context):
        """Test dry-run execution returns success without actual data."""
        execution_context.dry_run = True

        executor = SecondOrderAggregationExecutor(
            sample_second_order_aggregation_node,
            execution_context
        )

        result = executor.execute()

        assert result.status == ExecutionStatus.SUCCESS
        assert result.is_dry_run is True
        assert result.metadata["source"] == "aggregation.user_daily_features"
        assert result.metadata["feature_count"] == 3

    def test_build_aggregation_specs_basic(self, sample_second_order_aggregation_node, execution_context):
        """Test building aggregation specs for basic aggregations."""
        executor = SecondOrderAggregationExecutor(
            sample_second_order_aggregation_node,
            execution_context
        )

        specs = executor._build_aggregation_specs(sample_second_order_aggregation_node.config)

        # total_users: [count] = 1 spec
        # avg_user_spend_30d: [mean, stddev] = 2 specs
        # weekly_total: window creates both basic and basic_days = 2 specs
        assert len(specs) == 5
        assert specs[0].name == "basic"
        assert specs[0].aggregations == "count"
        assert specs[1].aggregations == "mean"
        assert specs[2].aggregations == "stddev"

    def test_build_aggregation_specs_window(self, execution_context):
        """Test building aggregation specs for window aggregations."""
        config = {
            "id_col": "region_id",
            "feature_date_col": "date",
            "source": "aggregation.user_metrics",
            "features": {
                "weekly_total": {
                    "window": [7, 7],
                    "basic": ["sum"],
                    "source_feature": "daily_volume"
                }
            }
        }

        node = Node(
            id="second_order_aggregation.test",
            name="test",
            node_type=NodeType.SECOND_ORDER_AGGREGATION,
            config=config,
        )

        executor = SecondOrderAggregationExecutor(node, execution_context)
        specs = executor._build_aggregation_specs(config)

        # Window creates 2 specs: basic + basic_days
        assert len(specs) == 2
        # First spec is the basic aggregation
        assert specs[0].name == "basic"
        assert specs[0].aggregations == "sum"
        # Second spec is the window-specific aggregation
        assert specs[1].name == "basic_days"
        assert specs[1].dayLimitLower1 == "7"
        assert specs[1].dayLimitUpper1 == "7"

    def test_build_aggregation_specs_ratio(self, execution_context):
        """Test building aggregation specs for ratio aggregations."""
        config = {
            "id_col": "region_id",
            "feature_date_col": "date",
            "source": "aggregation.user_metrics",
            "features": {
                "recent_vs_historical": {
                    "ratio": {
                        "numerator": [1, 7],
                        "denominator": [8, 30],
                        "aggs": ["sum"]
                    },
                    "source_feature": "transaction_amount"
                }
            }
        }

        node = Node(
            id="second_order_aggregation.test",
            name="test",
            node_type=NodeType.SECOND_ORDER_AGGREGATION,
            config=config,
        )

        executor = SecondOrderAggregationExecutor(node, execution_context)
        specs = executor._build_aggregation_specs(config)

        assert len(specs) == 1
        assert specs[0].name == "ratio"
        assert specs[0].dayLimitLower1 == "1"
        assert specs[0].dayLimitUpper1 == "7"
        assert specs[0].dayLimitLower2 == "8"
        assert specs[0].dayLimitUpper2 == "30"

    def test_get_feature_summary(self, sample_second_order_aggregation_node, execution_context):
        """Test getting feature summary."""
        executor = SecondOrderAggregationExecutor(
            sample_second_order_aggregation_node,
            execution_context
        )

        summary = executor._get_feature_summary()

        assert len(summary) == 3
        assert summary[0]["name"] == "total_users"
        assert summary[0]["total_aggregations"] == 1
        assert summary[1]["name"] == "avg_user_spend_30d"
        assert summary[1]["total_aggregations"] == 2


class TestSecondOrderAggregationExecutorIntegration:
    """Integration tests for SecondOrderAggregationExecutor."""

    def test_node_type_property(self, sample_second_order_aggregation_node, execution_context):
        """Test node_type property returns correct type."""
        executor = SecondOrderAggregationExecutor(
            sample_second_order_aggregation_node,
            execution_context
        )

        assert executor.node_type == NodeType.SECOND_ORDER_AGGREGATION

    def test_post_execute_adds_metadata(self, sample_second_order_aggregation_node, execution_context):
        """Test post_execute adds metadata to result."""
        from seeknal.workflow.executors.base import ExecutorResult

        executor = SecondOrderAggregationExecutor(
            sample_second_order_aggregation_node,
            execution_context
        )

        input_result = ExecutorResult(
            node_id=sample_second_order_aggregation_node.id,
            status=ExecutionStatus.SUCCESS,
            duration_seconds=1.0,
            row_count=100,
        )

        result = executor.post_execute(input_result)

        assert result.metadata["executor_version"] == "1.0.0"
        assert result.status == ExecutionStatus.SUCCESS
