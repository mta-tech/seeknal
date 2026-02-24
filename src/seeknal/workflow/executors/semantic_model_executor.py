"""
Semantic model executor for Seeknal workflow execution.

Handles SEMANTIC_MODEL and METRIC node types. These are metadata-only nodes
that define business entities, dimensions, measures, and metrics on top of
existing transform/source nodes. They don't produce data output — they
validate the semantic definition and register it for downstream use
(e.g., by the semantic compiler for metric queries).
"""

import logging
import time
from typing import Any, Dict

from seeknal.dag.manifest import NodeType  # ty: ignore[unresolved-import]
from seeknal.workflow.executors.base import (  # ty: ignore[unresolved-import]
    BaseExecutor,
    ExecutionStatus,
    ExecutorResult,
    ExecutorValidationError,
    ExecutorExecutionError,
    register_executor,
)

logger = logging.getLogger(__name__)


@register_executor(NodeType.SEMANTIC_MODEL)
class SemanticModelExecutor(BaseExecutor):
    """
    Executor for SEMANTIC_MODEL nodes.

    Semantic models are metadata-only: they define entities, dimensions,
    and measures on top of an existing node (referenced via model: ref(...)).
    Execution validates the definition and returns success without producing
    data output.

    YAML Structure:
        kind: semantic_model
        name: orders
        model: "ref('transform.orders_cleaned')"
        entities:
          - name: order_id
            type: primary
        dimensions:
          - name: order_date
            type: time
        measures:
          - name: total_revenue
            expr: amount
            agg: sum
    """

    @property
    def node_type(self) -> NodeType:
        return NodeType.SEMANTIC_MODEL

    def validate(self) -> None:
        """Validate the semantic model configuration."""
        config = self.node.config

        # Must have a model reference
        model_ref = config.get("model", "")
        if not model_ref:
            raise ExecutorValidationError(
                self.node.id,
                "Semantic model missing 'model' reference "
                "(e.g., model: \"ref('transform.orders')\")"
            )

    def execute(self) -> ExecutorResult:
        """
        Execute semantic model validation and registration.

        Since semantic models are metadata-only, this validates the
        definition and returns success without producing data.
        """
        start_time = time.time()
        config = self.node.config
        metadata: Dict[str, Any] = {"node_type": "semantic_model"}

        # Handle dry-run mode
        if self.context.dry_run:
            return ExecutorResult(
                node_id=self.node.id,
                status=ExecutionStatus.SUCCESS,
                duration_seconds=0.0,
                row_count=0,
                metadata={"dry_run": True, **metadata},
                is_dry_run=True,
            )

        try:
            # Parse and validate the semantic model
            from seeknal.workflow.semantic.models import SemanticModel  # ty: ignore[unresolved-import]
            model = SemanticModel.from_dict(config)
            errors = model.validate()
            if errors:
                logger.warning(
                    "Semantic model '%s' validation warnings: %s",
                    self.node.name, "; ".join(errors),
                )
                metadata["validation_warnings"] = errors

            # Collect metadata
            metadata["model_ref"] = model.model_ref
            metadata["entity_count"] = len(model.entities)
            metadata["dimension_count"] = len(model.dimensions)
            metadata["measure_count"] = len(model.measures)
            metadata["metric_count"] = len(model.metrics)

            duration = time.time() - start_time
            return ExecutorResult(
                node_id=self.node.id,
                status=ExecutionStatus.SUCCESS,
                duration_seconds=duration,
                row_count=0,
                metadata=metadata,
            )

        except Exception as e:
            raise ExecutorExecutionError(
                self.node.id,
                f"Semantic model validation failed: {e}",
                e,
            ) from e


@register_executor(NodeType.METRIC)
class MetricExecutor(BaseExecutor):
    """
    Executor for METRIC nodes.

    Metrics are metadata-only: they define calculations (simple, ratio,
    cumulative, derived) on top of semantic model measures. Execution
    validates the definition and returns success.

    YAML Structure:
        kind: metric
        name: monthly_revenue
        type: simple
        measure: total_revenue
        filter: "order_date >= '2024-01-01'"
    """

    @property
    def node_type(self) -> NodeType:
        return NodeType.METRIC

    def validate(self) -> None:
        """Validate the metric configuration."""
        config = self.node.config

        metric_type = config.get("type", "")
        if not metric_type:
            raise ExecutorValidationError(
                self.node.id,
                "Metric missing 'type' field "
                "(valid: simple, ratio, cumulative, derived)"
            )

    def execute(self) -> ExecutorResult:
        """Validate metric definition and return success."""
        start_time = time.time()
        config = self.node.config
        metadata: Dict[str, Any] = {"node_type": "metric"}

        if self.context.dry_run:
            return ExecutorResult(
                node_id=self.node.id,
                status=ExecutionStatus.SUCCESS,
                duration_seconds=0.0,
                row_count=0,
                metadata={"dry_run": True, **metadata},
                is_dry_run=True,
            )

        try:
            from seeknal.workflow.semantic.models import Metric  # ty: ignore[unresolved-import]
            metric = Metric.from_dict(config)
            errors = metric.validate()
            if errors:
                logger.warning(
                    "Metric '%s' validation warnings: %s",
                    self.node.name, "; ".join(errors),
                )
                metadata["validation_warnings"] = errors

            metadata["metric_type"] = metric.type.value

            duration = time.time() - start_time
            return ExecutorResult(
                node_id=self.node.id,
                status=ExecutionStatus.SUCCESS,
                duration_seconds=duration,
                row_count=0,
                metadata=metadata,
            )

        except Exception as e:
            raise ExecutorExecutionError(
                self.node.id,
                f"Metric validation failed: {e}",
                e,
            ) from e
