from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import patch

import duckdb
import pandas as pd

from seeknal.dag.manifest import Node, NodeType
from seeknal.workflow.executors.base import ExecutorResult, ExecutionStatus
from seeknal.workflow.executors.feature_group_executor import (
    FeatureGroupExecutor,
    _duckdb_compatible_frame,
    _event_time_column,
)
from seeknal.workflow.materialization.dispatcher import DispatchResult


def _executor_with_connection(connection):
    return SimpleNamespace(
        context=SimpleNamespace(get_duckdb_connection=lambda: connection)
    )


def test_qualified_source_transform_uses_existing_execution_view():
    connection = duckdb.connect()
    connection.execute("CREATE SCHEMA source")
    connection.execute(
        """
        CREATE VIEW source.customers AS
        SELECT 'c-001'::VARCHAR AS customer_id, 41::INTEGER AS age
        """
    )

    result = FeatureGroupExecutor._apply_transform_duckdb(
        _executor_with_connection(connection),
        pd.DataFrame({"customer_id": pd.Series(["ignored"], dtype="str")}),
        "SELECT customer_id, age FROM source.customers",
    )

    assert result.to_dict(orient="records") == [
        {"customer_id": "c-001", "age": 41}
    ]


def test_unqualified_source_transform_accepts_pandas_string_dtype():
    connection = duckdb.connect()
    frame = pd.DataFrame(
        {
            "customer_id": pd.Series(["c-001"], dtype="str"),
            "age": [41],
        }
    )

    result = FeatureGroupExecutor._apply_transform_duckdb(
        _executor_with_connection(connection),
        frame,
        "SELECT customer_id, age + 1 AS age FROM source",
    )

    assert result.to_dict(orient="records") == [
        {"customer_id": "c-001", "age": 42}
    ]


def test_duckdb_view_frame_normalizes_pandas_string_dtype():
    frame = pd.DataFrame({"customer_id": pd.Series(["c-001"], dtype="str")})

    compatible = _duckdb_compatible_frame(frame)

    connection = duckdb.connect()
    connection.register("feature_rows", compatible)
    assert connection.execute(
        "SELECT customer_id FROM feature_rows"
    ).fetchall() == [("c-001",)]


def test_atlas_online_event_time_is_used_by_feature_group_execution():
    assert _event_time_column(
        {
            "materialization": {"offline": True},
            "materializations": [
                {
                    "type": "atlas_online",
                    "event_time_column": "observed_at",
                }
            ],
        }
    ) == "observed_at"


def _materializing_executor(targets, *, run_id="run-123"):
    node = Node(
        id="feature_group.customer_profile",
        name="customer_profile",
        node_type=NodeType.FEATURE_GROUP,
        config={
            "entity": {"join_keys": ["customer_id"]},
            "materializations": targets,
        },
    )
    context = SimpleNamespace(
        verbose=False,
        materialize_enabled=None,
        config={
            "_seeknal_node_fingerprints": {
                node.id: {
                    "combined": "revision-sha",
                    "content_hash": "definition-sha",
                    "schema_hash": "schema-sha",
                }
            },
            "_seeknal_run_id": run_id,
        },
        profile_path=None,
        env_name=None,
        get_duckdb_connection=lambda: object(),
    )
    return FeatureGroupExecutor(node, cast(Any, context))


def _successful_execution_result():
    return ExecutorResult(
        node_id="feature_group.customer_profile",
        status=ExecutionStatus.SUCCESS,
    )


def test_failed_atlas_online_dispatch_marks_feature_group_failed():
    executor = _materializing_executor([{"type": "atlas_online"}])
    dispatch_result = DispatchResult(
        total=1,
        failed=1,
        results=[
            {
                "target": "feature_group.customer_profile[0]:atlas_online",
                "type": "atlas_online",
                "success": False,
                "error": "Atlas publish rejected revision",
            }
        ],
    )

    with patch(
        "seeknal.workflow.materialization.dispatcher.MaterializationDispatcher.dispatch",
        return_value=dispatch_result,
    ):
        result = executor.post_execute(_successful_execution_result())

    assert result.status is ExecutionStatus.FAILED
    assert result.error_message == "Atlas publish rejected revision"
    assert result.metadata["materialization"]["results"] == dispatch_result.results


def test_atlas_online_dispatch_exception_marks_feature_group_failed():
    executor = _materializing_executor([{"type": "atlas_online"}])

    with patch(
        "seeknal.workflow.materialization.dispatcher.MaterializationDispatcher.dispatch",
        side_effect=RuntimeError("Atlas connection reset"),
    ):
        result = executor.post_execute(_successful_execution_result())

    assert result.status is ExecutionStatus.FAILED
    assert result.error_message == "Atlas connection reset"
    assert result.metadata["materialization"]["error"] == "Atlas connection reset"


def test_atlas_online_requires_injected_run_id_before_dispatch():
    executor = _materializing_executor([{"type": "atlas_online"}])
    executor.context.config.pop("_seeknal_run_id")

    with patch(
        "seeknal.workflow.materialization.dispatcher.MaterializationDispatcher.dispatch"
    ) as dispatch:
        result = executor.post_execute(_successful_execution_result())

    dispatch.assert_not_called()
    assert result.status is ExecutionStatus.FAILED
    assert result.error_message == (
        "injected _seeknal_run_id is required for atlas_online materialization"
    )
    assert result.metadata["materialization"]["success"] is False


def test_failed_optional_iceberg_dispatch_remains_best_effort():
    executor = _materializing_executor([{"type": "iceberg", "table": "a.b.c"}])
    dispatch_result = DispatchResult(
        total=1,
        failed=1,
        results=[
            {
                "target": "feature_group.customer_profile[0]:iceberg",
                "type": "iceberg",
                "success": False,
                "error": "Iceberg catalog unavailable",
            }
        ],
    )

    with patch(
        "seeknal.workflow.materialization.dispatcher.MaterializationDispatcher.dispatch",
        return_value=dispatch_result,
    ):
        result = executor.post_execute(_successful_execution_result())

    assert result.status is ExecutionStatus.SUCCESS
    assert result.error_message is None
    assert result.metadata["materialization"]["success"] is False
