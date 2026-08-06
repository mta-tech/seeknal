from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest

from seeknal.dag.manifest import Manifest, Node, NodeType
from seeknal.workflow.executors.base import (
    ExecutionContext,
    ExecutorResult,
    ExecutionStatus as ExecutorExecutionStatus,
)
from seeknal.workflow.runner import DAGRunner, ExecutionStatus


def _feature_group_manifest():
    manifest = Manifest(project="test")
    manifest.add_node(
        Node(
            id="feature_group.customer_profile",
            name="customer_profile",
            node_type=NodeType.FEATURE_GROUP,
            tags=["serving"],
            config={
                "features": {"score": "double"},
                "materializations": [{"type": "atlas_online"}],
            },
        )
    )
    return manifest


@pytest.mark.parametrize(
    "selection",
    [
        {"nodes": ["customer_profile"]},
        {"tags": ["serving"]},
        {"nodes": ["customer_profile"], "tags": ["serving"]},
    ],
)
def test_targeted_selection_computes_fingerprints(selection, tmp_path):
    runner = DAGRunner(
        _feature_group_manifest(),
        target_path=tmp_path / "target",
    )

    to_run, _ = runner._get_nodes_to_run(**selection)

    assert to_run == {"feature_group.customer_profile"}
    assert runner._current_fingerprints[
        "feature_group.customer_profile"
    ].combined


def test_diff_selection_computes_fingerprints(tmp_path):
    runner = DAGRunner(
        _feature_group_manifest(),
        target_path=tmp_path / "target",
    )
    runner.diff = cast(
        Any,
        SimpleNamespace(
            get_nodes_to_rebuild=lambda manifest: {
                "feature_group.customer_profile": "modified"
            }
        ),
    )

    to_run, _ = runner._get_nodes_to_run()

    assert to_run == {"feature_group.customer_profile"}
    assert runner._current_fingerprints[
        "feature_group.customer_profile"
    ].combined


def test_executor_failure_result_marks_runner_node_failed(tmp_path):
    context = ExecutionContext(
        project_name="test",
        workspace_path=tmp_path,
        target_path=tmp_path / "target",
    )
    runner = DAGRunner(
        _feature_group_manifest(),
        target_path=tmp_path / "target",
        exec_context=context,
    )
    runner._get_nodes_to_run(nodes=["customer_profile"])
    executor = MagicMock()
    executor.run.return_value = ExecutorResult(
        node_id="feature_group.customer_profile",
        status=ExecutorExecutionStatus.FAILED,
        error_message="Atlas publish rejected revision",
        metadata={"materialization": {"success": False}},
    )

    with patch("seeknal.workflow.executors.get_executor", return_value=executor):
        result = runner._execute_node("feature_group.customer_profile")

    assert result.status is ExecutionStatus.FAILED
    assert result.error_message == "Atlas publish rejected revision"
    assert result.metadata["materialization"]["success"] is False


def test_failed_run_persists_materialization_evidence(tmp_path):
    context = ExecutionContext(
        project_name="test",
        workspace_path=tmp_path,
        target_path=tmp_path / "target",
    )
    runner = DAGRunner(
        _feature_group_manifest(),
        target_path=tmp_path / "target",
        exec_context=context,
    )
    executor = MagicMock()
    executor.run.return_value = ExecutorResult(
        node_id="feature_group.customer_profile",
        status=ExecutorExecutionStatus.FAILED,
        error_message="Atlas publish rejected revision",
        metadata={
            "materialization": {
                "success": False,
                "results": [
                    {
                        "type": "atlas_online",
                        "success": False,
                        "error": "Atlas publish rejected revision",
                    }
                ],
            }
        },
    )

    with (
        patch("seeknal.workflow.executors.get_executor", return_value=executor),
        patch.object(runner, "_consolidate_entities"),
        patch.object(runner, "_append_dq_history"),
    ):
        summary = runner.run(nodes=["customer_profile"])

    assert summary.failed_nodes == 1
    node_state = runner.run_state.nodes["feature_group.customer_profile"]
    assert node_state.status == "failed"
    assert node_state.metadata["error"] == "Atlas publish rejected revision"
    assert node_state.metadata["materialization"]["results"][0]["error"] == (
        "Atlas publish rejected revision"
    )
