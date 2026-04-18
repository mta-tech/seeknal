"""Tests for PythonExecutor runtime context injection."""

from __future__ import annotations

from pathlib import Path

from seeknal.dag.manifest import Node, NodeType
from seeknal.workflow.executors.base import ExecutionContext
from seeknal.workflow.executors.python_executor import PythonExecutor


def test_generate_runner_script_injects_params_and_node_metadata(tmp_path):
    pipeline_file = tmp_path / "fetch_pages.py"
    pipeline_file.write_text(
        "# /// script\n# requires-python = \">=3.11\"\n# dependencies = [\n#     \"seeknal\",\n# ]\n# ///\n"
        "from seeknal.pipeline import transform\n\n"
        "@transform(name='fetch_pages')\n"
        "def fetch_pages(ctx):\n"
        "    return []\n"
    )

    node = Node(
        id="transform.fetch_pages",
        name="fetch_pages",
        node_type=NodeType.TRANSFORM,
        config={
            "params": {"timeout": 15},
            "profile_config": {"path": ":memory:"},
        },
        file_path=str(pipeline_file),
    )
    context = ExecutionContext(
        project_name="test_project",
        workspace_path=tmp_path,
        target_path=tmp_path / "target",
        params={"session_name": "nightly"},
    )

    script = PythonExecutor(node, context)._generate_runner_script()

    assert "params={'timeout': 15, 'session_name': 'nightly'}" in script
    assert 'node_id="transform.fetch_pages"' in script
    assert 'node_kind="transform"' in script
    assert "node_meta={'params': {'timeout': 15}, 'profile_config': {'path': ':memory:'}}" in script

