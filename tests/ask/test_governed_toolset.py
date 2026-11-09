"""Tests for governed Ask toolset (TASK-SPEC.md § Governed mode)."""

from __future__ import annotations

import pytest

from seeknal.ask.agents.tools.toolset import (
    GOVERNED_TOOL_ALLOWLIST,
    GOVERNED_TOOL_NAMES,
    create_ask_toolset,
)


def _tool_names(toolset) -> set[str]:
    return set(toolset.tools.keys())


EXPECTED_GOVERNED_TOOLS = {
    # Database analysis tools
    "execute_sql",
    "preview_query",
    "list_tables",
    "describe_table",
    # Visualization tool
    "visualize_chart",
    # Read-only context tools
    "list_source_context",
    "read_source_context",
    "list_sql_pairs",
    "read_sql_pair",
    "execute_sql_pair",
    "list_ask_tests",
    "read_ask_test",
    "run_ask_test",
    "list_ask_test_results",
    "read_ask_test_result",
    # Memory read tools
    "list_context_files",
    "read_project_file",
}


def test_governed_tool_names_equal_exact_allowlist():
    assert GOVERNED_TOOL_NAMES == EXPECTED_GOVERNED_TOOLS
    assert len(EXPECTED_GOVERNED_TOOLS) == 17

    # Explicit parameter
    toolset = create_ask_toolset(governed=True)
    assert _tool_names(toolset) == EXPECTED_GOVERNED_TOOLS


def test_governed_toolset_via_env(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("ATLAS_ASK_MODE", "governed")
    toolset = create_ask_toolset()
    assert _tool_names(toolset) == EXPECTED_GOVERNED_TOOLS


def test_governed_toolset_excludes_python_write_upload_and_web_tools():
    toolset = create_ask_toolset(
        governed=True,
        include_ask_user=True,
        include_request_clarification=True,
        include_forecast=True,
        include_anomaly=True,
        include_upload_to_s3=True,
        include_visualize_chart=True,
    )
    names = _tool_names(toolset)

    # Allowlist is strictly enforced
    assert names == EXPECTED_GOVERNED_TOOLS

    # Specifically excluded tools
    assert "execute_python" not in names
    assert "write_project_file" not in names
    assert "save_preference" not in names
    assert "upload_to_s3" not in names
    assert "run_forecast" not in names
    assert "detect_anomaly" not in names
    assert "run_pipeline" not in names
    assert "plan_pipeline" not in names
    assert "draft_node" not in names
    assert "publish_to_proof" not in names
    assert "publish_to_seeknal_report" not in names
    assert "generate_report" not in names
    assert "ask_user" not in names
    assert "request_clarification" not in names


def test_non_governed_toolsets_unchanged(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("ATLAS_ASK_MODE", raising=False)

    # Standard analysis mode retains execute_python and memory write tools
    analysis_names = _tool_names(create_ask_toolset(mode="analysis", include_ask_user=False))
    assert "execute_python" in analysis_names
    assert "write_project_file" in analysis_names
    assert "save_preference" in analysis_names
    assert "execute_sql" in analysis_names
    assert "visualize_chart" not in analysis_names

    # Standard full mode retains write, build, pipeline and reporting tools
    full_names = _tool_names(create_ask_toolset(mode="full", include_ask_user=False))
    assert "run_pipeline" in full_names
    assert "write_project_file" in full_names
    assert "generate_report" in full_names
    assert "execute_python" in full_names
