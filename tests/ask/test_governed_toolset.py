"""Tests for governed Ask toolset (TASK-SPEC.md § Governed mode)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from seeknal.ask.agents.tools._context import ToolContext, set_tool_context
from seeknal.ask.agents.tools.toolset import (
    GOVERNED_TOOL_ALLOWLIST,
    GOVERNED_TOOL_NAMES,
    create_ask_toolset,
)
from seeknal.ask.governed import (
    MAX_GOVERNED_FILE_SIZE,
    read_project_file_governed,
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
    # Read-only context tools (read_source_context removed: contains ungoverned sample rows)
    "list_source_context",
    "list_sql_pairs",
    "read_sql_pair",
    "execute_sql_pair",
    "list_ask_tests",
    "read_ask_test",
    "run_ask_test",
    "list_ask_test_results",
    "read_ask_test_result",
    # Memory read tools (read_project_file replaced by governed variant)
    "list_context_files",
    "read_project_file_governed",
}


def test_governed_tool_names_equal_exact_allowlist():
    assert GOVERNED_TOOL_NAMES == EXPECTED_GOVERNED_TOOLS
    assert len(EXPECTED_GOVERNED_TOOLS) == 16

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
    assert "read_source_context" not in names
    assert "read_project_file" not in names
    assert "read_project_file_governed" in names
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

    # Standard analysis mode retains execute_python, read_source_context, and memory write tools
    analysis_names = _tool_names(create_ask_toolset(mode="analysis", include_ask_user=False))
    assert "execute_python" in analysis_names
    assert "read_source_context" in analysis_names
    assert "read_project_file" in analysis_names
    assert "read_project_file_governed" not in analysis_names
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
    assert "read_source_context" in full_names
    assert "read_project_file" in full_names
    assert "read_project_file_governed" not in full_names


def test_read_project_file_governed_permissions(tmp_path: Path):
    ctx = ToolContext(
        repl=MagicMock(),
        artifact_discovery=MagicMock(),
        project_path=tmp_path,
        is_governed=True,
    )
    set_tool_context(ctx)

    # 1. Refuses .csv
    csv_file = tmp_path / "orders.csv"
    csv_file.write_text("id,amount\n1,100", encoding="utf-8")
    res = read_project_file_governed("orders.csv")
    assert "Error" in res and (".csv" in res or "not permitted" in res)

    # 2. Refuses file under target/
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    target_file = target_dir / "compiled.sql"
    target_file.write_text("SELECT 1;", encoding="utf-8")
    res = read_project_file_governed("target/compiled.sql")
    assert "Error" in res and ("target" in res or "prohibited" in res)

    # 3. Refuses file under data/
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    data_file = data_dir / "notes.md"
    data_file.write_text("# Notes", encoding="utf-8")
    res = read_project_file_governed("data/notes.md")
    assert "Error" in res and ("data" in res or "prohibited" in res)

    # 4. Refuses file under seeds/
    seeds_dir = tmp_path / "seeds"
    seeds_dir.mkdir()
    seeds_file = seeds_dir / "lookup.sql"
    seeds_file.write_text("SELECT 1;", encoding="utf-8")
    res = read_project_file_governed("seeds/lookup.sql")
    assert "Error" in res and ("seeds" in res or "prohibited" in res)

    # 5. Refuses YAML outside seeknal/ folder (when not root seeknal_project.yml)
    other_dir = tmp_path / "other"
    other_dir.mkdir()
    other_yaml = other_dir / "config.yml"
    other_yaml.write_text("key: value", encoding="utf-8")
    res = read_project_file_governed("other/config.yml")
    assert "Error" in res and "restricted" in res.lower()

    # 6. Refuses file larger than 256 KiB
    large_dir = tmp_path / "seeknal" / "transforms"
    large_dir.mkdir(parents=True)
    large_file = large_dir / "huge.sql"
    large_file.write_text("x" * (MAX_GOVERNED_FILE_SIZE + 1024), encoding="utf-8")
    res = read_project_file_governed("seeknal/transforms/huge.sql")
    assert "File too large" in res

    # 7. Allows seeknal/transforms/x.yml
    transform_file = large_dir / "x.yml"
    transform_file.write_text("kind: transform\nname: x", encoding="utf-8")
    res = read_project_file_governed("seeknal/transforms/x.yml")
    assert "kind: transform" in res
    assert "name: x" in res

    # 8. Allows root seeknal_project.yml
    proj_file = tmp_path / "seeknal_project.yml"
    proj_file.write_text("project: test_proj", encoding="utf-8")
    res = read_project_file_governed("seeknal_project.yml")
    assert "project: test_proj" in res
