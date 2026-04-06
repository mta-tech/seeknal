"""Tests for seeknal ask subagent configurations."""

import pytest

from seeknal.ask.agents.subagents import (
    create_pipeline_toolset,
    get_subagent_configs,
)


class TestCreatePipelineToolset:
    def test_returns_function_toolset(self):
        from pydantic_ai.toolsets import FunctionToolset

        ts = create_pipeline_toolset()
        assert isinstance(ts, FunctionToolset)

    def test_has_correct_id(self):
        ts = create_pipeline_toolset()
        assert ts.id == "seeknal-pipeline"

    def test_has_exactly_four_tools(self):
        ts = create_pipeline_toolset()
        assert len(ts.tools) == 4

    def test_has_expected_tool_names(self):
        ts = create_pipeline_toolset()
        tool_names = set(ts.tools.keys())
        assert tool_names == {
            "search_pipelines",
            "read_pipeline",
            "search_project_files",
            "read_project_file",
        }

    def test_does_not_have_execute_tools(self):
        """Pipeline toolset must NOT include execute_sql or execute_python."""
        ts = create_pipeline_toolset()
        tool_names = set(ts.tools.keys())
        assert "execute_sql" not in tool_names
        assert "execute_python" not in tool_names


class TestGetSubagentConfigs:
    def test_returns_list(self):
        configs = get_subagent_configs()
        assert isinstance(configs, list)

    def test_has_one_config(self):
        configs = get_subagent_configs()
        assert len(configs) == 1

    def test_lineage_investigator_name(self):
        configs = get_subagent_configs()
        assert configs[0]["name"] == "lineage_investigator"

    def test_lineage_investigator_has_description(self):
        configs = get_subagent_configs()
        desc = configs[0]["description"]
        assert "lineage" in desc.lower()
        assert "pipeline" in desc.lower()

    def test_lineage_investigator_has_instructions(self):
        configs = get_subagent_configs()
        instr = configs[0]["instructions"]
        assert len(instr) > 50
        assert "lineage" in instr.lower()

    def test_lineage_investigator_has_toolsets(self):
        configs = get_subagent_configs()
        assert "toolsets" in configs[0]
        assert len(configs[0]["toolsets"]) == 1

    def test_lineage_toolset_is_pipeline_only(self):
        """Subagent toolset should be the restricted pipeline toolset."""
        configs = get_subagent_configs()
        ts = configs[0]["toolsets"][0]
        tool_names = set(ts.tools.keys())
        assert "execute_sql" not in tool_names
        assert "execute_python" not in tool_names
        assert "search_pipelines" in tool_names
