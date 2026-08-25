"""Tests for the opt-in typed action-delivery harness path."""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from pydantic_ai import Agent
from pydantic_ai.messages import (
    ModelMessage,
    ModelRequest,
    ModelResponse,
    TextPart,
    ToolCallPart,
    ToolReturnPart,
)
from pydantic_ai.models.function import AgentInfo, FunctionModel

from seeknal.ask.agents.actions import (
    AskUserAction,
    VisualizeAction,
    WriteReport,
    action_output_types,
)
from seeknal.ask.agents.skills import (
    SkillCapabilities,
    action_capabilities,
    derive_loaded_skills,
    prepare_regular_action_tools,
    prepare_action_output_tools,
)
from seeknal.ask.agents.tools.toolset import create_ask_toolset
from seeknal.ask.agents.tools.write_report import write_report as deliver_report
from seeknal.ask.config import get_action_delivery_enabled


def _create_agent_kwargs(project_path: Path, config: str = "") -> dict:
    if config:
        (project_path / "seeknal_agent.yml").write_text(config, encoding="utf-8")

    with (
        patch("seeknal.cli.repl.REPL") as mock_repl_cls,
        patch("seeknal.ask.security.configure_safe_connection"),
        patch("seeknal.ask.modules.artifact_discovery.service.ArtifactDiscovery"),
        patch("seeknal.ask.agents.providers.get_model_string", return_value="test:model"),
        patch("seeknal.ask.agents.context_toolset.SeeknaContextToolset"),
        patch("pydantic_deep.create_deep_agent") as mock_create,
        patch("pydantic_deep.DeepAgentDeps"),
    ):
        mock_repl_cls.return_value = MagicMock(conn=MagicMock())
        mock_create.return_value = MagicMock()

        from seeknal.ask.agents.agent import create_agent

        create_agent(project_path=project_path, environment="interactive")
        return mock_create.call_args.kwargs


def test_action_delivery_defaults_off_without_new_agent_kwargs(tmp_path: Path) -> None:
    kwargs = _create_agent_kwargs(tmp_path)

    assert "output_type" not in kwargs
    assert "prepare_output_tools" not in kwargs


def test_action_delivery_flag_adds_typed_outputs_and_preparer(tmp_path: Path) -> None:
    kwargs = _create_agent_kwargs(
        tmp_path,
        "agent_harness:\n  action_delivery:\n    enabled: true\n    consumer: iba-premises-worker\n",
    )

    assert {output.name for output in kwargs["output_type"]} == {"ask_user", "visualize"}
    assert callable(kwargs["prepare_output_tools"])
    assert callable(kwargs["prepare_tools"])
    assert any("action_skills" in path for path in kwargs["skill_directories"])

    capabilities = action_capabilities(kwargs["skill_directories"])
    assert capabilities["report"].actions == frozenset({"write_report"})


def test_action_delivery_fails_closed_without_its_premises_worker_consumer(
    tmp_path: Path,
) -> None:
    (tmp_path / "seeknal_agent.yml").write_text(
        "agent_harness:\n  action_delivery:\n    enabled: true\n",
        encoding="utf-8",
    )
    from seeknal.ask.agents.agent import create_agent

    with (
        patch("seeknal.cli.repl.REPL") as mock_repl_cls,
        pytest.raises(ValueError, match="consumer: iba-premises-worker"),
    ):
        create_agent(project_path=tmp_path, environment="interactive")

    mock_repl_cls.assert_not_called()


def test_action_delivery_configuration_requires_the_named_consumer() -> None:
    with pytest.raises(ValueError, match="consumer: iba-premises-worker"):
        get_action_delivery_enabled(
            {"agent_harness": {"action_delivery": {"enabled": True}}}
        )

    assert get_action_delivery_enabled({}) is False
    assert get_action_delivery_enabled(
        {
            "agent_harness": {
                "action_delivery": {
                    "enabled": True,
                    "consumer": "iba-premises-worker",
                }
            }
        }
    ) is True


def test_action_delivery_replaces_blocking_ask_user_function_tool() -> None:
    tool_names = set(
        create_ask_toolset(
            mode="full",
            include_ask_user=True,
            action_delivery=True,
        ).tools
    )

    assert "ask_user" not in tool_names


def test_core_actions_are_always_offered_as_typed_output_tools() -> None:
    observed: list[list[str]] = []

    def model(_messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        observed.append(sorted(tool.name for tool in info.output_tools))
        return ModelResponse(
            parts=[
                ToolCallPart(
                    "ask_user",
                    {
                        "questions": [
                            {
                                "text": "Which metric matters most?",
                                "responseType": "single_choice",
                                "required": True,
                                "options": ["Revenue", "Retention"],
                            }
                        ]
                    },
                )
            ]
        )

    agent = Agent(
        FunctionModel(model),
        output_type=action_output_types(),
        prepare_output_tools=prepare_action_output_tools(action_capabilities([])),
    )

    result = agent.run_sync("Ask a clarifying question")

    assert observed == [["ask_user", "visualize"]]
    assert isinstance(result.output, AskUserAction)
    assert result.output.questions[0].responseType == "single_choice"


def test_output_preparer_derives_loaded_skill_from_real_load_skill_argument() -> None:
    capabilities = action_capabilities([])
    capabilities["report"] = SkillCapabilities(actions=frozenset({"write_report"}))
    definitions = [
        SimpleNamespace(name="visualize"),
        SimpleNamespace(name="ask_user"),
        SimpleNamespace(name="write_report"),
    ]
    prepare = prepare_action_output_tools(capabilities)

    before_load = asyncio.run(
        prepare(SimpleNamespace(messages=[]), definitions)
    )
    after_load = asyncio.run(
        prepare(
            SimpleNamespace(
                messages=[
                    ModelResponse(
                        parts=[ToolCallPart("load_skill", {"skill_name": "report"}, tool_call_id="report-call")]
                    ),
                    ModelRequest(
                        parts=[ToolReturnPart("load_skill", "<skill>\n<name>report</name>\n</skill>", tool_call_id="report-call")]
                    ),
                ]
            ),
            definitions,
        )
    )

    assert [definition.name for definition in before_load] == ["visualize", "ask_user"]
    assert [definition.name for definition in after_load] == [
        "visualize",
        "ask_user",
        "write_report",
    ]


def test_load_skill_unlocks_actions_only_after_a_confirmed_skill_return() -> None:
    call = ToolCallPart(
        "load_skill", {"skill_name": "report"}, tool_call_id="report-call"
    )
    failed = ModelRequest(
        parts=[
            ToolReturnPart(
                "load_skill",
                "Error: Skill 'report' not found.",
                tool_call_id="report-call",
            )
        ]
    )
    succeeded = ModelRequest(
        parts=[
            ToolReturnPart(
                "load_skill",
                "<skill>\n<name>report</name>\n</skill>",
                tool_call_id="report-call",
            )
        ]
    )
    request = ModelResponse(parts=[call])

    assert derive_loaded_skills([request, failed]) == {"core"}
    assert derive_loaded_skills([request, succeeded]) == {"core", "report"}


def test_regular_write_report_tool_is_gated_by_confirmed_skill_load() -> None:
    capabilities = action_capabilities([])
    capabilities["report"] = SkillCapabilities(actions=frozenset({"write_report"}))
    definitions = [
        SimpleNamespace(name="execute_sql"),
        SimpleNamespace(name="write_report"),
    ]
    prepare = prepare_regular_action_tools(capabilities, {"write_report"})
    call = ModelResponse(
        parts=[ToolCallPart("load_skill", {"skill_name": "report"}, tool_call_id="report-call")]
    )
    failed = ModelRequest(
        parts=[ToolReturnPart("load_skill", "Error: unavailable", tool_call_id="report-call")]
    )
    succeeded = ModelRequest(
        parts=[ToolReturnPart("load_skill", "<skill>\n<name>report</name>\n</skill>", tool_call_id="report-call")]
    )

    before = asyncio.run(prepare(SimpleNamespace(messages=[]), definitions))
    after_failed = asyncio.run(prepare(SimpleNamespace(messages=[call, failed]), definitions))
    after_success = asyncio.run(prepare(SimpleNamespace(messages=[call, succeeded]), definitions))

    assert [definition.name for definition in before] == ["execute_sql"]
    assert [definition.name for definition in after_failed] == ["execute_sql"]
    assert [definition.name for definition in after_success] == ["execute_sql", "write_report"]


def test_write_report_returns_the_full_markdown_for_the_bridge() -> None:
    report = "# Q3\n\nRevenue rose."

    assert WriteReport(report=report).report == report
    assert deliver_report(report) == report


def test_write_report_is_regular_and_unlocked_after_confirmed_load() -> None:
    observed: list[list[str]] = []

    def model(_messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        observed.append(sorted(tool.name for tool in info.function_tools))
        step = len(observed)
        if step == 1:
            return ModelResponse(
                parts=[ToolCallPart("load_skill", {"skill_name": "report"})]
            )
        if step == 2:
            return ModelResponse(
                parts=[ToolCallPart("write_report", {"report": "# Q3"})]
            )
        if step == 3:
            return ModelResponse(parts=[TextPart("Report delivered.")])
        raise AssertionError("write_report should not terminate the run")

    capabilities = action_capabilities([])
    capabilities["report"] = SkillCapabilities(actions=frozenset({"write_report"}))
    agent = Agent(
        FunctionModel(model),
        prepare_tools=prepare_regular_action_tools(capabilities, {"write_report"}),
    )

    @agent.tool_plain
    async def load_skill(skill_name: str) -> str:
        return f"<skill>\n<name>{skill_name}</name>\n</skill>"

    @agent.tool_plain
    async def write_report(report: str) -> str:
        return deliver_report(report)

    result = agent.run_sync("Write the report")

    assert "write_report" not in observed[0]
    assert "write_report" in observed[1]
    assert result.output == "Report delivered."


def test_visualize_schema_transcribes_df_required_and_optional_fields() -> None:
    action = VisualizeAction(
        title="Monthly revenue",
        code="result = conn.sql('select 1').df()",
        output_variable="result",
        chart={"chart_type": "LineChart"},
    )

    assert action.subtitle == ""
    assert action.input_tables == []
    assert action.field_metadata == {}
    assert action.field_display_names == {}
