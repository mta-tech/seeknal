"""Regression tests for Ask model settings and synchronous usage limits."""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from pydantic_ai.usage import RunUsage

from seeknal.ask.agents.agent import _quality_gate, ask, create_agent


def _result(output: str):
    result = MagicMock()
    result.output = output
    result.all_messages.return_value = []
    return result


def _create_agent_with_settings(tmp_path: Path, explicit_settings):
    (tmp_path / "seeknal_agent.yml").write_text(
        "agent_harness:\n"
        "  model_settings:\n"
        "    temperature: 0.75\n"
        "    max_tokens: 2048\n"
    )

    with (
        patch("seeknal.cli.repl.REPL") as repl_cls,
        patch("seeknal.ask.security.configure_safe_connection"),
        patch("seeknal.ask.modules.artifact_discovery.service.ArtifactDiscovery"),
        patch(
            "seeknal.ask.agents.providers.get_model_string",
            return_value="test:model",
        ),
        patch("seeknal.ask.agents.context_toolset.SeeknaContextToolset"),
        patch("seeknal.ask.agents.tools.toolset.create_ask_toolset"),
        patch("pydantic_deep.create_deep_agent") as create_deep_agent,
        patch("pydantic_deep.DeepAgentDeps") as deps_cls,
    ):
        repl_cls.return_value = MagicMock(conn=MagicMock())
        create_deep_agent.return_value = MagicMock()
        deps_cls.return_value = MagicMock()

        create_agent(project_path=tmp_path, model_settings=explicit_settings)
        return create_deep_agent.call_args.kwargs["model_settings"]


@pytest.mark.parametrize("explicit_settings", [{}, {"temperature": 0.0}])
def test_explicit_model_settings_override_project_config(tmp_path, explicit_settings):
    assert _create_agent_with_settings(tmp_path, explicit_settings) == explicit_settings


def test_project_model_settings_are_used_when_argument_is_none(tmp_path):
    assert _create_agent_with_settings(tmp_path, None) == {
        "temperature": 0.75,
        "max_tokens": 2048,
    }


def _create_agent_capture_context(
    tmp_path: Path,
    yaml_text: str,
    *,
    analysis_mode: bool,
):
    """Create an agent from a real seeknal_agent.yml and return its ToolContext."""
    (tmp_path / "seeknal_agent.yml").write_text(yaml_text)

    with (
        patch("seeknal.cli.repl.REPL") as repl_cls,
        patch("seeknal.ask.security.configure_safe_connection"),
        patch("seeknal.ask.modules.artifact_discovery.service.ArtifactDiscovery"),
        patch(
            "seeknal.ask.agents.providers.get_model_string",
            return_value="test:model",
        ),
        patch("seeknal.ask.agents.context_toolset.SeeknaContextToolset"),
        patch("seeknal.ask.agents.tools.toolset.create_ask_toolset"),
        patch("pydantic_deep.create_deep_agent") as create_deep_agent,
        patch("pydantic_deep.DeepAgentDeps") as deps_cls,
        patch(
            "seeknal.ask.config.get_ask_toolset_mode",
            return_value="analysis" if analysis_mode else "full",
        ),
    ):
        repl_cls.return_value = MagicMock(conn=MagicMock())
        create_deep_agent.return_value = MagicMock()
        deps_cls.return_value = MagicMock()
        create_agent(project_path=tmp_path)

    from seeknal.ask.agents.tools._context import get_tool_context

    return get_tool_context()


def test_yaml_tool_call_limit_reaches_tool_context(tmp_path):
    """A small configured limit (5) must flow from YAML into the tool context."""
    ctx = _create_agent_capture_context(
        tmp_path,
        "tool_call_limit: 5\n",
        analysis_mode=True,
    )
    assert ctx.tool_call_limit == 5


def test_tool_call_limit_is_ignored_in_full_mode_with_warning(tmp_path, caplog):
    """Full mode has no tool-call ceiling; a configured value must not be
    silently dropped without a trace."""
    with caplog.at_level("WARNING"):
        ctx = _create_agent_capture_context(
            tmp_path,
            "tool_call_limit: 5\n",
            analysis_mode=False,
        )
    assert ctx.tool_call_limit is None
    assert "full toolset mode" in caplog.text


def test_full_mode_tool_call_limit_warns_once_per_project(tmp_path, caplog):
    """Repeated agent creation warns once, while another project still warns."""
    with caplog.at_level("WARNING"):
        for _ in range(2):
            _create_agent_capture_context(
                tmp_path, "tool_call_limit: 5\n", analysis_mode=False,
            )
        other_project = tmp_path / "other"
        other_project.mkdir()
        _create_agent_capture_context(
            other_project, "tool_call_limit: 5\n", analysis_mode=False,
        )

    warnings = [r for r in caplog.records if "full toolset mode" in r.message]
    assert len(warnings) == 2


def test_sync_ask_degrades_to_evidence_when_budget_5_is_spent():
    """Limit=5 spent → evidence answer, not UsageLimitExceeded (GW1 gap fix)."""
    from pydantic_ai.exceptions import UsageLimitExceeded

    from seeknal.ask.agents.tools._context import ToolContext

    agent = MagicMock()
    observed_limits = []

    def run_sync(*_args, **kwargs):
        observed_limits.append(kwargs["usage_limits"])
        if len(observed_limits) == 1:
            raise UsageLimitExceeded("tool-call budget reached after 5 tool calls")
        return _result("Answer synthesized from gathered evidence.")

    agent.run_sync.side_effect = run_sync
    tool_ctx = ToolContext(
        repl=MagicMock(),
        artifact_discovery=MagicMock(),
        project_path=Path("/tmp/does-not-matter"),
        request_limit=33,
        tool_call_limit=5,
        disable_quality_gate=True,
        current_question="count rows",
        evidence_snippets_this_turn=["tool result: 12 rows"],
    )

    with patch(
        "seeknal.ask.agents.tools._context.get_tool_context",
        return_value=tool_ctx,
    ):
        assert ask(agent, MagicMock(), [], "count rows") == (
            "Answer synthesized from gathered evidence."
        )

    assert observed_limits[0].tool_calls_limit == 5
    assert observed_limits[1].tool_calls_limit == 0, (
        "evidence-synthesis pass must run without tools"
    )


def test_sync_ask_returns_deterministic_text_if_synthesis_also_hits_limit():
    from pydantic_ai.exceptions import UsageLimitExceeded

    from seeknal.ask.agents.tools._context import ToolContext

    agent = MagicMock()
    agent.run_sync.side_effect = UsageLimitExceeded("tool-call budget reached")
    tool_ctx = ToolContext(
        repl=MagicMock(),
        artifact_discovery=MagicMock(),
        project_path=Path("/tmp/does-not-matter"),
        request_limit=33,
        tool_call_limit=5,
        disable_quality_gate=True,
        current_question="count rows",
    )

    with patch(
        "seeknal.ask.agents.tools._context.get_tool_context",
        return_value=tool_ctx,
    ):
        answer = ask(agent, MagicMock(), [], "count rows")
    assert "stopping further tool use" in answer.lower()
    assert "tool-call budget" in answer


@pytest.mark.asyncio
async def test_quiet_stream_uses_configured_tool_call_limit():
    """Quiet/non-TTY streaming used to pass only request_limit, silently
    ignoring the configured tool-call ceiling (GW1 gap fix)."""
    from io import StringIO

    from rich.console import Console

    from seeknal.ask.agents.tools._context import ToolContext
    from seeknal.ask.streaming import stream_ask

    agent = MagicMock()
    captured = {}

    async def _run(*_args, **kwargs):
        captured["usage_limits"] = kwargs["usage_limits"]
        return _result("ok")

    agent.run = _run
    tool_ctx = ToolContext(
        repl=MagicMock(),
        artifact_discovery=MagicMock(),
        project_path=Path("/tmp/does-not-matter"),
        request_limit=33,
        tool_call_limit=5,
        disable_quality_gate=True,
    )

    with (
        patch("seeknal.ask.streaming.lookup_prior_turn_answer", return_value=None),
        patch("seeknal.ask.streaming.reset_report_approval"),
        patch("seeknal.ask.streaming.reset_turn_governor"),
        patch("seeknal.ask.streaming.record_prior_turn_answer"),
        patch("seeknal.ask.streaming.compact_history_for_analysis_mode"),
        patch(
            "seeknal.ask.agents.tools._context.get_tool_context",
            return_value=tool_ctx,
        ),
    ):
        answer = await stream_ask(
            agent,
            MagicMock(),
            [],
            "count rows",
            Console(file=StringIO()),
            quiet=True,
        )

    assert answer == "ok"
    assert captured["usage_limits"].request_limit == 33
    assert captured["usage_limits"].tool_calls_limit == 5


@pytest.mark.asyncio
@pytest.mark.parametrize("synthesis", ["answer", "limit", "empty", "no-evidence"])
async def test_quiet_stream_degrades_when_tool_budget_is_spent(synthesis):
    """Spent budgets yield evidence, with no tools allowed during synthesis."""
    from io import StringIO

    from rich.console import Console

    from seeknal.ask.agents.tools._context import ToolContext
    from seeknal.ask.streaming import stream_ask

    agent = MagicMock()
    observed_limits = []
    history = ["existing history"]
    new_history = ["evidence answer history"]

    async def run(prompt, **kwargs):
        limits = kwargs["usage_limits"]
        observed_limits.append(limits)
        if len(observed_limits) == 1:
            limits.check_before_tool_call(RunUsage(tool_calls=6))
        assert "12 rows" in prompt
        assert "Do not call any tools" in prompt
        assert kwargs["message_history"] is history
        if synthesis == "limit":
            limits.check_before_tool_call(RunUsage(tool_calls=1))
        result = _result("Found 12 rows." if synthesis == "answer" else "")
        result.all_messages.return_value = new_history
        return result

    agent.run = run
    ctx = ToolContext(
        repl=MagicMock(), artifact_discovery=MagicMock(), project_path=Path("/tmp"),
        request_limit=33, tool_call_limit=5, current_question="count rows",
        evidence_snippets_this_turn=(
            [] if synthesis == "no-evidence" else ["tool result: 12 rows"]
        ),
    )
    console = Console(file=StringIO(), force_terminal=False)
    with (
        patch("seeknal.ask.streaming.lookup_prior_turn_answer", return_value=None),
        patch("seeknal.ask.streaming.reset_report_approval"),
        patch("seeknal.ask.streaming.reset_turn_governor"),
        patch("seeknal.ask.streaming.record_prior_turn_answer") as record,
        patch("seeknal.ask.streaming.compact_history_for_analysis_mode"),
        patch("seeknal.ask.agents.tools._context.get_tool_context", return_value=ctx),
    ):
        answer = await stream_ask(
            agent, MagicMock(), history, "count rows", console, quiet=True,
        )

    assert observed_limits[0].tool_calls_limit == 5
    if synthesis == "no-evidence":
        assert len(observed_limits) == 1
    else:
        assert len(observed_limits) == 2
        assert observed_limits[1].request_limit == 33
        assert observed_limits[1].tool_calls_limit == 0
        assert "12 rows" in answer
    if synthesis == "answer":
        assert answer == "Found 12 rows."
    else:
        assert "stopping further tool use" in answer
        assert "tool-call budget" in answer
    if synthesis in ("answer", "empty"):
        assert history == new_history
    else:
        assert history == ["existing history"]
    record.assert_called_once_with(question="count rows", answer=answer)
    agent.run_sync.assert_not_called()


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        (
            "authoritative sql pair result",
            "authoritative SQL pair produced the answer for this business question",
        ),
        (
            "terminal tool error after sufficient evidence",
            "terminal tool error after sufficient evidence",
        ),
        (
            "tool-call budget reached",
            "tool-call budget reached before the model produced a final answer",
        ),
    ],
)
def test_usage_limit_stop_reason_preserves_existing_strings(message, expected):
    from pydantic_ai.exceptions import UsageLimitExceeded

    from seeknal.ask.agents.tools._context import usage_limit_stop_reason

    assert usage_limit_stop_reason(UsageLimitExceeded(message)) == expected


def test_sync_ask_applies_tool_call_limit_to_initial_and_ralph_retry():
    agent = MagicMock()
    observed_usage = []

    def run_sync(*_args, **kwargs):
        usage = kwargs["usage"]
        observed_usage.append((usage, usage.requests, usage.tool_calls))
        usage.incr(RunUsage(requests=1, tool_calls=4))
        return _result("" if len(observed_usage) == 1 else "Found 12 rows.")

    agent.run_sync.side_effect = run_sync
    ctx = SimpleNamespace(
        request_limit=33,
        tool_call_limit=7,
        disable_quality_gate=True,
    )

    with patch(
        "seeknal.ask.agents.tools._context.get_tool_context",
        return_value=ctx,
    ):
        assert ask(agent, MagicMock(), [], "count rows") == "Found 12 rows."

    assert agent.run_sync.call_count == 2
    for call in agent.run_sync.call_args_list:
        limits = call.kwargs["usage_limits"]
        assert limits.request_limit == 33
        assert limits.tool_calls_limit == 7
    assert observed_usage[0][1:] == (0, 0)
    assert observed_usage[1][1:] == (1, 4)
    assert observed_usage[0][0] is observed_usage[1][0]


def test_sync_quality_retry_keeps_tool_call_limit():
    agent = MagicMock()
    observed_usage = []

    def run_sync(*_args, **kwargs):
        usage = kwargs["usage"]
        observed_usage.append((usage, usage.requests, usage.tool_calls))
        usage.incr(RunUsage(requests=1, tool_calls=3))
        if len(observed_usage) == 1:
            return _result(
                "The data contains some interesting patterns worth exploring."
            )
        return _result("There are 12 rows across 3 categories.")

    agent.run_sync.side_effect = run_sync
    ctx = SimpleNamespace(
        request_limit=41,
        tool_call_limit=9,
        disable_quality_gate=False,
    )

    with patch(
        "seeknal.ask.agents.tools._context.get_tool_context",
        return_value=ctx,
    ):
        assert ask(agent, MagicMock(), [], "summarize") == (
            "There are 12 rows across 3 categories."
        )

    assert agent.run_sync.call_count == 2
    for call in agent.run_sync.call_args_list:
        limits = call.kwargs["usage_limits"]
        assert limits.request_limit == 41
        assert limits.tool_calls_limit == 9
    assert observed_usage[0][1:] == (0, 0)
    assert observed_usage[1][1:] == (1, 3)
    assert observed_usage[0][0] is observed_usage[1][0]


def test_quality_gate_direct_caller_does_not_require_usage():
    agent = MagicMock()
    agent.run_sync.return_value = _result("There are 12 verified rows.")

    answer = _quality_gate(
        agent,
        MagicMock(),
        [],
        "The data contains some interesting patterns worth exploring.",
    )

    assert answer == "There are 12 verified rows."
    assert agent.run_sync.call_args.kwargs["usage"] is None


@pytest.mark.parametrize("quality_retry", [False, True])
def test_retry_cannot_exceed_cumulative_tool_budget(quality_retry):
    """The budget guard still raises inside the loop; `ask()` then degrades."""
    from pydantic_ai.exceptions import UsageLimitExceeded

    from seeknal.ask.agents.agent import _ask_with_budget

    agent = MagicMock()
    calls = 0

    def run_sync(*_args, **kwargs):
        nonlocal calls
        calls += 1
        usage = kwargs["usage"]
        # Use the real Pydantic AI pre-call guard with four proposed tool calls.
        projected = RunUsage(tool_calls=usage.tool_calls + 4)
        kwargs["usage_limits"].check_before_tool_call(projected)
        usage.incr(RunUsage(requests=1, tool_calls=4))
        if quality_retry:
            return _result("The data contains some interesting patterns worth exploring.")
        return _result("")

    agent.run_sync.side_effect = run_sync
    ctx = SimpleNamespace(
        request_limit=33, tool_call_limit=7, disable_quality_gate=not quality_retry,
    )
    with patch("seeknal.ask.agents.tools._context.get_tool_context", return_value=ctx):
        with pytest.raises(UsageLimitExceeded):
            _ask_with_budget(agent, MagicMock(), [], "summarize")
    assert calls == 2
