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
    from pydantic_ai.exceptions import UsageLimitExceeded

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
            ask(agent, MagicMock(), [], "summarize")
    assert calls == 2
