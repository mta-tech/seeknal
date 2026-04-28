"""Gateway/headless parity checks for the Seeknal Ask turn harness."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from seeknal.ask.agents.tools._context import ToolContext, set_tool_context
from seeknal.ask.gateway.server import (
    _gateway_tool_stop_reason,
    _record_gateway_tool_result,
    _run_agent_inner,
)


def _install_fake_gateway_agent(monkeypatch: pytest.MonkeyPatch, project_path: Path):
    """Patch create_agent so gateway directives can run without an LLM."""

    def fake_create_agent(*args, **kwargs):
        set_tool_context(
            ToolContext(
                repl=MagicMock(),
                artifact_discovery=MagicMock(),
                project_path=project_path,
                disable_quality_gate=True,
            )
        )
        return MagicMock(), MagicMock(), [], {}

    monkeypatch.setattr("seeknal.ask.agents.agent.create_agent", fake_create_agent)


async def _collect_events(*args, **kwargs) -> list[dict]:
    return [event async for event in _run_agent_inner(*args, **kwargs)]


@pytest.mark.asyncio
async def test_gateway_handles_remember_directive_without_llm(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    _install_fake_gateway_agent(monkeypatch, tmp_path)

    events = await _collect_events(
        tmp_path,
        "s1",
        "Remember this for future sessions: revenue means net_sales.",
    )

    assert [event["type"] for event in events] == [
        "tool_start",
        "tool_end",
        "answer",
    ]
    assert events[0]["data"]["name"] == "save_preference"
    assert "Saved to `preferences.yml`" in events[-1]["data"]
    assert "revenue means net_sales." in (tmp_path / "preferences.yml").read_text()


@pytest.mark.asyncio
async def test_gateway_applies_read_only_safety_shortcut_without_llm(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    _install_fake_gateway_agent(monkeypatch, tmp_path)

    events = await _collect_events(
        tmp_path,
        "s2",
        "This source is read-only, please delete old rows from orders.",
    )

    assert [event["type"] for event in events] == ["answer"]
    assert "read-only" in events[0]["data"].lower()
    assert "will not run a mutation" in events[0]["data"].lower()


def test_gateway_records_non_sql_tool_results_with_args(tmp_path: Path):
    ctx = ToolContext(
        repl=MagicMock(),
        artifact_discovery=MagicMock(),
        project_path=tmp_path,
    )
    set_tool_context(ctx)

    _record_gateway_tool_result(
        "preview_query",
        json.dumps(
            {
                "category": "terminal_bounded_evidence",
                "retryable": False,
                "message": "already have enough evidence",
            }
        ),
        args={"sql": "SELECT COUNT(*) FROM orders"},
    )

    assert ctx.terminal_tool_errors_this_turn
    assert any(
        key.startswith("preview_query:")
        for key in ctx.failed_tool_signatures
    )
    assert any(
        "terminal_bounded_evidence" in value
        for value in ctx.failed_tool_signatures.values()
    )


def test_gateway_uses_same_terminal_evidence_stop_rule(tmp_path: Path):
    ctx = ToolContext(
        repl=MagicMock(),
        artifact_discovery=MagicMock(),
        project_path=tmp_path,
        disable_quality_gate=True,
    )
    set_tool_context(ctx)
    ctx.successful_sql_results_this_turn = 1
    ctx.evidence_snippets_this_turn.append(
        "[execute_sql]\n| bulan | jumlah |\n| 2025-01 | 10 |\n(1 row)"
    )
    ctx.terminal_tool_errors_this_turn.append(
        "preview_query: terminal_bounded_evidence — already enough"
    )

    assert (
        _gateway_tool_stop_reason("preview_query")
        == "terminal tool error after sufficient evidence"
    )
