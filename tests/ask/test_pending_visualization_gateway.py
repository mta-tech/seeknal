"""Tests for the pending_visualization ToolContext field + gateway contract (M9).

The gateway streaming loop emits ``visualization`` when
``ctx.pending_visualization`` is set and CONTINUES the turn (no ``return``),
same lifecycle as ``pending_upload`` and unlike ``pending_clarification`` which
ends the turn. Two properties matter and are testable without a live LLM:

  - anti-leak: ``reset_turn_governor`` clears the slot between turns, so a
    chart built in turn N never re-emits in turn N+1
  - no-return: the answer the chart belongs to is still streamed afterwards

A full streaming integration test is out of scope here because it would require
standing up the complete gateway request path; the control-flow replica below
mirrors the real block in ``gateway/server.py``.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import duckdb
import pytest

from seeknal.ask.agents.tools._context import (
    ToolContext,
    reset_turn_governor,
    set_tool_context,
)

pytestmark = pytest.mark.asyncio(loop_scope="function")


def _chart_payload(title: str = "Sales per month") -> list[dict]:
    return [
        {
            "chart_json": {
                "widgetId": "sess-abc-9f21a3",
                "widgetType": "line_chart",
                "widgetTitle": title,
                "widgetData": [{"bulan": "2026-01-01", "total": 10}],
                "widgetSize": 1,
            },
            "chart_type": "line_chart",
        }
    ]


class _REPLStub:
    def __init__(self) -> None:
        self.conn = duckdb.connect(":memory:")

    def execute_oneshot(self, sql, limit=None):
        return [], []


def _ctx(tmp_path: Path) -> ToolContext:
    ctx = ToolContext(
        repl=_REPLStub(),
        artifact_discovery=MagicMock(),
        project_path=tmp_path,
        sql_timeout_seconds=0,
    )
    set_tool_context(ctx)
    return ctx


def test_pending_visualization_field_defaults_none(tmp_path: Path):
    ctx = _ctx(tmp_path)
    assert ctx.pending_visualization is None


def test_reset_turn_governor_clears_pending_visualization(tmp_path: Path):
    ctx = _ctx(tmp_path)
    ctx.pending_visualization = _chart_payload()

    reset_turn_governor(question="next question")  # turn N+1 begins

    assert ctx.pending_visualization is None  # anti-leak: stale chart cleared


def test_reset_clears_every_pending_side_channel(tmp_path: Path):
    ctx = _ctx(tmp_path)
    ctx.pending_clarification = [{"question": "q?"}]
    ctx.pending_upload = {"download_url": "http://x/f.csv"}
    ctx.pending_visualization = _chart_payload()

    reset_turn_governor(question="next")

    assert ctx.pending_clarification is None
    assert ctx.pending_upload is None
    assert ctx.pending_visualization is None


# ---------------------------------------------------------------------------
# No-return control-flow contract (M9 gateway block).
#
# Faithful replica of the pending_* block in ``gateway/server.py``:
# clarification ends the turn; upload and visualization emit and fall through.
# ---------------------------------------------------------------------------


async def _gateway_loop_replica(ctx, nodes):
    """Replica of the server.py pending_* block inside the run loop."""
    for node in nodes:
        # A tool executed during this node may have set pending state.
        node_hook = node.get("on_enter")
        if node_hook:
            node_hook(ctx)

        if ctx.pending_clarification:
            _prompts = ctx.pending_clarification
            ctx.pending_clarification = None
            yield {"type": "ask_user", "data": {"prompts": _prompts}}
            return  # clarification ENDS the turn

        if ctx.pending_upload:
            _upload = ctx.pending_upload
            ctx.pending_upload = None
            yield {"type": "upload_complete", "data": _upload}
            # deliberately NO return — continue the turn

        if ctx.pending_visualization:
            _visualization = ctx.pending_visualization
            ctx.pending_visualization = None
            yield {"type": "visualization", "data": _visualization}
            # deliberately NO return — continue the turn

        if node.get("answer"):
            yield {"type": "message", "data": {"text": node["answer"]}}

        if node.get("final"):
            yield {"type": "done", "data": {}}


@pytest.mark.asyncio
async def test_visualization_does_not_end_turn(tmp_path: Path):
    """The chart is yielded AND the answer still follows (no-return)."""
    ctx = _ctx(tmp_path)

    def set_chart(ctx):
        ctx.pending_visualization = _chart_payload()

    nodes = [
        {"on_enter": set_chart},                 # visualize_chart runs here
        {"answer": "Sales grew every month."},   # agent presents its answer
        {"final": True},
    ]

    events = [ev async for ev in _gateway_loop_replica(ctx, nodes)]

    types = [ev["type"] for ev in events]
    assert types == ["visualization", "message", "done"], types
    # The answer survived — proving visualization did NOT end the turn.
    assert events[1]["data"]["text"] == "Sales grew every month."
    # Slot cleared after emit (anti-double-emit).
    assert ctx.pending_visualization is None


@pytest.mark.asyncio
async def test_visualization_event_carries_the_array_payload(tmp_path: Path):
    """The event data is the array the frontend parses, not a flat object."""
    ctx = _ctx(tmp_path)

    def set_chart(ctx):
        ctx.pending_visualization = _chart_payload()

    events = [
        ev
        async for ev in _gateway_loop_replica(ctx, [{"on_enter": set_chart}])
    ]

    data = events[0]["data"]
    assert isinstance(data, list)
    assert data[0]["chart_json"]["widgetType"] == "line_chart"
    assert data[0]["chart_type"] == "line_chart"


@pytest.mark.asyncio
async def test_upload_and_chart_can_both_emit_in_one_turn(tmp_path: Path):
    """A CSV export and a chart are independent side channels, not rivals."""
    ctx = _ctx(tmp_path)

    def set_both(ctx):
        ctx.pending_upload = {"download_url": "http://x/f.csv", "file_name": "f.csv"}
        ctx.pending_visualization = _chart_payload()

    nodes = [{"on_enter": set_both}, {"answer": "Done."}, {"final": True}]

    events = [ev async for ev in _gateway_loop_replica(ctx, nodes)]

    types = [ev["type"] for ev in events]
    assert types == ["upload_complete", "visualization", "message", "done"], types
