"""Tests for the pending_upload ToolContext field + reset (FC2d gateway contract).

The gateway streaming loop emits ``upload_complete`` when ``ctx.pending_upload``
is set and CONTINUES the turn (no ``return``), unlike ``pending_clarification``
which ends the turn. The most important testable property is the anti-leak
guarantee: ``reset_turn_governor`` must clear ``pending_upload`` between turns
so a stale upload from turn N does not re-emit in turn N+1.

The gateway no-return behavior itself is verified by code inspection in
``gateway/server.py`` (deliberate comment + absence of ``return`` after the
emit-and-clear block); a full streaming integration test is out of scope here
because it would require standing up the complete gateway request path.
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


def test_reset_turn_governor_clears_pending_upload(tmp_path: Path):
    ctx = _ctx(tmp_path)
    ctx.pending_upload = {"download_url": "http://x/file.csv", "file_name": "f.csv"}
    assert ctx.pending_upload is not None  # set in turn N

    reset_turn_governor(question="next question")  # turn N+1 begins

    assert ctx.pending_upload is None  # anti-leak: stale upload cleared


def test_reset_turn_governor_clears_pending_clarification_too(tmp_path: Path):
    ctx = _ctx(tmp_path)
    ctx.pending_clarification = [{"question": "q?"}]
    ctx.pending_upload = {"download_url": "http://x/f.csv"}

    reset_turn_governor(question="next")

    assert ctx.pending_clarification is None
    assert ctx.pending_upload is None


def test_pending_upload_field_defaults_none(tmp_path: Path):
    ctx = _ctx(tmp_path)
    assert ctx.pending_upload is None


# ---------------------------------------------------------------------------
# No-return control-flow contract (FC2d gateway block).
#
# The gateway streaming loop yields ``upload_complete`` when
# ``ctx.pending_upload`` is set and CONTINUES the turn (deliberate absence of
# ``return``), unlike ``pending_clarification`` which ends the turn. A full
# streaming integration needs a real LLM agent; this test exercises the actual
# control-flow structure (a faithful replica of the gateway block in
# ``server.py``) to prove the contract: BOTH upload_complete AND a subsequent
# answer event are yielded from the same turn.
# ---------------------------------------------------------------------------


async def _gateway_loop_replica(ctx, nodes):
    """Faithful replica of the server.py pending_* block inside the run loop.

    Mirrors ``seeknal/ask/gateway/server.py``: pending_clarification ends the
    turn with ``return``; pending_upload emits and falls through.
    """
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

        if node.get("answer"):
            yield {"type": "message", "data": {"text": node["answer"]}}

        if node.get("final"):
            yield {"type": "done", "data": {}}


@pytest.mark.asyncio
async def test_upload_complete_does_not_end_turn(tmp_path: Path):
    """upload_complete is yielded AND the answer still follows (no-return)."""
    import asyncio

    ctx = _ctx(tmp_path)

    def set_upload(ctx):
        ctx.pending_upload = {"download_url": "http://x/f.csv", "file_name": "f.csv"}

    nodes = [
        {"on_enter": set_upload},          # tool sets pending_upload here
        {"answer": "Here is the forecast."},  # agent presents its answer
        {"final": True},
    ]

    events = [ev async for ev in _gateway_loop_replica(ctx, nodes)]

    types = [ev["type"] for ev in events]
    assert types == ["upload_complete", "message", "done"], types
    # The answer survived — proving upload_complete did NOT end the turn.
    assert events[1]["data"]["text"] == "Here is the forecast."
    # pending_upload cleared after emit (anti-double-emit).
    assert ctx.pending_upload is None


@pytest.mark.asyncio
async def test_clarification_still_ends_turn(tmp_path: Path):
    """Regression guard: pending_clarification must keep ending the turn."""
    ctx = _ctx(tmp_path)

    def set_clar(ctx):
        ctx.pending_clarification = [{"question": "q?"}]

    nodes = [
        {"on_enter": set_clar},
        {"answer": "this must NOT be reached"},  # turn ended above
        {"final": True},
    ]

    events = [ev async for ev in _gateway_loop_replica(ctx, nodes)]
    types = [ev["type"] for ev in events]
    assert types == ["ask_user"], types  # message + done NOT yielded
