"""run_pipeline tool: refuses nodes+full and surfaces unrecognized CLI errors."""

from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import MagicMock

from seeknal.ask.agents.tools._context import ToolContext
from seeknal.ask.agents.tools.run_pipeline import run_pipeline


def test_nodes_with_full_is_refused_before_running(monkeypatch):
    called = []
    monkeypatch.setattr(asyncio, "create_subprocess_exec", lambda *a, **k: called.append(a))

    out = asyncio.run(run_pipeline(nodes="transform.clean", full=True, confirmed=True))

    assert "cannot be combined with nodes" in out
    assert called == []


class _FakeStream:
    def __init__(self, lines):
        self._lines = [line.encode() + b"\n" for line in lines]

    def __aiter__(self):
        async def gen():
            for line in self._lines:
                yield line
        return gen()

    async def read(self):
        return b""


class _FakeProc:
    def __init__(self, stdout_lines, returncode):
        self.stdout = _FakeStream(stdout_lines)
        self.stderr = _FakeStream([])
        self.returncode = returncode

    async def wait(self):
        return self.returncode


def test_failure_without_error_keywords_shows_output_tail(tmp_path: Path, monkeypatch):
    from seeknal.ask.agents.tools import _context

    token = _context._tool_context_var.set(
        ToolContext(repl=MagicMock(), artifact_discovery=MagicMock(), project_path=tmp_path)
    )
    try:
        _run_failing_pipeline_and_check(monkeypatch)
    finally:
        _context._tool_context_var.reset(token)


def _run_failing_pipeline_and_check(monkeypatch):

    async def fake_exec(*_args, **_kwargs):
        return _FakeProc(["Seeknal Pipeline Run", "✗ --full cannot be combined with --tags"], 1)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)

    out = asyncio.run(run_pipeline(confirmed=True))

    assert "Pipeline execution FAILED" in out
    assert "(no details captured)" not in out
    assert "--full cannot be combined with --tags" in out
