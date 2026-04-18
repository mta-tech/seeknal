"""Tests for execute_uv_script tool."""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

from seeknal.ask.agents.tools.execute_uv_script import execute_uv_script


def test_execute_uv_script_requires_uv(monkeypatch):
    monkeypatch.setattr("seeknal.ask.agents.tools.execute_uv_script.shutil.which", lambda cmd: None)

    result = asyncio.run(execute_uv_script("print('hello')"))
    assert "uv is required" in result


def test_execute_uv_script_formats_structured_result(monkeypatch, tmp_path):
    monkeypatch.setattr("seeknal.ask.agents.tools.execute_uv_script.shutil.which", lambda cmd: "/usr/bin/uv")
    monkeypatch.setattr(
        "seeknal.ask.agents.tools.execute_uv_script.get_tool_context",
        lambda: SimpleNamespace(project_path=tmp_path, session_id="abc123"),
    )

    class Result:
        returncode = 0
        stdout = json.dumps({"stdout": "hello\n", "value": {"ok": True}, "error": None, "plots": []})
        stderr = ""

    monkeypatch.setattr(
        "seeknal.ask.agents.tools.execute_uv_script.subprocess.run",
        lambda *args, **kwargs: Result(),
    )

    result = asyncio.run(execute_uv_script("print('hello')", deps=["httpx", "httpx"]))
    assert "hello" in result
    assert "{'ok': True}" in result or '"ok": true' in result.lower()

