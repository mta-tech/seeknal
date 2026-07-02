"""Tests for the POST_TOOL_USE CSV upload reminder hook (FC2d)."""

from __future__ import annotations

import asyncio

import pytest

from seeknal.ask.agents.hooks import (
    _count_result_rows,
    _csv_upload_reminder_handler,
    get_ask_hooks,
)


def _make_result(rows: int) -> str:
    header = "| id | value |\n|---|---|\n"
    body = "".join(f"| {i} | {i * 10} |\n" for i in range(rows))
    return header + body


class _HI:
    def __init__(self, tool_name: str, tool_result: str):
        self.tool_name = tool_name
        self.tool_result = tool_result


def test_count_rows_parses_markdown_table():
    assert _count_result_rows(_make_result(25)) == 25
    assert _count_result_rows(_make_result(5)) == 5
    assert _count_result_rows("") == 0


def test_reminder_appended_when_above_threshold():
    result = asyncio.run(_csv_upload_reminder_handler(_HI("execute_sql", _make_result(30))))
    assert result.modified_result is not None
    assert "upload_to_s3" in result.modified_result
    assert "30 rows" in result.modified_result


def test_no_reminder_below_threshold():
    result = asyncio.run(_csv_upload_reminder_handler(_HI("execute_sql", _make_result(10))))
    assert result.modified_result is None


def test_no_reminder_for_non_execute_sql():
    result = asyncio.run(_csv_upload_reminder_handler(_HI("preview_query", _make_result(100))))
    assert result.modified_result is None


def test_hook_registered_in_get_ask_hooks():
    from seeknal.ask.agents.hooks import _csv_upload_reminder_handler

    hooks = get_ask_hooks({"enabled": True})
    handlers = [h.handler for h in hooks]
    assert _csv_upload_reminder_handler in handlers


def test_hook_gated_by_config():
    from seeknal.ask.agents.hooks import _csv_upload_reminder_handler

    hooks = get_ask_hooks({"enabled": True, "csv_upload_reminder": False})
    handlers = [h.handler for h in hooks]
    assert _csv_upload_reminder_handler not in handlers
