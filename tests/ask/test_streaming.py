"""Tests for seeknal ask streaming module.

Uses synthetic events from mock async generators -- no LLM needed.
"""

import asyncio
import re
from io import StringIO
from unittest.mock import AsyncMock, MagicMock

import pytest

from seeknal.ask.streaming import (
    _normalize_content,
    _sanitize_output,
    _show_reasoning,
    _show_tool_start,
    _show_tool_end,
    _show_answer,
    _show_retry,
    _stream_one_pass,
    stream_ask,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_console():
    """Create a Rich Console that captures output to a string."""
    from rich.console import Console

    buf = StringIO()
    return Console(file=buf, force_terminal=False, width=120), buf


class MockChunk:
    """Mock LLM chunk with content attribute."""

    def __init__(self, content):
        self.content = content


async def event_stream(events):
    """Async generator that yields events from a list."""
    for event in events:
        yield event


class MockAgent:
    """Mock agent with astream_events method."""

    def __init__(self, events_per_call=None):
        # events_per_call: list of event lists (one per invocation)
        self._events_per_call = events_per_call or [[]]
        self._call_index = 0

    def astream_events(self, input_dict, config, version):
        events = (
            self._events_per_call[self._call_index]
            if self._call_index < len(self._events_per_call)
            else []
        )
        self._call_index += 1
        return event_stream(events)


# ---------------------------------------------------------------------------
# _normalize_content tests
# ---------------------------------------------------------------------------


class TestNormalizeContent:
    def test_plain_string(self):
        assert _normalize_content("hello") == "hello"

    def test_gemini_list_format(self):
        content = [{"type": "text", "text": "hello "}, {"type": "text", "text": "world"}]
        assert _normalize_content(content) == "hello world"

    def test_mixed_list(self):
        content = [{"type": "text", "text": "hello"}, "world"]
        assert _normalize_content(content) == "helloworld"

    def test_empty_content(self):
        assert _normalize_content("") == ""
        assert _normalize_content(None) == ""
        assert _normalize_content([]) == ""


# ---------------------------------------------------------------------------
# _sanitize_output tests
# ---------------------------------------------------------------------------


class TestSanitizeOutput:
    def test_strips_ansi_escape_codes(self):
        text = "hello \x1b[31mred\x1b[0m world"
        assert _sanitize_output(text) == "hello red world"

    def test_strips_screen_clear(self):
        text = "\x1b[2J\x1b[Hdangerous"
        assert _sanitize_output(text) == "dangerous"

    def test_preserves_clean_text(self):
        text = "clean text with no escapes"
        assert _sanitize_output(text) == text


# ---------------------------------------------------------------------------
# Rendering helper tests
# ---------------------------------------------------------------------------


class TestRenderingHelpers:
    def test_show_reasoning(self):
        console, buf = make_console()
        _show_reasoning(console, "I need to find the table")
        output = buf.getvalue()
        assert "Reasoning" in output
        assert "I need to find the table" in output

    def test_show_tool_start_generic(self):
        console, buf = make_console()
        _show_tool_start(console, "list_tables", {})
        output = buf.getvalue()
        assert "list_tables" in output

    def test_show_tool_start_execute_sql(self):
        console, buf = make_console()
        _show_tool_start(console, "execute_sql", {"sql": "SELECT 1"})
        output = buf.getvalue()
        assert "execute_sql" in output
        assert "SELECT" in output

    def test_show_tool_end_generic(self):
        console, buf = make_console()
        _show_tool_end(console, "list_tables", "Available tables:\n- customers\n- orders")
        output = buf.getvalue()
        assert "Done" in output

    def test_show_tool_end_truncates_long_output(self):
        console, buf = make_console()
        long_output = "x" * 500
        _show_tool_end(console, "list_tables", long_output)
        output = buf.getvalue()
        assert "..." in output

    def test_show_tool_end_sanitizes_ansi(self):
        console, buf = make_console()
        _show_tool_end(console, "list_tables", "result \x1b[31mred\x1b[0m text")
        output = buf.getvalue()
        assert "\x1b[31m" not in output

    def test_show_answer(self):
        console, buf = make_console()
        _show_answer(console, "There are 5 customers.")
        output = buf.getvalue()
        assert "Answer" in output
        assert "5 customers" in output

    def test_show_retry(self):
        console, buf = make_console()
        _show_retry(console, 1, 3)
        output = buf.getvalue()
        assert "attempt 1/3" in output

    def test_show_tool_end_sql_result_table(self):
        """execute_sql output with markdown table gets rendered as Rich Table."""
        console, buf = make_console()
        md_table = (
            "| city | count |\n"
            "| --- | --- |\n"
            "| Jakarta | 14 |\n"
            "| Bandung | 12 |\n"
            "\n(2 rows)"
        )
        _show_tool_end(console, "execute_sql", md_table)
        output = buf.getvalue()
        assert "Jakarta" in output
        assert "Bandung" in output


# ---------------------------------------------------------------------------
# Event stream integration tests
# ---------------------------------------------------------------------------


class TestStreamOnePass:
    def test_text_only_produces_answer(self):
        """LLM text with no tool calls = answer."""
        events = [
            {
                "event": "on_chat_model_stream",
                "data": {"chunk": MockChunk("Hello ")},
            },
            {
                "event": "on_chat_model_stream",
                "data": {"chunk": MockChunk("world")},
            },
        ]
        agent = MockAgent([events])
        console, buf = make_console()
        answer = asyncio.run(
            _stream_one_pass(agent, {"configurable": {"thread_id": "t"}}, "hi", console)
        )
        assert answer == "Hello world"
        output = buf.getvalue()
        assert "Answer" in output

    def test_reasoning_then_tool_then_answer(self):
        """Text -> tool_start flushes text as reasoning, final text = answer."""
        events = [
            # Reasoning
            {
                "event": "on_chat_model_stream",
                "data": {"chunk": MockChunk("Let me check")},
            },
            # Tool start flushes reasoning
            {
                "event": "on_tool_start",
                "name": "list_tables",
                "data": {"input": {}},
            },
            {
                "event": "on_tool_end",
                "name": "list_tables",
                "data": {"output": "Available tables:\n- customers"},
            },
            # Answer
            {
                "event": "on_chat_model_stream",
                "data": {"chunk": MockChunk("Found 1 table")},
            },
        ]
        agent = MockAgent([events])
        console, buf = make_console()
        answer = asyncio.run(
            _stream_one_pass(agent, {"configurable": {"thread_id": "t"}}, "q", console)
        )
        output = buf.getvalue()
        assert "Reasoning" in output
        assert "Let me check" in output
        assert "list_tables" in output
        assert "Answer" in output
        assert answer == "Found 1 table"

    def test_tool_only_no_answer(self):
        """Tool calls with no final text = empty string (triggers Ralph Loop)."""
        events = [
            {
                "event": "on_tool_start",
                "name": "list_tables",
                "data": {"input": {}},
            },
            {
                "event": "on_tool_end",
                "name": "list_tables",
                "data": {"output": "tables found"},
            },
        ]
        agent = MockAgent([events])
        console, buf = make_console()
        answer = asyncio.run(
            _stream_one_pass(agent, {"configurable": {"thread_id": "t"}}, "q", console)
        )
        assert answer == ""

    def test_gemini_cumulative_dedup(self):
        """Cumulative content (each chunk = all previous + new) gets deduped."""
        events = [
            {
                "event": "on_chat_model_stream",
                "data": {"chunk": MockChunk("Hello")},
            },
            {
                "event": "on_chat_model_stream",
                "data": {"chunk": MockChunk("Hello world")},
            },
            {
                "event": "on_chat_model_stream",
                "data": {"chunk": MockChunk("Hello world!")},
            },
        ]
        agent = MockAgent([events])
        console, buf = make_console()
        answer = asyncio.run(
            _stream_one_pass(agent, {"configurable": {"thread_id": "t"}}, "q", console)
        )
        assert answer == "Hello world!"

    def test_execute_sql_renders_sql_and_table(self):
        """execute_sql tool shows SQL syntax and result table."""
        sql = "SELECT city, COUNT(*) FROM customers GROUP BY city"
        md_result = (
            "| city | count |\n"
            "| --- | --- |\n"
            "| Jakarta | 14 |\n"
            "\n(1 row)"
        )
        events = [
            {
                "event": "on_tool_start",
                "name": "execute_sql",
                "data": {"input": {"sql": sql}},
            },
            {
                "event": "on_tool_end",
                "name": "execute_sql",
                "data": {"output": md_result},
            },
            {
                "event": "on_chat_model_stream",
                "data": {"chunk": MockChunk("Jakarta has 14 customers")},
            },
        ]
        agent = MockAgent([events])
        console, buf = make_console()
        answer = asyncio.run(
            _stream_one_pass(agent, {"configurable": {"thread_id": "t"}}, "q", console)
        )
        output = buf.getvalue()
        assert "execute_sql" in output
        assert "SELECT" in output
        assert "Jakarta" in output
        assert answer == "Jakarta has 14 customers"


class TestStreamAsk:
    def test_ralph_loop_retries_on_no_answer(self):
        """If first pass returns no text, Ralph Loop sends nudge and retries."""
        # First pass: tools only, no answer
        pass1 = [
            {"event": "on_tool_start", "name": "list_tables", "data": {"input": {}}},
            {"event": "on_tool_end", "name": "list_tables", "data": {"output": "tables"}},
        ]
        # Second pass: answer produced
        pass2 = [
            {
                "event": "on_chat_model_stream",
                "data": {"chunk": MockChunk("Summary answer")},
            },
        ]
        agent = MockAgent([pass1, pass2])
        console, buf = make_console()
        answer = asyncio.run(
            stream_ask(agent, {"configurable": {"thread_id": "t"}}, "q", console)
        )
        assert answer == "Summary answer"
        output = buf.getvalue()
        assert "attempt 1/3" in output

    def test_quiet_mode_uses_sync_ask(self):
        """Quiet mode falls back to sync ask() with spinner."""
        import unittest.mock as mock

        console, buf = make_console()
        # Patch the lazy import target
        with mock.patch(
            "seeknal.ask.agents.agent.ask", return_value="sync result"
        ) as m:
            answer = asyncio.run(
                stream_ask(
                    MockAgent([]),
                    {"configurable": {"thread_id": "t"}},
                    "q",
                    console,
                    quiet=True,
                )
            )
            assert answer == "sync result"
            m.assert_called_once()
