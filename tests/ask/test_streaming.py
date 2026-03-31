"""Tests for seeknal ask streaming module.

Tests rendering helpers (no LLM needed) and basic function contracts.
Streaming integration tests require pydantic-ai's agent.iter() which
needs a real or mocked Agent — those are covered in QA/E2E tests.
"""

import re
from io import StringIO

import pytest

from seeknal.ask.streaming import (
    _sanitize_output,
    _show_reasoning,
    _show_tool_start,
    _show_tool_end,
    _show_answer,
    _show_retry,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_console():
    """Create a Rich Console that captures output to a string."""
    from rich.console import Console

    buf = StringIO()
    return Console(file=buf, force_terminal=False, width=120), buf


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

    def test_show_tool_start_execute_python(self):
        console, buf = make_console()
        _show_tool_start(console, "execute_python", {"code": "print(1)"})
        output = buf.getvalue()
        assert "execute_python" in output
        assert "print" in output

    def test_show_tool_start_generate_report(self):
        console, buf = make_console()
        _show_tool_start(console, "generate_report", {"title": "My Report"})
        output = buf.getvalue()
        assert "generate_report" in output
        assert "My Report" in output

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

    def test_show_tool_end_python_output(self):
        console, buf = make_console()
        _show_tool_end(console, "execute_python", "42")
        output = buf.getvalue()
        assert "42" in output

    def test_show_tool_end_python_plot(self):
        console, buf = make_console()
        _show_tool_end(console, "execute_python", "Plots saved: /tmp/plot.png")
        output = buf.getvalue()
        assert "Plots saved" in output

    def test_show_tool_end_report_success(self):
        console, buf = make_console()
        _show_tool_end(console, "generate_report", "Report built successfully: /tmp/report.html")
        output = buf.getvalue()
        assert "Report built" in output

    def test_show_tool_end_report_error(self):
        console, buf = make_console()
        _show_tool_end(console, "generate_report", "Error: npm not found")
        output = buf.getvalue()
        assert "Report Build Error" in output
