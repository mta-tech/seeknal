"""Tests for interactive_ask_user callback."""

import asyncio
from unittest.mock import patch

import pytest

from seeknal.ask.agents.tools.ask_user import interactive_ask_user


OPTIONS = [
    {"label": "Alpha", "description": "First option"},
    {"label": "Beta", "description": "Second option", "recommended": "true"},
]


class TestAsyncContract:
    """Verify the async contract fix — the core bug fix."""

    def test_is_coroutine_function(self):
        """interactive_ask_user must be async (pydantic-deep awaits it)."""
        assert asyncio.iscoroutinefunction(interactive_ask_user)

    def test_await_completes_without_typeerror(self):
        """Calling await on the callback does not raise TypeError."""
        with patch("seeknal.ask.agents.tools.ask_user.sys") as mock_sys:
            mock_sys.stdout.isatty.return_value = False
            with patch("builtins.input", return_value="1"):
                result = asyncio.run(interactive_ask_user("Pick?", OPTIONS))
                assert result == "Alpha"


class TestNonTTYFallback:
    """Test the numbered list fallback for non-interactive environments."""

    def test_selects_option_by_number(self):
        with patch("seeknal.ask.agents.tools.ask_user.sys") as mock_sys:
            mock_sys.stdout.isatty.return_value = False
            with patch("builtins.input", return_value="2"):
                result = asyncio.run(interactive_ask_user("Pick?", OPTIONS))
                assert result == "Beta"

    def test_invalid_input_returns_first(self):
        with patch("seeknal.ask.agents.tools.ask_user.sys") as mock_sys:
            mock_sys.stdout.isatty.return_value = False
            with patch("builtins.input", side_effect=ValueError):
                result = asyncio.run(interactive_ask_user("Pick?", OPTIONS))
                assert result == "Alpha"

    def test_eof_returns_first(self):
        with patch("seeknal.ask.agents.tools.ask_user.sys") as mock_sys:
            mock_sys.stdout.isatty.return_value = False
            with patch("builtins.input", side_effect=EOFError):
                result = asyncio.run(interactive_ask_user("Pick?", OPTIONS))
                assert result == "Alpha"

    def test_keyboard_interrupt_returns_first(self):
        with patch("seeknal.ask.agents.tools.ask_user.sys") as mock_sys:
            mock_sys.stdout.isatty.return_value = False
            with patch("builtins.input", side_effect=KeyboardInterrupt):
                result = asyncio.run(interactive_ask_user("Pick?", OPTIONS))
                assert result == "Alpha"


class TestEdgeCases:
    def test_empty_options_returns_empty(self):
        result = asyncio.run(interactive_ask_user("Pick?", []))
        assert result == ""

    def test_options_without_description(self):
        opts = [{"label": "A"}, {"label": "B"}]
        with patch("seeknal.ask.agents.tools.ask_user.sys") as mock_sys:
            mock_sys.stdout.isatty.return_value = False
            with patch("builtins.input", return_value="1"):
                result = asyncio.run(interactive_ask_user("Pick?", opts))
                assert result == "A"

    def test_recommended_passed_through(self):
        opts = [
            {"label": "A"},
            {"label": "B", "recommended": "true"},
        ]
        with patch("seeknal.ask.agents.tools.ask_user.sys") as mock_sys:
            mock_sys.stdout.isatty.return_value = False
            with patch("builtins.input", return_value="2"):
                result = asyncio.run(interactive_ask_user("Pick?", opts))
                assert result == "B"


class TestAskUserToolApprovalTracking:
    def test_records_report_approval_when_generate_report_selected(self, monkeypatch):
        from types import SimpleNamespace

        from seeknal.ask.agents.tools.ask_user_tool import ask_user

        ctx = SimpleNamespace(require_report_approval=True, report_approval_granted=False)
        options = [
            {"label": "Continue analysis"},
            {"label": "Generate report now"},
            {"label": "Done for now"},
            {"label": "Type your own"},
        ]

        monkeypatch.setattr(
            "seeknal.ask.agents.tools._context.get_tool_context",
            lambda: ctx,
        )
        monkeypatch.setattr(
            "seeknal.ask.agents.tools.ask_user.interactive_ask_user",
            lambda question, options: asyncio.sleep(0, result="Generate report now"),
        )

        result = asyncio.run(ask_user("What next?", options))
        assert result == "Generate report now"
        assert ctx.report_approval_granted is True
