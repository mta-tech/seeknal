"""Tests for the ask_user interactive tool."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from seeknal.ask.agents.tools._context import ToolContext, set_tool_context
from seeknal.ask.agents.tools.ask_user import (
    _BUDGET_EXHAUSTED,
    _SKIP_SENTINEL,
    ask_user,
)
from seeknal.ask.agents.profiles import get_tools_for_profile
from seeknal.ask.agents.prompt_builder import PromptBuilder


def _make_ctx(interactive: bool = False, questions_remaining: int = 5) -> ToolContext:
    """Create a minimal ToolContext for testing."""
    repl = MagicMock()
    discovery = MagicMock()
    return ToolContext(
        repl=repl,
        artifact_discovery=discovery,
        project_path=Path("/tmp/test"),
        console=MagicMock(),
        interactive=interactive,
        questions_remaining=questions_remaining,
    )


class TestAskUserGuards:
    """Test guard clauses that skip without user interaction."""

    def test_non_interactive_returns_skip(self):
        ctx = _make_ctx(interactive=False)
        set_tool_context(ctx)
        result = ask_user.invoke({"question": "Which key?", "options": ["a", "b"]})
        assert result == _SKIP_SENTINEL

    def test_exposure_mode_returns_skip(self):
        ctx = _make_ctx(interactive=True)
        ctx.exposure_mode = True
        set_tool_context(ctx)
        result = ask_user.invoke({"question": "Which key?", "options": ["a", "b"]})
        assert result == _SKIP_SENTINEL

    def test_budget_exhausted_returns_message(self):
        ctx = _make_ctx(interactive=True, questions_remaining=0)
        set_tool_context(ctx)
        result = ask_user.invoke({"question": "Which key?", "options": ["a", "b"]})
        assert result == _BUDGET_EXHAUSTED

    def test_budget_decrements_on_call(self):
        ctx = _make_ctx(interactive=True, questions_remaining=3)
        set_tool_context(ctx)
        with patch("seeknal.ask.agents.tools.ask_user.sys") as mock_sys:
            mock_sys.stdin.isatty.return_value = False
            with patch("builtins.input", return_value="1"):
                ask_user.invoke({"question": "Pick one", "options": ["a", "b"]})
        assert ctx.questions_remaining == 2


class TestAskUserSelection:
    """Test option selection and return values."""

    def test_returns_selected_option_text(self):
        ctx = _make_ctx(interactive=True)
        set_tool_context(ctx)
        with patch("seeknal.ask.agents.tools.ask_user.sys") as mock_sys:
            mock_sys.stdin.isatty.return_value = False
            with patch("builtins.input", return_value="1"):
                result = ask_user.invoke(
                    {"question": "Pick one", "options": ["alpha", "beta"]}
                )
        assert result == "alpha"

    def test_second_option_selected(self):
        ctx = _make_ctx(interactive=True)
        set_tool_context(ctx)
        with patch("seeknal.ask.agents.tools.ask_user.sys") as mock_sys:
            mock_sys.stdin.isatty.return_value = False
            with patch("builtins.input", return_value="2"):
                result = ask_user.invoke(
                    {"question": "Pick one", "options": ["alpha", "beta"]}
                )
        assert result == "beta"

    def test_type_your_own_option(self):
        ctx = _make_ctx(interactive=True)
        set_tool_context(ctx)
        # Options are ["a", "b", "Type your own..."] → index 3 is "Type your own"
        with patch("seeknal.ask.agents.tools.ask_user.sys") as mock_sys:
            mock_sys.stdin.isatty.return_value = False
            with patch("builtins.input", side_effect=["3", "my_custom_col"]):
                result = ask_user.invoke(
                    {"question": "Pick one", "options": ["a", "b"]}
                )
        assert result == "my_custom_col"

    def test_eof_returns_skip_sentinel(self):
        ctx = _make_ctx(interactive=True)
        set_tool_context(ctx)
        with patch("seeknal.ask.agents.tools.ask_user.sys") as mock_sys:
            mock_sys.stdin.isatty.return_value = False
            with patch("builtins.input", side_effect=EOFError):
                result = ask_user.invoke(
                    {"question": "Pick one", "options": ["a", "b"]}
                )
        assert result == _SKIP_SENTINEL

    def test_empty_input_returns_skip_sentinel(self):
        ctx = _make_ctx(interactive=True)
        set_tool_context(ctx)
        with patch("seeknal.ask.agents.tools.ask_user.sys") as mock_sys:
            mock_sys.stdin.isatty.return_value = False
            with patch("builtins.input", return_value=""):
                result = ask_user.invoke(
                    {"question": "Pick one", "options": ["a", "b"]}
                )
        assert result == _SKIP_SENTINEL

    def test_tty_uses_terminal_menu(self):
        ctx = _make_ctx(interactive=True)
        set_tool_context(ctx)
        with patch("seeknal.ask.agents.tools.ask_user.sys") as mock_sys:
            mock_sys.stdin.isatty.return_value = True
            with patch(
                "seeknal.ask.agents.tools.ask_user._select_with_arrow_keys",
                return_value=0,
            ):
                result = ask_user.invoke(
                    {"question": "Pick one", "options": ["alpha", "beta"]}
                )
        assert result == "alpha"

    def test_terminal_menu_failure_falls_back(self):
        ctx = _make_ctx(interactive=True)
        set_tool_context(ctx)
        with patch("seeknal.ask.agents.tools.ask_user.sys") as mock_sys:
            mock_sys.stdin.isatty.return_value = True
            with patch(
                "seeknal.ask.agents.tools.ask_user._select_with_arrow_keys",
                side_effect=Exception("terminal error"),
            ):
                with patch(
                    "seeknal.ask.agents.tools.ask_user._select_with_numbered_input",
                    return_value=1,
                ):
                    result = ask_user.invoke(
                        {"question": "Pick", "options": ["alpha", "beta"]}
                    )
        assert result == "beta"


class TestAskUserIntegration:
    """Integration tests for ask_user registration and prompt rendering."""

    def test_ask_user_in_all_profiles(self):
        for profile in ("analysis", "build", "full"):
            names = [t.name for t in get_tools_for_profile(profile)]
            assert "ask_user" in names, f"ask_user missing from {profile}"

    def test_ask_user_excluded_from_subagents(self):
        from seeknal.ask.agents.subagents import _PARENT_ONLY_TOOLS

        assert "ask_user" in _PARENT_ONLY_TOOLS

    def test_prompt_contains_ask_user_instructions(self):
        builder = PromptBuilder()
        prompt = builder.build("analysis")
        assert "ask_user" in prompt
        assert "Interactive Clarification" in prompt

    def test_prompt_renders_defaults(self):
        builder = PromptBuilder()
        prompt = builder.build(
            "analysis",
            defaults={"entity_key": "customer_id", "time_column": "event_time"},
        )
        assert "Project Defaults" in prompt
        assert "entity_key" in prompt
        assert "customer_id" in prompt

    def test_prompt_omits_defaults_when_empty(self):
        builder = PromptBuilder()
        prompt = builder.build("analysis", defaults={})
        assert "Project Defaults" not in prompt

    def test_build_prompt_references_ask_user(self):
        builder = PromptBuilder()
        prompt = builder.build("build")
        assert "ask_user()" in prompt

    def test_config_max_questions(self):
        ctx = _make_ctx(interactive=True, questions_remaining=3)
        ctx.max_questions = 3
        set_tool_context(ctx)
        assert ctx.max_questions == 3
        assert ctx.questions_remaining == 3


class TestExtractChoices:
    """Test _extract_choices parser for detecting options in agent text."""

    def test_numbered_options(self):
        from seeknal.ask.streaming import _extract_choices

        text = (
            "Here are your options:\n\n"
            "1. Build RFM Segmentation\n"
            "2. Focus on Churn Prediction\n"
            "3. Explain CLV in detail\n"
        )
        choices = _extract_choices(text)
        assert choices == [
            "Build RFM Segmentation",
            "Focus on Churn Prediction",
            "Explain CLV in detail",
        ]

    def test_bullet_options(self):
        from seeknal.ask.streaming import _extract_choices

        text = (
            "Would you like me to proceed?\n\n"
            "• Ya, buatkan pipeline\n"
            "• Tidak, cukup ringkasan\n"
            "• Jelaskan lebih detail\n"
        )
        choices = _extract_choices(text)
        assert choices == [
            "Ya, buatkan pipeline",
            "Tidak, cukup ringkasan",
            "Jelaskan lebih detail",
        ]

    def test_dash_options(self):
        from seeknal.ask.streaming import _extract_choices

        text = (
            "Choose one:\n\n"
            "- Option A\n"
            "- Option B\n"
        )
        choices = _extract_choices(text)
        assert choices == ["Option A", "Option B"]

    def test_no_options_returns_empty(self):
        from seeknal.ask.streaming import _extract_choices

        text = "Here is my analysis. Revenue is $52M. Customers: 80."
        assert _extract_choices(text) == []

    def test_single_option_returns_empty(self):
        from seeknal.ask.streaming import _extract_choices

        text = "Summary:\n\n1. Only one option\n"
        assert _extract_choices(text) == []

    def test_short_text_returns_empty(self):
        from seeknal.ask.streaming import _extract_choices

        text = "OK"
        assert _extract_choices(text) == []

    def test_options_at_end_only(self):
        from seeknal.ask.streaming import _extract_choices

        text = (
            "Analysis complete. Here are the findings:\n"
            "- Revenue grew 15%\n"
            "- Customers increased by 20%\n\n"
            "Some more analysis text here.\n\n"
            "What would you like to do?\n"
            "1. Generate a report\n"
            "2. Build a pipeline\n"
        )
        choices = _extract_choices(text)
        assert choices == ["Generate a report", "Build a pipeline"]

    def test_parenthetical_numbered(self):
        from seeknal.ask.streaming import _extract_choices

        text = (
            "Select:\n\n"
            "1) First choice\n"
            "2) Second choice\n"
            "3) Third choice\n"
        )
        choices = _extract_choices(text)
        assert choices == ["First choice", "Second choice", "Third choice"]
