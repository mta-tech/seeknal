"""Tests for InteractiveMenu — key handling + Live loop."""

from io import StringIO
from unittest.mock import MagicMock, patch

import pytest

from rich.console import Console

from seeknal.ui.interactive_menu import InteractiveMenu
from seeknal.ui.theme import DARK_THEME


def make_console(width: int = 80) -> Console:
    """Create a Rich Console that captures output."""
    buf = StringIO()
    return Console(file=buf, force_terminal=False, width=width, theme=DARK_THEME)


OPTIONS = [
    {"label": "Alpha", "description": "First option"},
    {"label": "Beta", "description": "Second option"},
    {"label": "Gamma", "description": "Third option"},
]


class TestHandleKey:
    """Test key handling logic without terminal I/O."""

    def test_up_moves_cursor(self):
        menu = InteractiveMenu("Pick?", OPTIONS, make_console())
        menu._state.cursor = 1
        menu._handle_key("up")
        assert menu._state.cursor == 0

    def test_down_moves_cursor(self):
        menu = InteractiveMenu("Pick?", OPTIONS, make_console())
        menu._state.cursor = 0
        menu._handle_key("down")
        assert menu._state.cursor == 1

    def test_up_wraps_around(self):
        menu = InteractiveMenu("Pick?", OPTIONS, make_console())
        menu._state.cursor = 0
        menu._handle_key("up")
        # Wraps to last option (3 options + "Other" = 4, so index 3)
        assert menu._state.cursor == 3

    def test_down_wraps_around(self):
        menu = InteractiveMenu("Pick?", OPTIONS, make_console())
        menu._state.cursor = 3  # Last option ("Other")
        menu._handle_key("down")
        assert menu._state.cursor == 0

    def test_enter_confirms(self):
        menu = InteractiveMenu("Pick?", OPTIONS, make_console())
        menu._state.cursor = 1
        menu._handle_key("enter")
        assert menu._done is True

    def test_enter_on_other_enters_text_mode(self):
        menu = InteractiveMenu("Pick?", OPTIONS, make_console())
        other_idx = len(menu._state.options) - 1
        menu._state.cursor = other_idx
        menu._handle_key("enter")
        assert menu._state.in_text_mode is True
        assert menu._done is False

    def test_space_in_single_select_confirms(self):
        menu = InteractiveMenu("Pick?", OPTIONS, make_console())
        menu._state.cursor = 1
        menu._handle_key("space")
        assert menu._done is True

    def test_escape_cancels(self):
        menu = InteractiveMenu("Pick?", OPTIONS, make_console())
        menu._state.cursor = 2
        menu._handle_key("escape")
        assert menu._done is True
        assert menu._state.cursor == 0  # Reset to first option

    def test_multi_select_space_toggles(self):
        menu = InteractiveMenu("Pick?", OPTIONS, make_console(), multi_select=True)
        menu._state.cursor = 1
        menu._handle_key("space")
        assert 1 in menu._state.selected

        # Toggle off
        menu._handle_key("space")
        assert 1 not in menu._state.selected

    def test_multi_select_enter_confirms(self):
        menu = InteractiveMenu("Pick?", OPTIONS, make_console(), multi_select=True)
        menu._state.selected = {0, 2}
        menu._handle_key("enter")
        assert menu._done is True


class TestTextMode:
    """Test text input mode key handling."""

    def test_typing_appends(self):
        menu = InteractiveMenu("Pick?", OPTIONS, make_console())
        menu._state.in_text_mode = True
        menu._handle_key("h")
        menu._handle_key("i")
        assert menu._state.text_input == "hi"

    def test_space_appends_literal_space(self):
        menu = InteractiveMenu("Pick?", OPTIONS, make_console())
        menu._state.in_text_mode = True
        menu._state.text_input = "let"
        menu._handle_key("space")
        menu._handle_key("d")
        menu._handle_key("a")
        menu._handle_key("t")
        menu._handle_key("a")
        assert menu._state.text_input == "let data"

    def test_backspace_deletes(self):
        menu = InteractiveMenu("Pick?", OPTIONS, make_console())
        menu._state.in_text_mode = True
        menu._state.text_input = "hello"
        menu._handle_key("backspace")
        assert menu._state.text_input == "hell"

    def test_backspace_on_empty(self):
        menu = InteractiveMenu("Pick?", OPTIONS, make_console())
        menu._state.in_text_mode = True
        menu._state.text_input = ""
        menu._handle_key("backspace")
        assert menu._state.text_input == ""

    def test_enter_confirms_text(self):
        menu = InteractiveMenu("Pick?", OPTIONS, make_console())
        menu._state.in_text_mode = True
        menu._state.text_input = "custom"
        menu._handle_key("enter")
        assert menu._done is True

    def test_escape_exits_text_mode(self):
        menu = InteractiveMenu("Pick?", OPTIONS, make_console())
        menu._state.in_text_mode = True
        menu._state.text_input = "partial"
        menu._handle_key("escape")
        assert menu._state.in_text_mode is False
        assert menu._state.text_input == ""
        assert menu._done is False

    def test_non_printable_ignored(self):
        menu = InteractiveMenu("Pick?", OPTIONS, make_console())
        menu._state.in_text_mode = True
        menu._handle_key("up")  # Should be ignored in text mode
        assert menu._state.text_input == ""


class TestGetAnswer:
    """Test that InteractiveMenu returns correct answers."""

    def test_single_select_returns_label(self):
        menu = InteractiveMenu("Pick?", OPTIONS, make_console())
        menu._state.cursor = 2
        menu._done = True
        assert menu._state.get_answer() == "Gamma"

    def test_multi_select_returns_comma_separated(self):
        menu = InteractiveMenu("Pick?", OPTIONS, make_console(), multi_select=True)
        menu._state.selected = {0, 2}
        assert menu._state.get_answer() == "Alpha, Gamma"

    def test_other_with_text_returns_text(self):
        menu = InteractiveMenu("Pick?", OPTIONS, make_console())
        other_idx = len(menu._state.options) - 1
        menu._state.cursor = other_idx
        menu._state.text_input = "My custom answer"
        assert menu._state.get_answer() == "My custom answer"


class TestFallbackNumbered:
    """Test non-TTY fallback."""

    def test_numbered_fallback_selects_option(self):
        menu = InteractiveMenu("Pick?", OPTIONS, make_console())
        with patch("builtins.input", return_value="2"):
            result = menu._fallback_numbered()
        assert result == "Beta"

    def test_numbered_fallback_invalid_returns_first(self):
        menu = InteractiveMenu("Pick?", OPTIONS, make_console())
        with patch("builtins.input", side_effect=ValueError):
            result = menu._fallback_numbered()
        assert result == "Alpha"

    def test_numbered_fallback_eof_returns_first(self):
        menu = InteractiveMenu("Pick?", OPTIONS, make_console())
        with patch("builtins.input", side_effect=EOFError):
            result = menu._fallback_numbered()
        assert result == "Alpha"

    def test_numbered_fallback_other_option(self):
        menu = InteractiveMenu("Pick?", OPTIONS, make_console())
        other_idx = len(menu._state.options)
        with patch("builtins.input", side_effect=[str(other_idx), "custom text"]):
            result = menu._fallback_numbered()
        assert result == "custom text"
