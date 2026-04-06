"""Tests for menu renderer — Rich renderables."""

from io import StringIO

from rich.console import Console

from seeknal.ui.menu_render import render_menu, _build_options_text, _build_help_footer
from seeknal.ui.menu_state import MenuState
from seeknal.ui.theme import DARK_THEME


def make_console(width: int = 80) -> tuple[Console, StringIO]:
    """Create a Rich Console that captures output to a string."""
    buf = StringIO()
    return Console(file=buf, force_terminal=False, width=width, theme=DARK_THEME), buf


class TestRenderMenu:
    def test_contains_question_text(self):
        state = MenuState.from_options("Which DB?", [{"label": "Postgres"}, {"label": "MySQL"}])
        console, buf = make_console()
        console.print(render_menu(state))
        output = buf.getvalue()
        assert "Which DB?" in output

    def test_contains_option_labels(self):
        state = MenuState.from_options("Pick?", [{"label": "Alpha"}, {"label": "Beta"}])
        console, buf = make_console()
        console.print(render_menu(state))
        output = buf.getvalue()
        assert "Alpha" in output
        assert "Beta" in output
        assert "Other" in output

    def test_contains_description(self):
        state = MenuState.from_options(
            "Pick?", [{"label": "A", "description": "First option"}]
        )
        console, buf = make_console()
        console.print(render_menu(state))
        output = buf.getvalue()
        assert "First option" in output

    def test_recommended_badge(self):
        state = MenuState.from_options(
            "Pick?", [{"label": "A", "recommended": "true"}, {"label": "B"}]
        )
        console, buf = make_console()
        console.print(render_menu(state))
        output = buf.getvalue()
        assert "recommended" in output

    def test_no_crash_on_empty_options(self):
        state = MenuState.from_options("Pick?", [])
        console, buf = make_console()
        console.print(render_menu(state))
        output = buf.getvalue()
        assert "Other" in output

    def test_preview_layout_wide_terminal(self):
        """With preview content and wide terminal, uses split layout."""
        state = MenuState.from_options(
            "Layout?",
            [
                {"label": "Grid", "preview": "```\n+--+\n```"},
                {"label": "List"},
            ],
        )
        console, buf = make_console(width=100)
        console.print(render_menu(state, console_width=100))
        output = buf.getvalue()
        assert "Preview" in output
        assert "Grid" in output

    def test_preview_falls_back_on_narrow_terminal(self):
        """On narrow terminals (<60), preview is not used."""
        state = MenuState.from_options(
            "Layout?",
            [
                {"label": "Grid", "preview": "```\n+--+\n```"},
                {"label": "List"},
            ],
        )
        console, buf = make_console(width=50)
        renderable = render_menu(state, console_width=50)
        console.print(renderable)
        output = buf.getvalue()
        assert "Grid" in output
        # No "Preview" panel title when narrow
        # (it renders as a simple panel without the split layout)


class TestBuildOptionsText:
    def test_focused_option_has_cursor(self):
        state = MenuState.from_options("Pick?", [{"label": "A"}, {"label": "B"}])
        state.cursor = 0
        text = _build_options_text(state)
        plain = text.plain
        assert "\u203a" in plain  # › cursor

    def test_multi_select_toggle(self):
        from seeknal.ui import figures

        state = MenuState.from_options(
            "Pick?", [{"label": "A"}, {"label": "B"}], multi_select=True
        )
        state.selected = {0}
        text = _build_options_text(state)
        plain = text.plain
        diamond = figures.get("diamond")
        diamond_open = figures.get("diamond_open")
        # Selected option should show filled diamond, unselected should show open
        assert diamond in plain
        assert diamond_open in plain

    def test_text_input_mode_shows_cursor(self):
        state = MenuState.from_options("Pick?", [{"label": "A"}])
        state.in_text_mode = True
        state.text_input = "hello"
        text = _build_options_text(state)
        plain = text.plain
        assert "hello" in plain
        assert "\u2588" in plain  # Block cursor


class TestBuildHelpFooter:
    def test_single_select_help(self):
        state = MenuState.from_options("Pick?", [{"label": "A"}])
        help_text = _build_help_footer(state)
        plain = help_text.plain
        assert "navigate" in plain
        assert "select" in plain
        assert "confirm" in plain

    def test_multi_select_help(self):
        state = MenuState.from_options("Pick?", [{"label": "A"}], multi_select=True)
        help_text = _build_help_footer(state)
        plain = help_text.plain
        assert "toggle" in plain

    def test_text_mode_help(self):
        state = MenuState.from_options("Pick?", [{"label": "A"}])
        state.in_text_mode = True
        help_text = _build_help_footer(state)
        plain = help_text.plain
        assert "esc cancel" in plain
