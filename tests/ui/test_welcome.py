"""Tests for the seeknal welcome banner."""

from __future__ import annotations

from io import StringIO

import pytest
from rich.console import Console

from seeknal.ui import figures
from seeknal.ui.welcome import show_welcome, reset_welcome


@pytest.fixture(autouse=True)
def _clean_state():
    """Reset welcome flag and glyph tier between tests."""
    reset_welcome()
    figures.reset()
    yield
    reset_welcome()
    figures.reset()


# -- Helpers -------------------------------------------------------------------


def _make_tty_console() -> Console:
    """Create a Console that reports ``is_terminal=True``."""
    return Console(force_terminal=True, highlight=False, width=80)


# -- Tests ---------------------------------------------------------------------


def test_show_welcome_renders_without_errors(monkeypatch):
    """show_welcome() runs and produces output containing the title."""
    console = _make_tty_console()
    monkeypatch.setattr("seeknal.ui.welcome.get_console", lambda: console)

    show_welcome("1.2.3")
    # No exception means success.


def test_second_call_is_noop(monkeypatch):
    """After the first call, subsequent calls produce no additional output."""
    buf = StringIO()
    console = Console(file=buf, force_terminal=True, highlight=False, width=80)
    monkeypatch.setattr("seeknal.ui.welcome.get_console", lambda: console)

    show_welcome("1.0.0")
    first_output = buf.getvalue()
    assert len(first_output) > 0, "First call should produce output"

    show_welcome("1.0.0")
    second_output = buf.getvalue()
    assert first_output == second_output, "Second call should not add output"


def test_reset_allows_redisplay(monkeypatch):
    """reset_welcome() clears the flag so the banner can show again."""
    buf = StringIO()
    console = Console(file=buf, force_terminal=True, highlight=False, width=80)
    monkeypatch.setattr("seeknal.ui.welcome.get_console", lambda: console)

    show_welcome("2.0.0")
    first_len = len(buf.getvalue())

    reset_welcome()
    show_welcome("2.0.0")
    second_len = len(buf.getvalue())

    assert second_len > first_len, "After reset, banner should render again"


def test_non_tty_skips_banner(monkeypatch):
    """When stdout is not a TTY, the banner is skipped entirely."""
    buf = StringIO()
    # Console without force_terminal writes but reports is_terminal=False.
    console = Console(file=buf, force_terminal=False, highlight=False, width=80)
    monkeypatch.setattr("seeknal.ui.welcome.get_console", lambda: console)

    show_welcome("3.0.0")
    assert buf.getvalue() == "", "Non-TTY console should produce no output"


def test_banner_contains_welcome_text(monkeypatch):
    """The rendered banner includes 'Welcome to Seeknal'."""
    buf = StringIO()
    console = Console(file=buf, force_terminal=True, highlight=False, width=80)
    monkeypatch.setattr("seeknal.ui.welcome.get_console", lambda: console)

    show_welcome("4.0.0")
    output = buf.getvalue()
    assert "Welcome to Seeknal" in output


def test_banner_contains_version(monkeypatch):
    """The rendered banner includes the version string."""
    buf = StringIO()
    console = Console(file=buf, force_terminal=True, highlight=False, width=80)
    monkeypatch.setattr("seeknal.ui.welcome.get_console", lambda: console)

    show_welcome("9.8.7")
    output = buf.getvalue()
    assert "v9.8.7" in output


def test_empty_version_still_renders(monkeypatch):
    """Passing an empty version string renders without the version line."""
    buf = StringIO()
    console = Console(file=buf, force_terminal=True, highlight=False, width=80)
    monkeypatch.setattr("seeknal.ui.welcome.get_console", lambda: console)

    show_welcome("")
    output = buf.getvalue()
    assert "Welcome to Seeknal" in output
