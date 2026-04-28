"""pyte-based helpers for visual TUI testing.

Bridges Rich renderables to a pyte virtual terminal screen, enabling
assertions on character styling (fg color, bold, etc.) without a real
terminal.

Usage::

    from tests.ui.pyte_helpers import render_to_screen, TEAL, SUCCESS

    screen = render_to_screen(success_panel('ok'), width=40, height=6)
    assert_text_has_style(screen, 'ok', fg=SUCCESS)
"""

from __future__ import annotations

from io import StringIO
from typing import Any, Optional
from unittest.mock import patch

import pyte
from rich.console import Console, RenderableType

from seeknal.ui.theme import DARK_THEME

# ---------------------------------------------------------------------------
# Brand color constants — lowercase hex as pyte stores them
# ---------------------------------------------------------------------------

TEAL = "00bcb4"
TEAL_DIM = "008c86"
TEAL_BRIGHT = "33d6cf"
ORANGE = "ffa53c"
ORANGE_DIM = "cc842f"
ORANGE_BRIGHT = "ffb960"
SUCCESS = "6ddb7c"
ERROR = "ffa8b4"
WARNING = "ffcf40"
INFO = "8b9bff"
DIM = "555555"
MUTED = "808080"


# ---------------------------------------------------------------------------
# Core rendering helpers
# ---------------------------------------------------------------------------


def render_to_screen(
    renderable: RenderableType,
    width: int = 80,
    height: int = 24,
) -> pyte.Screen:
    """Render a Rich renderable into a pyte Screen.

    Creates a truecolor Rich Console that writes to StringIO, prints the
    renderable, then feeds the ANSI output into a pyte virtual terminal.
    """
    buf = StringIO()
    console = Console(
        file=buf,
        force_terminal=True,
        color_system="truecolor",
        no_color=False,
        theme=DARK_THEME,
        highlight=False,
        width=width,
    )
    console.print(renderable)

    screen = pyte.Screen(width, height)
    stream = pyte.Stream(screen)
    stream.feed(buf.getvalue())
    return screen


def make_capturing_console(width: int = 80) -> Console:
    """Create a truecolor Rich Console that captures output to StringIO.

    Use when you need to pass a Console to a function (e.g. streaming
    helpers) and then feed the output to pyte yourself.
    """
    return Console(
        file=StringIO(),
        force_terminal=True,
        color_system="truecolor",
        no_color=False,
        theme=DARK_THEME,
        highlight=False,
        width=width,
    )


def console_to_screen(console: Console, width: int = 80, height: int = 24) -> pyte.Screen:
    """Feed a capturing console's output into a pyte Screen."""
    screen = pyte.Screen(width, height)
    stream = pyte.Stream(screen)
    stream.feed(console.file.getvalue())
    return screen


def render_echo_to_screen(
    echo_fn: Any,
    message: str,
    width: int = 80,
    height: int = 5,
) -> pyte.Screen:
    """Render an echo function (echo_success, etc.) into a pyte Screen.

    Monkeypatches ``seeknal.ui.output.get_console`` so the echo function
    writes to our capturing console instead of the real terminal.
    """
    console = make_capturing_console(width)
    with patch("seeknal.ui.output.get_console", return_value=console):
        echo_fn(message)
    return console_to_screen(console, width, height)


# ---------------------------------------------------------------------------
# Screen query helpers
# ---------------------------------------------------------------------------


def _buffer_row_text(screen: pyte.Screen, row: int) -> str:
    """Build a text string from a screen buffer row.

    Iterates ``screen.buffer[row]`` and concatenates ``char.data``.
    This avoids pyte's Unicode width issues in ``screen.display``.
    """
    return "".join(screen.buffer[row][col].data for col in range(screen.columns))


def find_text(screen: pyte.Screen, text: str) -> list[tuple[int, int]]:
    """Find all occurrences of *text* in the screen.

    Returns a list of ``(row, col)`` tuples where *col* is the position
    within the buffer-reconstructed row string.
    """
    results = []
    for row in range(screen.lines):
        row_str = _buffer_row_text(screen, row)
        start = 0
        while True:
            idx = row_str.find(text, start)
            if idx == -1:
                break
            results.append((row, idx))
            start = idx + 1
    return results


def get_style_at(screen: pyte.Screen, row: int, col: int) -> dict:
    """Return the style attributes of a single character."""
    char = screen.buffer[row][col]
    return {
        "data": char.data,
        "fg": char.fg,
        "bg": char.bg,
        "bold": char.bold,
    }


def get_fg_colors_for_text(screen: pyte.Screen, text: str) -> set[str]:
    """Collect all fg colors used to render *text* on the screen."""
    colors = set()
    for row in range(screen.lines):
        row_str = _buffer_row_text(screen, row)
        start = 0
        while True:
            idx = row_str.find(text, start)
            if idx == -1:
                break
            for col in range(idx, idx + len(text)):
                colors.add(screen.buffer[row][col].fg)
            start = idx + 1
    return colors


# ---------------------------------------------------------------------------
# Assertion helpers
# ---------------------------------------------------------------------------


def assert_text_has_style(
    screen: pyte.Screen,
    text: str,
    fg: Optional[str] = None,
    bold: Optional[bool] = None,
) -> None:
    """Assert that *text* appears on screen with the given style.

    Searches the screen buffer row-by-row for the text, then checks
    that at least one occurrence has the expected style on its characters.
    """
    positions = find_text(screen, text)
    assert positions, f"Text {text!r} not found on screen. Display:\n" + "\n".join(
        screen.display
    )

    for row, col in positions:
        match = True
        for i, ch in enumerate(text):
            char = screen.buffer[row][col + i]
            if fg is not None and char.fg != fg:
                match = False
                break
            if bold is not None and char.bold != bold:
                match = False
                break
        if match:
            return  # Found a matching occurrence

    # Build diagnostic message
    sample_row, sample_col = positions[0]
    actual_styles = []
    for i in range(len(text)):
        c = screen.buffer[sample_row][sample_col + i]
        actual_styles.append(f"  '{c.data}' fg={c.fg!r} bold={c.bold}")
    raise AssertionError(
        f"Text {text!r} found but style mismatch.\n"
        f"Expected: fg={fg!r} bold={bold!r}\n"
        f"Actual chars:\n" + "\n".join(actual_styles)
    )


def assert_text_present(screen: pyte.Screen, text: str) -> None:
    """Assert that *text* appears somewhere on the screen."""
    positions = find_text(screen, text)
    assert positions, f"Text {text!r} not found on screen. Display:\n" + "\n".join(
        screen.display
    )


def find_char(screen: pyte.Screen, char: str) -> list[tuple[int, int]]:
    """Find all positions of a single character in the buffer."""
    results = []
    for row in range(screen.lines):
        for col in range(screen.columns):
            if screen.buffer[row][col].data == char:
                results.append((row, col))
    return results
