"""
Seeknal brand mark — animated signal-graph mascot.

Renders the Seeknal logo as Rich-styled Unicode: a rounded frame
containing three connected nodes (two teal, one orange) with a
"signal pulse" idle animation cycling between the source and the
destination node.

The module filename and public API (``render_fox``,
``render_animated_fox``, ``IDLE_SEQUENCE``, ``TICK_MS``, ``EYES``)
are retained so existing imports keep working.

Falls back to uncolored ASCII when TERM=dumb or non-interactive.
"""

from __future__ import annotations

import itertools
from typing import Iterator, Literal

from rich.text import Text

from seeknal.ui.figures import get_tier

# -- Constants -----------------------------------------------------------------

Pose = Literal["default", "look_left", "look_right"]

# Retained for backward-compat. The signal-graph mark doesn't have eyes,
# so the ``eyes`` parameter is accepted but ignored during rendering.
EYES: dict[str, str] = {
    "default": "°",
    "sparkle": "✦",
    "dot": "·",
    "wide": "◉",
    "closed": "-",
}

# Animation timing
IDLE_SEQUENCE: list[int] = [0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 0]
TICK_MS: int = 500

# -- Brand palette (matches docs/assets/logos/seeknal-mark-dark.svg) ----------

_TEAL = "#00CBA8"
_ORANGE = "#FF6430"

T = _TEAL                  # node A (source, teal)
Td = f"dim {_TEAL}"        # node B + A→B line (secondary teal)
O = _ORANGE                # node C (destination, orange)
Od = f"dim {_ORANGE}"      # B→C line (secondary orange)
B = _TEAL                  # border
Hi_T = f"bold {_TEAL}"     # pulse highlight (teal)
Hi_O = f"bold {_ORANGE}"   # pulse highlight (orange)

# -- Sprite frames -------------------------------------------------------------
#
# Three 5-line × 10-char frames.  Layout mirrors the SVG mark:
#
#     ╭──────╮      border          (teal)
#     │ ●──◐ │      A───B           (teal → dim teal)
#     │   ╱  │      diagonal B→C    (dim orange)
#     │  ●   │      C (orange node)
#     ╰──────╯
#
# Animation:  frame 0 = rest, frame 1 = source pulse, frame 2 = dest pulse.


def _frame0() -> list[list[tuple[str, str]]]:
    """Rest state — all nodes at normal intensity."""
    return [
        [(" ", ""), ("╭──────╮", B), (" ", "")],
        [(" ", ""), ("│", B), (" ", ""), ("●", T), ("──", Td), ("◐", Td), (" ", ""), ("│", B), (" ", "")],
        [(" ", ""), ("│", B), ("   ", ""), ("╱", Od), ("  ", ""), ("│", B), (" ", "")],
        [(" ", ""), ("│", B), ("  ", ""), ("●", O), ("   ", ""), ("│", B), (" ", "")],
        [(" ", ""), ("╰──────╯", B), (" ", "")],
    ]


def _frame1() -> list[list[tuple[str, str]]]:
    """Source node pulses — signal origin."""
    return [
        [(" ", ""), ("╭──────╮", B), (" ", "")],
        [(" ", ""), ("│", B), (" ", ""), ("◉", Hi_T), ("──", T), ("◐", Td), (" ", ""), ("│", B), (" ", "")],
        [(" ", ""), ("│", B), ("   ", ""), ("╱", Od), ("  ", ""), ("│", B), (" ", "")],
        [(" ", ""), ("│", B), ("  ", ""), ("●", O), ("   ", ""), ("│", B), (" ", "")],
        [(" ", ""), ("╰──────╯", B), (" ", "")],
    ]


def _frame2() -> list[list[tuple[str, str]]]:
    """Destination node pulses — signal arrives."""
    return [
        [(" ", ""), ("╭──────╮", B), (" ", "")],
        [(" ", ""), ("│", B), (" ", ""), ("●", T), ("──", Td), ("◐", Td), (" ", ""), ("│", B), (" ", "")],
        [(" ", ""), ("│", B), ("   ", ""), ("╱", O), ("  ", ""), ("│", B), (" ", "")],
        [(" ", ""), ("│", B), ("  ", ""), ("◉", Hi_O), ("   ", ""), ("│", B), (" ", "")],
        [(" ", ""), ("╰──────╯", B), (" ", "")],
    ]


_FRAMES = [_frame0, _frame1, _frame2]


# -- ASCII-only frames ---------------------------------------------------------


def _ascii_frame(highlight: str = "none") -> list[list[tuple[str, str]]]:
    """ASCII fallback for TERM=dumb and legacy terminals."""
    src = "@" if highlight == "source" else "*"
    dst = "@" if highlight == "dest" else "o"
    return [
        [(" +------+ ", "")],
        [(" | " + src + "--. | ", "")],
        [(" |   /  | ", "")],
        [(" |  " + dst + "   | ", "")],
        [(" +------+ ", "")],
    ]


# ==============================================================================
# Public API
# ==============================================================================


def render_fox(
    pose: Pose = "default",
    frame: int = 0,
    eyes: str = "default",
) -> Text:
    """Render the Seeknal signal-graph mark as a Rich ``Text`` renderable.

    Parameters
    ----------
    pose:
        Retained for backward-compatibility; ignored.
    frame:
        Animation frame index (0, 1, or 2).
    eyes:
        Retained for backward-compatibility; ignored.
    """
    tier = get_tier()

    if tier == "ascii_only":
        highlight = {0: "none", 1: "source", 2: "dest"}.get(frame % 3, "none")
        rows = _ascii_frame(highlight)
    else:
        rows = _FRAMES[frame % len(_FRAMES)]()

    text = Text()
    for i, row in enumerate(rows):
        for segment_text, style in row:
            if tier == "ascii_only":
                text.append(segment_text)
            else:
                text.append(segment_text, style=style if style else None)
        if i < len(rows) - 1:
            text.append("\n")

    return text


def render_animated_fox(eyes: str = "default") -> Iterator[Text]:
    """Yield frames following the idle sequence.

    Each yielded ``Text`` corresponds to one animation tick (500ms).
    The sequence cycles indefinitely.
    """
    for step in itertools.cycle(IDLE_SEQUENCE):
        yield render_fox(frame=max(0, step), eyes=eyes)
