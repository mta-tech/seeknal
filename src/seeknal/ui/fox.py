"""
Seeknal fox mascot rendered in Unicode half-block art.

Uses upper/lower half-block characters with fg+bg Rich colors
to achieve 2x vertical resolution. Provides three poses:
  - "default": facing forward
  - "look_left": head turned left
  - "look_right": head turned right

Falls back to ASCII art when TERM=dumb or non-interactive.
"""

from __future__ import annotations

from typing import Literal

from rich.text import Text

from seeknal.ui.figures import get_tier

# -- Color tokens --------------------------------------------------------------

_TEAL = "#00BCB4"
_ORANGE = "#FFA53C"

Pose = Literal["default", "look_left", "look_right"]

# ==============================================================================
# Half-block art
# ==============================================================================
#
# Each line uses (text, style) segments.  The ▀/▄ characters with fg+bg
# colors encode two pixel rows per character row (2x vertical resolution).
#
#   ▀ = fg color on top half, bg color on bottom half
#   ▄ = bg color on top half, fg color on bottom half
#   █ = solid fg color both halves
#   (space with bg) = solid bg color both halves
#
# Shorthand:
#   T = teal body, O = orange accent, W = bold white

T = _TEAL
O = _ORANGE


def _default_rows() -> list[list[tuple[str, str]]]:
    """Fox facing forward, ears up, tail curling right."""
    return [
        # Row 0: ear tips
        [("  ", ""), ("▄", O), ("      ", ""), ("▄", O)],
        # Row 1: ears merging into head
        [(" ", ""), ("▄", f"{O} on {T}"), ("▄▀▀▀▀▄", T),
         ("▄", f"{O} on {T}")],
        # Row 2: face — eyes
        [(" ", ""), ("█", T), (" ", f"bold white on {T}"),
         ("▄▄", T), (" ", f"bold white on {T}"), ("█", T)],
        # Row 3: muzzle — nose
        [(" ", ""), (" ", f"on {T}"), ("▀", T), ("▄", O),
         ("▀", T), (" ", f"on {T}")],
        # Row 4: body widens, tail starts
        [("  ", ""), ("▄", T), ("████", T), ("▄", T), ("▄▄", O)],
        # Row 5: lower body, tail curves
        [("  ", ""), (" ", f"on {T}"), ("▄▄▄▄", T),
         (" ", f"on {T}"), (" ▄", O)],
        # Row 6: feet + tail tip
        [("  ", ""), ("▀", T), ("▀  ▀", ""), ("▀", T), ("  ▀", O)],
    ]


def _look_left_rows() -> list[list[tuple[str, str]]]:
    """Fox looking left — eyes and nose shifted left."""
    return [
        # Row 0: ear tips
        [("  ", ""), ("▄", O), ("      ", ""), ("▄", O)],
        # Row 1: ears + head
        [(" ", ""), ("▄", f"{O} on {T}"), ("▄▀▀▀▀▄", T),
         ("▄", f"{O} on {T}")],
        # Row 2: eyes shifted left
        [(" ", ""), ("█", T), (" ", f"bold white on {T}"),
         ("▄", T), (" ", f"bold white on {T}"), ("▄█", T)],
        # Row 3: nose shifted left
        [(" ", ""), ("▄", O), ("▀▀▀", T), ("▀", T), (" ", f"on {T}")],
        # Row 4: body + tail
        [("  ", ""), ("▄", T), ("████", T), ("▄", T), ("▄▄", O)],
        # Row 5: lower body
        [("  ", ""), (" ", f"on {T}"), ("▄▄▄▄", T),
         (" ", f"on {T}"), (" ▄", O)],
        # Row 6: feet + tail
        [("  ", ""), ("▀", T), ("▀  ▀", ""), ("▀", T), ("  ▀", O)],
    ]


def _look_right_rows() -> list[list[tuple[str, str]]]:
    """Fox looking right — eyes and nose shifted right."""
    return [
        # Row 0: ear tips
        [("  ", ""), ("▄", O), ("      ", ""), ("▄", O)],
        # Row 1: ears + head
        [(" ", ""), ("▄", f"{O} on {T}"), ("▄▀▀▀▀▄", T),
         ("▄", f"{O} on {T}")],
        # Row 2: eyes shifted right
        [(" ", ""), ("█▄", T), (" ", f"bold white on {T}"),
         ("▄", T), (" ", f"bold white on {T}"), ("█", T)],
        # Row 3: nose shifted right
        [(" ", ""), (" ", f"on {T}"), ("▀", T), ("▀▀▀", T), ("▄", O)],
        # Row 4: body + tail
        [("  ", ""), ("▄", T), ("████", T), ("▄", T), ("▄▄", O)],
        # Row 5: lower body
        [("  ", ""), (" ", f"on {T}"), ("▄▄▄▄", T),
         (" ", f"on {T}"), (" ▄", O)],
        # Row 6: feet + tail
        [("  ", ""), ("▀", T), ("▀  ▀", ""), ("▀", T), ("  ▀", O)],
    ]


_POSE_BUILDERS = {
    "default": _default_rows,
    "look_left": _look_left_rows,
    "look_right": _look_right_rows,
}

# ==============================================================================
# ASCII fallback
# ==============================================================================

_ASCII_DEFAULT = r"""
  /\_/\
 ( o.o )
  > ^ <
 /|   |\~
(_|   |_)
""".strip("\n")

_ASCII_LOOK_LEFT = r"""
  /\_/\
 (o.o  )
  > ^ <
 /|   |\~
(_|   |_)
""".strip("\n")

_ASCII_LOOK_RIGHT = r"""
  /\_/\
 (  o.o)
  > ^ <
 /|   |\~
(_|   |_)
""".strip("\n")

_ASCII_POSES = {
    "default": _ASCII_DEFAULT,
    "look_left": _ASCII_LOOK_LEFT,
    "look_right": _ASCII_LOOK_RIGHT,
}

# ==============================================================================
# Public API
# ==============================================================================


def render_fox(pose: Pose = "default") -> Text:
    """Render the seeknal fox mascot as a Rich ``Text`` renderable.

    Parameters
    ----------
    pose:
        One of ``"default"``, ``"look_left"``, ``"look_right"``.

    Returns
    -------
    Rich ``Text`` object suitable for ``console.print()``.
    """
    tier = get_tier()

    if tier == "ascii_only":
        return Text(_ASCII_POSES.get(pose, _ASCII_DEFAULT))

    builder = _POSE_BUILDERS.get(pose, _default_rows)
    rows = builder()

    text = Text()
    for i, row in enumerate(rows):
        for segment_text, style in row:
            text.append(segment_text, style=style if style else None)
        if i < len(rows) - 1:
            text.append("\n")

    return text
