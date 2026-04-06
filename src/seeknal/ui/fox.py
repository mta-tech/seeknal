"""
Seeknal bird mascot — Claude Code buddy sprite style.

A seeknal-branded duck rendered in colored ASCII art with Rich styles.
Uses {E} eye placeholders, 3 animation frames, and an idle fidget sequence
matching Claude Code's companion sprite system.

Provides:
  - render_fox(frame, eyes) → Rich Text (backward-compat function name)
  - render_animated_fox(eyes) → Iterator[Text] (yields frames per idle tick)
  - IDLE_SEQUENCE, TICK_MS for animation timing

Falls back to uncolored ASCII when TERM=dumb or non-interactive.
"""

from __future__ import annotations

import itertools
from typing import Iterator, Literal

from rich.text import Text

from seeknal.ui.figures import get_tier

# -- Constants -----------------------------------------------------------------

Pose = Literal["default", "look_left", "look_right"]

EYES: dict[str, str] = {
    "default": "°",
    "sparkle": "✦",
    "dot": "·",
    "wide": "◉",
    "closed": "-",
}

# Animation timing (matches Claude Code exactly)
IDLE_SEQUENCE: list[int] = [0, 0, 0, 0, 1, 0, 0, 0, -1, 0, 0, 2, 0, 0, 0]
TICK_MS: int = 500

# -- Color tokens --------------------------------------------------------------

T = "#00BCB4"   # teal (brand.primary)
O = "#FFA53C"   # orange (brand.accent)
W = "bold white"  # eyes

# -- Sprite frames -------------------------------------------------------------
#
# 3 frames, each 4 lines (no blank hat line).  12 chars wide.
# {E} is replaced with the eye character at render time.
#
# Frame 0 (neutral):
#     __
#   <({E} )___
#    (  ._>
#     `--'
#
# Frame 1 (tail splash):
#     __
#   <({E} )___
#    (  ._>
#     `--'~
#
# Frame 2 (beak move):
#     __
#   <({E} )___
#    (  .__>
#     `--'


def _frame0() -> list[list[tuple[str, str]]]:
    return [
        [("    ", ""), ("__", O), ("      ", "")],
        [("  ", ""), ("<(", O), ("{E}", W), (" )", O), ("___", O), ("  ", "")],
        [("   ", ""), ("(  ._>", T), ("   ", "")],
        [("    ", ""), ("`--'", T), ("    ", "")],
    ]


def _frame1() -> list[list[tuple[str, str]]]:
    return [
        [("    ", ""), ("__", O), ("      ", "")],
        [("  ", ""), ("<(", O), ("{E}", W), (" )", O), ("___", O), ("  ", "")],
        [("   ", ""), ("(  ._>", T), ("   ", "")],
        [("    ", ""), ("`--'", T), ("~", O), ("   ", "")],
    ]


def _frame2() -> list[list[tuple[str, str]]]:
    return [
        [("    ", ""), ("__", O), ("      ", "")],
        [("  ", ""), ("<(", O), ("{E}", W), (" )", O), ("___", O), ("  ", "")],
        [("   ", ""), ("(  .__>", T), ("  ", "")],
        [("    ", ""), ("`--'", T), ("    ", "")],
    ]


_FRAMES = [_frame0, _frame1, _frame2]

# ==============================================================================
# Public API
# ==============================================================================


def render_fox(
    pose: Pose = "default",
    frame: int = 0,
    eyes: str = "default",
) -> Text:
    """Render the seeknal bird mascot as a Rich ``Text`` renderable.

    Parameters
    ----------
    pose:
        Kept for backward compatibility. Ignored (bird has one orientation).
    frame:
        Animation frame index (0, 1, or 2).
    eyes:
        Eye style name from ``EYES`` dict, or a single character.

    Returns
    -------
    Rich ``Text`` object suitable for ``console.print()``.
    """
    tier = get_tier()
    if tier == "ascii_only":
        # Use ASCII-safe eye characters
        ascii_eyes = {"default": "o", "sparkle": "*", "dot": ".", "wide": "O", "closed": "-"}
        eye_char = ascii_eyes.get(eyes, "o")
    else:
        eye_char = EYES.get(eyes, eyes[0] if eyes else "°")

    builder = _FRAMES[frame % len(_FRAMES)]
    rows = builder()

    text = Text()
    for i, row in enumerate(rows):
        for segment_text, style in row:
            resolved = segment_text.replace("{E}", eye_char)
            if tier == "ascii_only":
                text.append(resolved)
            else:
                text.append(resolved, style=style if style else None)
        if i < len(rows) - 1:
            text.append("\n")

    return text


def render_animated_fox(eyes: str = "default") -> Iterator[Text]:
    """Yield frames following the idle sequence.

    Each yielded ``Text`` corresponds to one animation tick (500ms).
    The sequence cycles indefinitely. Frame index ``-1`` triggers a blink
    (eyes replaced with ``-``).
    """
    for step in itertools.cycle(IDLE_SEQUENCE):
        if step == -1:
            yield render_fox(frame=0, eyes="closed")
        else:
            yield render_fox(frame=step, eyes=eyes)
