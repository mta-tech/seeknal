"""Tests for the seeknal signal-graph brand-mark renderer (fox.py)."""

from __future__ import annotations

import pytest
from rich.text import Text

from seeknal.ui import figures
from seeknal.ui.fox import (
    EYES,
    IDLE_SEQUENCE,
    render_animated_fox,
    render_fox,
)

# -- Helpers -------------------------------------------------------------------

_MAX_WIDTH = 12
_MAX_LINES = 5
_FRAMES = [0, 1, 2]


@pytest.fixture(autouse=True)
def _reset_figures():
    """Reset glyph tier detection between tests."""
    figures.reset()
    yield
    figures.reset()


def _force_tier(tier: str):
    """Override the detected glyph tier."""
    figures._current_tier = tier
    figures._current_glyphs = figures._GLYPH_SETS[tier]


# -- Tests: all frames render without errors -----------------------------------


@pytest.mark.parametrize("frame", _FRAMES)
def test_render_returns_rich_text(frame: int):
    """render_fox() returns a Rich Text object for every frame."""
    _force_tier("unicode_full")
    result = render_fox(frame=frame)
    assert isinstance(result, Text)


@pytest.mark.parametrize("frame", _FRAMES)
def test_render_ascii_returns_rich_text(frame: int):
    """render_fox() returns a Rich Text object in ASCII mode."""
    _force_tier("ascii_only")
    result = render_fox(frame=frame)
    assert isinstance(result, Text)


# -- Tests: dimension constraints ----------------------------------------------


@pytest.mark.parametrize("frame", _FRAMES)
def test_within_width(frame: int):
    """Bird sprite fits within the maximum character width."""
    _force_tier("unicode_full")
    text = render_fox(frame=frame)
    lines = text.plain.split("\n")
    for i, line in enumerate(lines):
        assert len(line) <= _MAX_WIDTH, (
            f"Frame {frame} line {i} is {len(line)} chars wide "
            f"(max {_MAX_WIDTH}): {line!r}"
        )


@pytest.mark.parametrize("frame", _FRAMES)
def test_within_height(frame: int):
    """Bird sprite fits within the maximum line count."""
    _force_tier("unicode_full")
    text = render_fox(frame=frame)
    lines = text.plain.split("\n")
    assert len(lines) <= _MAX_LINES, (
        f"Frame {frame} has {len(lines)} lines (max {_MAX_LINES})"
    )


@pytest.mark.parametrize("frame", _FRAMES)
def test_ascii_within_width(frame: int):
    """ASCII bird sprite fits within the maximum character width."""
    _force_tier("ascii_only")
    text = render_fox(frame=frame)
    lines = text.plain.split("\n")
    for i, line in enumerate(lines):
        assert len(line) <= _MAX_WIDTH, (
            f"ASCII frame {frame} line {i} is {len(line)} chars wide: {line!r}"
        )


@pytest.mark.parametrize("frame", _FRAMES)
def test_ascii_within_height(frame: int):
    """ASCII bird sprite fits within the maximum line count."""
    _force_tier("ascii_only")
    text = render_fox(frame=frame)
    lines = text.plain.split("\n")
    assert len(lines) <= _MAX_LINES, (
        f"ASCII frame {frame} has {len(lines)} lines (max {_MAX_LINES})"
    )


# -- Tests: ASCII fallback uses only printable ASCII ---------------------------


@pytest.mark.parametrize("frame", _FRAMES)
def test_ascii_fallback_printable_only(frame: int):
    """ASCII fallback contains only printable ASCII characters (0x20-0x7E + newline)."""
    _force_tier("ascii_only")
    text = render_fox(frame=frame)
    plain = text.plain
    for ch in plain:
        if ch == "\n":
            continue
        assert 0x20 <= ord(ch) <= 0x7E, (
            f"Non-printable-ASCII char {ch!r} (ord {ord(ch)}) in frame {frame}"
        )


# -- Tests: sprite contains recognizable brand-mark characters -----------------


@pytest.mark.parametrize("frame", _FRAMES)
def test_sprite_has_brand_mark_chars(frame: int):
    """Sprite contains the Seeknal brand-mark glyphs (framed graph)."""
    _force_tier("unicode_full")
    text = render_fox(frame=frame)
    plain = text.plain
    # Rounded-frame border + at least one node glyph must be present.
    assert "╭" in plain and "╮" in plain and "╰" in plain and "╯" in plain, (
        f"Frame {frame} missing rounded-frame border"
    )
    node_chars = set("●◐◉")
    has_node = any(ch in node_chars for ch in plain)
    assert has_node, f"Frame {frame} missing brand-mark node glyphs"


# -- Tests: eye placeholder resolved -------------------------------------------


@pytest.mark.parametrize("frame", _FRAMES)
@pytest.mark.parametrize("eyes", list(EYES.keys()))
def test_eye_placeholder_resolved(frame: int, eyes: str):
    """No {E} placeholder remains in rendered text."""
    _force_tier("unicode_full")
    text = render_fox(frame=frame, eyes=eyes)
    assert "{E}" not in text.plain, (
        f"Unresolved {{E}} in frame {frame}, eyes={eyes}"
    )


# -- Tests: frames differ (animation has visible deltas) -----------------------


def test_frames_differ():
    """The three animation frames produce distinct output (pulse cycles)."""
    _force_tier("unicode_full")
    plains = [render_fox(frame=f).plain for f in _FRAMES]
    # Frame 1 highlights the source node (◉ at top); frame 2 highlights the
    # destination node (◉ at bottom); frame 0 shows neither.
    unique = set(plains)
    assert len(unique) >= 2, (
        "Animation frames should produce distinct output"
    )


# -- Tests: eyes parameter is accepted for backward-compat --------------------


def test_eyes_param_backward_compat():
    """Eyes parameter is accepted but ignored (mark has no eyes)."""
    _force_tier("unicode_full")
    default_plain = render_fox(eyes="default").plain
    sparkle_plain = render_fox(eyes="sparkle").plain
    closed_plain = render_fox(eyes="closed").plain
    # All produce identical output — the graph mark has no eye region.
    assert default_plain == sparkle_plain == closed_plain


# -- Tests: idle sequence valid ------------------------------------------------


def test_idle_sequence_valid():
    """All idle sequence indices are valid frame indices (0..2)."""
    for step in IDLE_SEQUENCE:
        assert 0 <= step < 3, (
            f"Invalid idle sequence step: {step}"
        )


# -- Tests: animated generator yields frames -----------------------------------


def test_animated_generator_yields():
    """render_animated_fox() yields Text objects."""
    _force_tier("unicode_full")
    gen = render_animated_fox()
    frames = [next(gen) for _ in range(5)]
    assert all(isinstance(f, Text) for f in frames)


# -- Tests: backward compatibility (pose param ignored) ------------------------


def test_pose_param_backward_compat():
    """Pose parameter is accepted but ignored (all poses produce same output)."""
    _force_tier("unicode_full")
    default = render_fox("default").plain
    left = render_fox("look_left").plain
    right = render_fox("look_right").plain
    assert default == left == right
