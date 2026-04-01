"""Tests for the seeknal fox mascot renderer."""

from __future__ import annotations

import pytest
from rich.text import Text

from seeknal.ui import figures
from seeknal.ui.fox import render_fox

# -- Helpers -------------------------------------------------------------------

_MAX_WIDTH = 22
_MAX_LINES = 8
_POSES = ["default", "look_left", "look_right"]


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


# -- Tests: all poses render without errors ------------------------------------


@pytest.mark.parametrize("pose", _POSES)
def test_render_returns_rich_text(pose: str):
    """render_fox() returns a Rich Text object for every pose."""
    _force_tier("unicode_full")
    result = render_fox(pose)
    assert isinstance(result, Text)


@pytest.mark.parametrize("pose", _POSES)
def test_render_ascii_returns_rich_text(pose: str):
    """render_fox() returns a Rich Text object in ASCII mode."""
    _force_tier("ascii_only")
    result = render_fox(pose)
    assert isinstance(result, Text)


# -- Tests: dimension constraints ----------------------------------------------


@pytest.mark.parametrize("pose", _POSES)
def test_unicode_fox_within_width(pose: str):
    """Unicode fox fits within the maximum character width."""
    _force_tier("unicode_full")
    text = render_fox(pose)
    lines = text.plain.split("\n")
    for i, line in enumerate(lines):
        assert len(line) <= _MAX_WIDTH, (
            f"Pose '{pose}' line {i} is {len(line)} chars wide "
            f"(max {_MAX_WIDTH}): {line!r}"
        )


@pytest.mark.parametrize("pose", _POSES)
def test_unicode_fox_within_height(pose: str):
    """Unicode fox fits within the maximum line count."""
    _force_tier("unicode_full")
    text = render_fox(pose)
    lines = text.plain.split("\n")
    assert len(lines) <= _MAX_LINES, (
        f"Pose '{pose}' has {len(lines)} lines (max {_MAX_LINES})"
    )


@pytest.mark.parametrize("pose", _POSES)
def test_ascii_fox_within_width(pose: str):
    """ASCII fox fits within the maximum character width."""
    _force_tier("ascii_only")
    text = render_fox(pose)
    lines = text.plain.split("\n")
    for i, line in enumerate(lines):
        assert len(line) <= _MAX_WIDTH, (
            f"ASCII pose '{pose}' line {i} is {len(line)} chars wide: {line!r}"
        )


@pytest.mark.parametrize("pose", _POSES)
def test_ascii_fox_within_height(pose: str):
    """ASCII fox fits within the maximum line count."""
    _force_tier("ascii_only")
    text = render_fox(pose)
    lines = text.plain.split("\n")
    assert len(lines) <= _MAX_LINES, (
        f"ASCII pose '{pose}' has {len(lines)} lines (max {_MAX_LINES})"
    )


# -- Tests: ASCII fallback uses only printable ASCII ---------------------------


@pytest.mark.parametrize("pose", _POSES)
def test_ascii_fallback_printable_only(pose: str):
    """ASCII fallback contains only printable ASCII characters (0x20-0x7E + newline)."""
    _force_tier("ascii_only")
    text = render_fox(pose)
    plain = text.plain
    for ch in plain:
        if ch == "\n":
            continue
        assert 0x20 <= ord(ch) <= 0x7E, (
            f"Non-printable-ASCII char {ch!r} (ord {ord(ch)}) in pose '{pose}'"
        )


# -- Tests: unicode poses have content ----------------------------------------


@pytest.mark.parametrize("pose", _POSES)
def test_unicode_fox_not_empty(pose: str):
    """Unicode fox contains visible block characters."""
    _force_tier("unicode_full")
    text = render_fox(pose)
    plain = text.plain
    block_chars = set("▀▄█░▒▓▌▐")
    has_blocks = any(ch in block_chars for ch in plain)
    assert has_blocks, f"Pose '{pose}' has no block characters"


# -- Tests: invalid pose falls back to default ---------------------------------


def test_invalid_pose_falls_back():
    """An unrecognized pose name falls back to the default pose."""
    _force_tier("unicode_full")
    default_text = render_fox("default")
    fallback_text = render_fox("nonexistent_pose")
    assert default_text.plain == fallback_text.plain


def test_invalid_pose_ascii_falls_back():
    """An unrecognized pose name falls back to default in ASCII mode."""
    _force_tier("ascii_only")
    default_text = render_fox("default")
    fallback_text = render_fox("nonexistent_pose")
    assert default_text.plain == fallback_text.plain


# -- Tests: poses are visually distinct ----------------------------------------


def test_poses_differ():
    """The three poses produce distinct output."""
    _force_tier("unicode_full")
    plains = {pose: render_fox(pose).plain for pose in _POSES}
    # At least the look_left and look_right should differ from default
    assert plains["look_left"] != plains["default"], (
        "look_left should differ from default"
    )
    assert plains["look_right"] != plains["default"], (
        "look_right should differ from default"
    )
    assert plains["look_left"] != plains["look_right"], (
        "look_left should differ from look_right"
    )
