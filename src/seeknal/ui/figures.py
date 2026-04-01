"""
Platform-aware Unicode symbols for seeknal CLI output.

Provides three tiers of glyphs:
  - unicode_full: macOS and modern GPU-accelerated terminals
  - unicode_safe: broad cross-platform subset
  - ascii_only:   TERM=dumb and legacy terminals
"""

import os
import sys
from typing import Dict


def _detect_glyph_tier() -> str:
    """Detect which glyph tier to use based on platform and terminal."""
    term = os.environ.get("TERM", "")
    term_program = os.environ.get("TERM_PROGRAM", "")

    # Dumb terminal or non-interactive
    if term == "dumb" or not sys.stdout.isatty():
        return "ascii_only"

    # Known GPU-accelerated / modern terminals (cross-platform)
    modern_terminals = {"ghostty", "WezTerm", "kitty", "alacritty"}
    if term_program in modern_terminals:
        return "unicode_full"

    # Windows Terminal (modern)
    if os.environ.get("WT_SESSION"):
        return "unicode_full"

    # macOS: Terminal.app has Unicode rendering quirks (emoji widths,
    # some combining characters); give it the safe subset.  Other macOS
    # terminals (iTerm2, Ghostty, etc.) are already caught above.
    if sys.platform == "darwin":
        if term_program == "Apple_Terminal":
            return "unicode_safe"
        return "unicode_full"

    # Default to safe subset
    return "unicode_safe"


# =============================================================================
# Glyph Sets
# =============================================================================

_UNICODE_FULL: Dict[str, str] = {
    # Status indicators
    "success": "✓",
    "error": "✗",
    "warning": "⚠",
    "info": "ℹ",
    # Progress / effort
    "effort_low": "○",
    "effort_medium": "◐",
    "effort_high": "●",
    "effort_max": "◉",
    # Arrows
    "arrow_right": "→",
    "arrow_left": "←",
    "arrow_up": "↑",
    "arrow_down": "↓",
    # Spinners (brand set — forward + reverse for bi-directional)
    "spinner_frames": ["·", "✢", "✳", "✶", "✻", "✽", "✻", "✶", "✳", "✢"],
    # Decorative
    "separator": "─",
    "separator_heavy": "━",
    "bullet": "•",
    "diamond": "◆",
    "diamond_open": "◇",
    "star": "✻",
    "ellipsis": "…",
    "blockquote": "▎",
    # Blocks (for mascot / visual art)
    "block_full": "█",
    "block_upper": "▀",
    "block_lower": "▄",
    "block_left": "▌",
    "block_right": "▐",
    "shade_light": "░",
    "shade_medium": "▒",
    "shade_dark": "▓",
    # Play / media
    "play": "▶",
    "pause": "⏸",
    # Lightning
    "lightning": "↯",
}

_UNICODE_SAFE: Dict[str, str] = {
    **_UNICODE_FULL,
    # Override characters that may not render on all Linux terminals
    "spinner_frames": ["·", "*", "✶", "✻", "✶", "*"],
    "star": "*",
    "pause": "||",
}

_ASCII_ONLY: Dict[str, str] = {
    "success": "[OK]",
    "error": "[ERR]",
    "warning": "[WARN]",
    "info": "[i]",
    "effort_low": "o",
    "effort_medium": "O",
    "effort_high": "@",
    "effort_max": "#",
    "arrow_right": "->",
    "arrow_left": "<-",
    "arrow_up": "^",
    "arrow_down": "v",
    "spinner_frames": ["-", "\\", "|", "/"],
    "separator": "-",
    "separator_heavy": "=",
    "bullet": "*",
    "diamond": "*",
    "diamond_open": "o",
    "star": "*",
    "ellipsis": "...",
    "blockquote": "|",
    "block_full": "#",
    "block_upper": "^",
    "block_lower": "_",
    "block_left": "|",
    "block_right": "|",
    "shade_light": ".",
    "shade_medium": ":",
    "shade_dark": "#",
    "play": ">",
    "pause": "||",
    "lightning": "!",
}

_GLYPH_SETS = {
    "unicode_full": _UNICODE_FULL,
    "unicode_safe": _UNICODE_SAFE,
    "ascii_only": _ASCII_ONLY,
}

# =============================================================================
# Public API
# =============================================================================

_current_tier: str | None = None
_current_glyphs: Dict[str, str] | None = None


def _ensure_loaded() -> Dict[str, str]:
    global _current_tier, _current_glyphs
    if _current_glyphs is None:
        _current_tier = _detect_glyph_tier()
        _current_glyphs = _GLYPH_SETS[_current_tier]
    return _current_glyphs


def get(name: str) -> str:
    """Get a glyph by name, using the auto-detected tier."""
    glyphs = _ensure_loaded()
    return glyphs.get(name, "?")


def get_spinner_frames() -> list[str]:
    """Get spinner frames for the current platform."""
    glyphs = _ensure_loaded()
    return glyphs["spinner_frames"]


def get_tier() -> str:
    """Return the current glyph tier name."""
    _ensure_loaded()
    return _current_tier


def reset():
    """Reset detection (for testing)."""
    global _current_tier, _current_glyphs
    _current_tier = None
    _current_glyphs = None
