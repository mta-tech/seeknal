"""
Seeknal Console singleton.

Provides a shared Rich Console instance with the correct theme applied.
All modules should import from here rather than creating their own Console.

Theme selection priority:
  1. SEEKNAL_THEME env var
  2. config.toml [ui] theme setting
  3. Auto-detect from terminal capabilities
"""

import os
import sys
from typing import Optional

from rich.console import Console
from rich.theme import Theme

from seeknal.ui.theme import (
    DARK_THEME,
    LIGHT_THEME,
    ANSI_THEME,
    THEMES,
    DEFAULT_THEME_NAME,
)

# =============================================================================
# Animation / reduced motion
# =============================================================================

_animation_disabled = False


def disable_animation():
    """Disable all animations (called by --no-animation flag)."""
    global _animation_disabled
    _animation_disabled = True


def is_animation_enabled() -> bool:
    """Check whether animations should run."""
    if _animation_disabled:
        return False
    if os.environ.get("NO_COLOR"):
        return False
    if os.environ.get("CI"):
        return False
    if os.environ.get("TERM") == "dumb":
        return False
    if not sys.stdout.isatty():
        return False
    return True


# =============================================================================
# Theme resolution
# =============================================================================


def _resolve_theme_name() -> str:
    """Determine which theme to use based on env / config / auto-detect."""
    # 1. Environment variable
    env_theme = os.environ.get("SEEKNAL_THEME", "").strip().lower()
    if env_theme and env_theme in THEMES:
        return env_theme

    # 2. config.toml (best-effort — don't crash if config unavailable)
    try:
        from seeknal.configuration import DotDict
        import toml
        from pathlib import Path

        config_path = Path("config.toml")
        if config_path.exists():
            cfg = toml.load(config_path)
            theme_name = cfg.get("ui", {}).get("theme", "").strip().lower()
            if theme_name and theme_name in THEMES:
                return theme_name
    except Exception:
        pass

    # 3. Auto-detect based on terminal capabilities
    return "auto"


def _resolve_theme() -> Theme:
    """Return the correct Rich Theme for the current environment."""
    name = _resolve_theme_name()

    if name != "auto":
        return THEMES[name]

    # Auto mode: pick based on terminal color support
    # We create a temporary console to probe capabilities
    probe = Console()
    color_system = probe.color_system

    if color_system in ("truecolor", "256"):
        return DARK_THEME
    elif color_system == "standard":
        return ANSI_THEME
    else:
        # No color support — return ANSI theme (Rich will strip colors)
        return ANSI_THEME


# =============================================================================
# Console singleton
# =============================================================================

_console: Optional[Console] = None
_err_console: Optional[Console] = None


def get_console() -> Console:
    """Return the shared seeknal Console (stdout)."""
    global _console
    if _console is None:
        theme = _resolve_theme()
        _console = Console(theme=theme, highlight=False)
    return _console


def get_err_console() -> Console:
    """Return the shared seeknal Console (stderr)."""
    global _err_console
    if _err_console is None:
        theme = _resolve_theme()
        _err_console = Console(theme=theme, stderr=True, highlight=False)
    return _err_console


def reset():
    """Reset console singletons (for testing)."""
    global _console, _err_console, _animation_disabled
    _console = None
    _err_console = None
    _animation_disabled = False
