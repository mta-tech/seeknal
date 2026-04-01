"""
Seeknal welcome banner.

Renders a branded welcome screen with the fox mascot on the left
and welcome text on the right, using ``rich.table.Table.grid()``
for side-by-side layout.

The banner is shown at most once per session (tracked via module-level
flag). Non-TTY environments skip it entirely.
"""

from __future__ import annotations

import os

from rich.table import Table
from rich.text import Text

from seeknal.ui.console import get_console, is_animation_enabled
from seeknal.ui.figures import get
from seeknal.ui.fox import render_fox

# Module-level flag to track whether the banner has been shown.
_shown = False


def show_welcome(version: str = "") -> None:
    """Show the branded welcome banner (once per session).

    Parameters
    ----------
    version:
        Version string to display. If empty, attempts to read from
        package metadata.
    """
    global _shown
    if _shown:
        return

    console = get_console()
    if not console.is_terminal:
        return

    _shown = True

    # Resolve version if not provided.
    ver = version
    if not ver:
        try:
            from importlib.metadata import version as pkg_version

            ver = pkg_version("seeknal")
        except Exception:
            ver = ""

    # Build the fox mascot renderable.
    fox = render_fox("default")

    # When animations are disabled, show a compact static banner.
    if not is_animation_enabled():
        ver_suffix = f" v{ver}" if ver else ""
        console.print(f"Welcome to Seeknal{ver_suffix}")
        return

    # Build the right-hand text column.
    star_char = get("star")
    sep_char = get("separator")

    right = Text()
    right.append(f" {star_char} ", style="brand.primary")
    right.append("Welcome to Seeknal", style="welcome.title")
    right.append("\n")
    if ver:
        right.append(f"    v{ver}", style="welcome.version")
        right.append("\n")
    right.append(f"    {sep_char * 24}", style="welcome.separator")

    # Layout: fox | text, side by side.
    grid = Table.grid(padding=(0, 2))
    grid.add_column(justify="left")  # fox
    grid.add_column(justify="left")  # text
    grid.add_row(fox, right)

    console.print()
    console.print(grid)
    console.print()


def reset_welcome() -> None:
    """Reset the shown flag so the banner can be displayed again.

    Intended for testing.
    """
    global _shown
    _shown = False
