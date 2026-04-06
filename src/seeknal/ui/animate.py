"""
Animated sprite display for seeknal headers.

Uses Rich ``Live`` to render an animated sprite beside static text,
matching Claude Code's companion animation timing.
"""

from __future__ import annotations

import time
from typing import Iterator

from rich.table import Table
from rich.text import Text

from seeknal.ui.console import get_console, is_animation_enabled
from seeknal.ui.fox import TICK_MS, render_fox


def animated_header(
    sprite_gen: Iterator[Text],
    right_content: Text,
    duration_ticks: int = 15,
) -> None:
    """Show an animated sprite beside static text, then settle to static.

    Parameters
    ----------
    sprite_gen:
        Iterator yielding one ``Text`` renderable per animation tick
        (typically from ``render_animated_fox()``).
    right_content:
        Static text to display beside the sprite.
    duration_ticks:
        Number of ticks to animate before settling. Default 15 = one
        full idle cycle (~7.5s at 500ms per tick).
    """
    console = get_console()

    if not console.is_terminal or not is_animation_enabled():
        # Static fallback — render frame 0 beside text
        static_sprite = render_fox(frame=0)
        grid = Table.grid(padding=(0, 2))
        grid.add_column(justify="left")
        grid.add_column(justify="left")
        grid.add_row(static_sprite, right_content)
        console.print()
        console.print(grid)
        console.print()
        return

    from rich.live import Live

    tick_s = TICK_MS / 1000.0

    def _build_grid(sprite: Text) -> Table:
        grid = Table.grid(padding=(0, 2))
        grid.add_column(justify="left")
        grid.add_column(justify="left")
        grid.add_row(sprite, right_content)
        return grid

    console.print()
    with Live(
        _build_grid(next(sprite_gen)),
        console=console,
        refresh_per_second=1000 / TICK_MS,
        transient=True,
    ) as live:
        for i, frame in enumerate(sprite_gen):
            if i >= duration_ticks - 1:
                break
            time.sleep(tick_s)
            live.update(_build_grid(frame))

    # Settle: print the final static frame (frame 0) so it persists
    static_sprite = render_fox(frame=0)
    grid = _build_grid(static_sprite)
    console.print(grid)
    console.print()
