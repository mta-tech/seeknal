"""
DAG execution progress display for seeknal.

Provides:
  - render_summary(): returns a Rich Panel with execution statistics
  - ExecutionProgress: live progress tracker for DAG node execution
"""

from __future__ import annotations

import sys
from typing import Optional

import rich.box
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table
from rich.text import Text

from seeknal.ui.console import get_console
from seeknal.ui.output import echo_info


def _format_duration(seconds: float) -> str:
    """Format seconds into a human-friendly duration string."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = int(seconds) // 60
    secs = seconds - minutes * 60
    return f"{minutes}m {secs:.1f}s"


def render_summary(stats: dict) -> Panel:
    """Return a Rich Panel displaying DAG execution statistics.

    Args:
        stats: Dict with optional keys: ``total``, ``executed``, ``cached``,
               ``failed``, ``duration``, ``log_path``.

    Returns:
        A ``rich.panel.Panel`` ready for printing.
    """
    total = stats.get("total", 0)
    executed = stats.get("executed", 0)
    cached = stats.get("cached", 0)
    failed = stats.get("failed", 0)
    duration = stats.get("duration", None)
    log_path = stats.get("log_path", None)

    grid = Table.grid(padding=(0, 2))
    grid.add_column(style="text.muted", justify="right")
    grid.add_column()

    grid.add_row("Total nodes", str(total))
    grid.add_row("Executed", str(executed))
    grid.add_row("Cached", str(cached))

    failed_style = "status.error" if failed > 0 else "status.success"
    failed_text = Text(str(failed), style=failed_style)
    grid.add_row("Failed", failed_text)

    if duration is not None:
        grid.add_row("Duration", _format_duration(duration))

    if log_path is not None:
        grid.add_row("Log", str(log_path))

    return Panel(
        grid,
        title="[panel.title]Execution Summary[/]",
        border_style="brand.primary",
        box=rich.box.ROUNDED,
    )


class ExecutionProgress:
    """Live progress tracker for DAG node execution.

    When the output is a TTY, uses Rich Progress bars.  Otherwise falls
    back to simple ``echo_info`` calls so CI logs remain readable.

    Usage::

        with ExecutionProgress(total_nodes=5) as progress:
            progress.start_node("transform_orders")
            ...
            progress.complete_node("transform_orders", rows=1000, duration=1.2)
    """

    def __init__(
        self,
        total_nodes: int,
        console: Optional[Console] = None,
    ) -> None:
        self._total = total_nodes
        self._console = console or get_console()
        self._is_tty = self._console.is_terminal
        self._progress: Optional[Progress] = None
        self._tasks: dict[str, object] = {}

    # -- context manager ------------------------------------------------------

    def __enter__(self) -> ExecutionProgress:
        if self._is_tty:
            self._progress = Progress(
                SpinnerColumn("seeknal"),
                TextColumn("{task.description}"),
                BarColumn(
                    complete_style="progress.bar.complete",
                    finished_style="status.success",
                ),
                TextColumn("{task.completed}/{task.total}"),
                TimeElapsedColumn(),
                console=self._console,
            )
            self._progress.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:  # noqa: ANN001
        if self._progress is not None:
            self._progress.__exit__(exc_type, exc_val, exc_tb)
            self._progress = None

    # -- public API -----------------------------------------------------------

    def start_node(self, name: str) -> None:
        """Register a node and mark it as in-progress."""
        if self._progress is not None:
            task_id = self._progress.add_task(name, total=1)
            self._tasks[name] = task_id
        else:
            echo_info(f"Running {name}")

    def complete_node(
        self,
        name: str,
        rows: int = 0,
        duration: float = 0,
    ) -> None:
        """Mark a node as successfully completed."""
        if self._progress is not None and name in self._tasks:
            self._progress.update(self._tasks[name], completed=1)
        elif self._progress is None:
            suffix = f" ({rows} rows, {_format_duration(duration)})" if rows else ""
            echo_info(f"Completed {name}{suffix}")

    def fail_node(self, name: str, error: str = "") -> None:
        """Mark a node as failed."""
        if self._progress is not None and name in self._tasks:
            task_id = self._tasks[name]
            self._progress.update(
                task_id,
                description=f"[status.error]{name}[/]",
                completed=1,
            )
        elif self._progress is None:
            msg = f"Failed {name}"
            if error:
                msg += f": {error}"
            echo_info(msg)
