"""
Rich Panel factories for seeknal CLI output.

Provides branded panel helpers for info, status, summary, and code display.
"""

from typing import Optional, Union

import rich.box
from rich.panel import Panel
from rich.syntax import Syntax
from rich.text import Text


def info_panel(
    content: Union[str, Text],
    title: Optional[str] = None,
) -> Panel:
    """Panel with the default panel border style."""
    return Panel(
        content,
        title=title,
        border_style="panel.border",
        box=rich.box.ROUNDED,
    )


def success_panel(
    content: Union[str, Text],
    title: Optional[str] = None,
) -> Panel:
    """Panel with success border styling."""
    return Panel(
        content,
        title=title,
        border_style="status.success",
        box=rich.box.ROUNDED,
    )


def error_panel(
    content: Union[str, Text],
    title: Optional[str] = None,
) -> Panel:
    """Panel with error border styling."""
    return Panel(
        content,
        title=title,
        border_style="status.error",
        box=rich.box.ROUNDED,
    )


def warning_panel(
    content: Union[str, Text],
    title: Optional[str] = None,
) -> Panel:
    """Panel with warning border styling."""
    return Panel(
        content,
        title=title,
        border_style="status.warning",
        box=rich.box.ROUNDED,
    )


def summary_panel(
    content: Union[str, Text],
    title: str = "Summary",
) -> Panel:
    """Panel with brand primary border styling."""
    return Panel(
        content,
        title=title,
        border_style="brand.primary",
        box=rich.box.ROUNDED,
    )


def code_panel(
    code: str,
    language: str = "sql",
    title: Optional[str] = None,
) -> Panel:
    """Panel wrapping a syntax-highlighted code block.

    Args:
        code: Source code string.
        language: Language for syntax highlighting (default ``"sql"``).
        title: Optional panel title.
    """
    syntax = Syntax(code, language, theme="monokai", word_wrap=True)
    return Panel(
        syntax,
        title=title,
        border_style="panel.border",
        box=rich.box.ROUNDED,
    )
