"""
Rich Table factory for seeknal CLI output.

Provides branded table creation and printing helpers.
"""

from typing import List, Optional, Sequence

import rich.box
from rich.table import Table

from seeknal.ui.console import get_console


def make_table(
    headers: Sequence[str],
    rows: Sequence[Sequence[str]],
    title: Optional[str] = None,
    caption: Optional[str] = None,
) -> Table:
    """Create a Rich Table with branded styling.

    Args:
        headers: Column header labels.
        rows: Row data (each row is a sequence of cell strings).
        title: Optional table title.
        caption: Optional table caption.

    Returns:
        A configured ``rich.table.Table`` instance.
    """
    table = Table(
        title=title,
        caption=caption,
        header_style="table.header",
        border_style="table.border",
        box=rich.box.ROUNDED,
    )

    for header in headers:
        table.add_column(header)

    for row in rows:
        table.add_row(*[str(cell) for cell in row])

    return table


def print_table(
    headers: Sequence[str],
    rows: Sequence[Sequence[str]],
    **kwargs,
) -> None:
    """Create and print a Rich Table with branded styling.

    Accepts the same keyword arguments as :func:`make_table`.
    """
    table = make_table(headers, rows, **kwargs)
    get_console().print(table)
