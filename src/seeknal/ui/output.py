"""
Rich-based echo functions for seeknal CLI output.

Drop-in replacements for the old typer-based _echo_* helpers.
Uses the shared Console and theme from seeknal.ui.
"""

from seeknal.ui.console import get_console
from seeknal.ui.figures import get


def echo_success(message: str) -> None:
    """Print a success message with themed styling."""
    symbol = get("success")
    get_console().print(f"[status.success]{symbol} {message}[/]")


def echo_error(message: str) -> None:
    """Print an error message with themed styling."""
    symbol = get("error")
    get_console().print(f"[status.error]{symbol} {message}[/]")


def echo_warning(message: str) -> None:
    """Print a warning message with themed styling."""
    symbol = get("warning")
    get_console().print(f"[status.warning]{symbol} {message}[/]")


def echo_info(message: str) -> None:
    """Print an info message with themed styling."""
    symbol = get("info")
    get_console().print(f"[status.info]{symbol} {message}[/]")
