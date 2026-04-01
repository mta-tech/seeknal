"""Seeknal UI — branded terminal output with Rich."""

from seeknal.ui.console import get_console, get_err_console, is_animation_enabled
from seeknal.ui.output import echo_success, echo_error, echo_warning, echo_info

__all__ = [
    "get_console",
    "get_err_console",
    "is_animation_enabled",
    "echo_success",
    "echo_error",
    "echo_warning",
    "echo_info",
]
