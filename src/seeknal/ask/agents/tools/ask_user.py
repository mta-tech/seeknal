"""ask_user tool — interactive question with selectable options.

Allows the agent to pause mid-execution, present a question with
arrow-key navigable options, and return the user's selection as the
tool result. In non-interactive mode (one-shot, exposure, quiet),
returns a skip sentinel immediately.

Uses raw /dev/tty terminal I/O for arrow-key selection — no signal
handling, works from any thread (including LangGraph executor threads).
"""

from __future__ import annotations

import os
import sys

from langchain_core.tools import tool

_SKIP_SENTINEL = "(skipped — no interactive input available)"
_BUDGET_EXHAUSTED = "(question budget exceeded — proceeding with best-effort assumptions)"
_TYPE_YOUR_OWN = "Type your own..."
_C_DIM = "dim"


def _render_question(console, question: str) -> None:
    """Print the question text to the Rich console."""
    from rich.markup import escape

    console.print(f"\n  [bold]{escape(question)}[/bold]\n")


def _select_with_arrow_keys(display_options: list[str]) -> int | None:
    """Arrow-key menu via raw /dev/tty. Works from any thread (no signals).

    Opens /dev/tty directly for both input and output, uses termios for
    raw mode. Renders a cursor-navigable menu. Returns selected index
    or None on abort (Escape/Ctrl-C).
    """
    import termios
    import tty

    tty_in = open("/dev/tty", "r")  # noqa: SIM115
    tty_out = open("/dev/tty", "w")  # noqa: SIM115
    fd = tty_in.fileno()
    old_settings = termios.tcgetattr(fd)

    current = 0
    n = len(display_options)

    def _write(text: str) -> None:
        tty_out.write(text)
        tty_out.flush()

    def _render(first: bool = False) -> None:
        if not first:
            _write(f"\033[{n}A")  # Move cursor up n lines to redraw
        for i, opt in enumerate(display_options):
            _write("\033[2K")  # Clear line
            if i == current:
                _write(f"  \033[32;1m❯ {opt}\033[0m\n")
            else:
                _write(f"    {opt}\n")

    _render(first=True)

    try:
        tty.setraw(fd)
        while True:
            ch = tty_in.read(1)
            if ch in ("\r", "\n"):  # Enter
                # Clear the menu and show selection
                _write(f"\033[{n}A")  # Move up
                for _ in range(n):
                    _write("\033[2K\n")  # Clear each line
                _write(f"\033[{n}A")  # Move back up
                return current
            elif ch == "\x1b":  # Escape sequence
                ch2 = tty_in.read(1)
                if ch2 == "[":
                    ch3 = tty_in.read(1)
                    if ch3 == "A":  # Up arrow
                        current = (current - 1) % n
                    elif ch3 == "B":  # Down arrow
                        current = (current + 1) % n
                else:
                    # Bare Escape = abort
                    return None
                # Redraw in cooked mode temporarily for clean output
                termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
                _render()
                tty.setraw(fd)
            elif ch == "\x03":  # Ctrl-C
                return None
            elif ch == "q":
                return None
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
        tty_in.close()
        tty_out.close()


def _select_with_numbered_input(console, display_options: list[str]) -> int | None:
    """Fallback numbered-list selection for non-TTY environments."""
    from rich.markup import escape

    for i, opt in enumerate(display_options, 1):
        console.print(f"  [{i}] {escape(opt)}")
    console.print()
    try:
        raw = input("  ▸ ").strip()
    except (EOFError, KeyboardInterrupt):
        return None
    if not raw:
        return None
    try:
        idx = int(raw) - 1
        if 0 <= idx < len(display_options):
            return idx
    except ValueError:
        pass
    return None


@tool
def ask_user(question: str, options: list[str]) -> str:
    """Ask the user a question with selectable options.

    Present a single question with 2-4 options. The user selects one
    option or types a custom answer. Use this when you need clarification
    on entity keys, time columns, pipeline type, or any decision where
    multiple plausible choices exist.

    The options list should contain concrete choices populated from
    schema data when possible. Place your recommended option first.

    Args:
        question: The question to ask (e.g., "Which column is the entity key?").
        options: List of 2-4 option strings (e.g., ["customer_id", "user_id"]).
    """
    from seeknal.ask.agents.tools._context import get_tool_context

    ctx = get_tool_context()

    # Guard 1: non-interactive mode (one-shot, exposure, quiet)
    if not ctx.interactive or ctx.exposure_mode:
        return _SKIP_SENTINEL

    # Guard 2: question budget exhausted
    if ctx.questions_remaining <= 0:
        return _BUDGET_EXHAUSTED

    # Decrement budget
    ctx.questions_remaining -= 1

    # Build display options with "Type your own" appended
    display_options = list(options) + [_TYPE_YOUR_OWN]

    # Render question
    console = ctx.console
    if console:
        _render_question(console, question)

    # Arrow-key menu via /dev/tty (works from any thread, no signals).
    # Falls back to numbered input if /dev/tty unavailable (CI, containers).
    try:
        selected_idx = _select_with_arrow_keys(display_options)
    except Exception:
        selected_idx = _select_with_numbered_input(console, display_options)

    if selected_idx is None:
        return _SKIP_SENTINEL

    selected = display_options[selected_idx]

    # Handle "Type your own" selection
    if selected == _TYPE_YOUR_OWN:
        if console:
            console.print()
        try:
            custom = input("  ▸ ").strip()
        except (EOFError, KeyboardInterrupt):
            return _SKIP_SENTINEL
        return custom if custom else _SKIP_SENTINEL

    return selected
