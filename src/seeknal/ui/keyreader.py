"""Raw terminal keypress reader using tty/termios.

Provides a context manager that puts the terminal into cbreak mode
and reads individual keypresses, parsing multi-byte escape sequences
into named key strings (e.g., "up", "down", "enter").

Used by InteractiveMenu for arrow-key navigation.
"""

from __future__ import annotations

import sys
import termios
import tty


class KeyReader:
    """Context manager for reading raw keypresses from the terminal.

    Uses ``tty.setcbreak()`` (not ``setraw``) so Ctrl-C still sends SIGINT.
    Always restores terminal settings on exit, even on exception.
    """

    def __init__(self) -> None:
        self._fd: int = -1
        self._old_settings: list | None = None

    def __enter__(self) -> KeyReader:
        self._fd = sys.stdin.fileno()
        self._old_settings = termios.tcgetattr(self._fd)
        tty.setcbreak(self._fd)
        return self

    def __exit__(self, *args: object) -> None:
        if self._old_settings is not None:
            termios.tcsetattr(self._fd, termios.TCSADRAIN, self._old_settings)

    def read_key(self) -> str:
        """Read a single keypress, handling multi-byte escape sequences.

        Returns one of:
            ``"up"``, ``"down"``, ``"left"``, ``"right"``,
            ``"enter"``, ``"space"``, ``"tab"``, ``"shift-tab"``,
            ``"escape"``, ``"backspace"``,
            or the literal character for printable keys.
        """
        ch = sys.stdin.read(1)

        if ch == "\x1b":
            # Start of escape sequence
            ch2 = sys.stdin.read(1)
            if ch2 == "[":
                ch3 = sys.stdin.read(1)
                return {
                    "A": "up",
                    "B": "down",
                    "C": "right",
                    "D": "left",
                    "Z": "shift-tab",
                }.get(ch3, f"esc-[{ch3}")
            return "escape"

        if ch == "\r" or ch == "\n":
            return "enter"
        if ch == " ":
            return "space"
        if ch == "\t":
            return "tab"
        if ch == "\x7f" or ch == "\x08":
            return "backspace"

        return ch
