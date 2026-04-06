"""Interactive terminal menu with arrow-key navigation.

Wires together :class:`KeyReader`, :class:`MenuState`, and the menu renderer
into a blocking ``run()`` method driven by Rich Live.

Replaces ``simple-term-menu`` with a custom implementation that supports
multi-select, preview panels, and themed visuals.
"""

from __future__ import annotations

import sys
from typing import Any

from rich.console import Console
from rich.live import Live

from seeknal.ui.keyreader import KeyReader
from seeknal.ui.menu_render import render_menu
from seeknal.ui.menu_state import MenuState


class InteractiveMenu:
    """Arrow-key navigable menu rendered via Rich Live.

    Parameters
    ----------
    question:
        The question text to display.
    options:
        List of option dicts (pydantic-deep format) with ``"label"``,
        ``"description"``, ``"preview"``, ``"recommended"`` keys.
    console:
        Rich Console instance to render on.
    multi_select:
        If True, allow toggling multiple options with space bar.
    """

    def __init__(
        self,
        question: str,
        options: list[dict[str, Any]],
        console: Console,
        multi_select: bool = False,
    ) -> None:
        self._console = console
        self._state = MenuState.from_options(question, options, multi_select=multi_select)
        self._done = False

    def run(self) -> str:
        """Run the interactive menu loop. Blocks until the user confirms.

        Returns the selected option label (or comma-separated labels for multi-select,
        or free text for "Other").
        """
        if not sys.stdout.isatty():
            return self._fallback_numbered()

        try:
            with KeyReader() as reader:
                with Live(
                    render_menu(self._state, self._console.width),
                    console=self._console,
                    auto_refresh=False,
                    transient=True,
                    redirect_stdout=False,
                    redirect_stderr=False,
                ) as live:
                    while not self._done:
                        key = reader.read_key()
                        self._handle_key(key)
                        if not self._done:
                            live.update(
                                render_menu(self._state, self._console.width),
                                refresh=True,
                            )
        except (KeyboardInterrupt, EOFError):
            # Cancel — return first/recommended option
            self._state.cursor = 0
            self._state.selected.clear()
            self._state.text_input = ""
            self._state.in_text_mode = False

        answer = self._state.get_answer()

        # Print compact summary after menu clears
        from rich.markup import escape

        self._console.print(f"  [brand.primary]\u203a {escape(answer)}[/]")

        return answer

    def _handle_key(self, key: str) -> None:
        """Process a single keypress and update menu state."""
        s = self._state

        if s.in_text_mode:
            self._handle_text_mode_key(key)
            return

        if key == "up":
            s.cursor = (s.cursor - 1) % len(s.options)
        elif key == "down":
            s.cursor = (s.cursor + 1) % len(s.options)
        elif key == "space":
            if s.multi_select:
                # Toggle selection
                if s.cursor in s.selected:
                    s.selected.discard(s.cursor)
                else:
                    s.selected.add(s.cursor)
            else:
                # Single-select: space acts like enter
                self._confirm()
        elif key == "enter":
            self._confirm()
        elif key == "escape":
            # Cancel — select first/recommended
            s.cursor = 0
            s.selected.clear()
            self._done = True

    def _handle_text_mode_key(self, key: str) -> None:
        """Handle keypresses while in text input mode."""
        s = self._state

        if key == "enter":
            # Confirm text input
            self._done = True
        elif key == "escape":
            # Exit text mode, return to option navigation
            s.in_text_mode = False
            s.text_input = ""
        elif key == "backspace":
            s.text_input = s.text_input[:-1]
        elif key == "space":
            s.text_input += " "
        elif len(key) == 1 and key.isprintable():
            s.text_input += key

    def _confirm(self) -> None:
        """Handle enter/confirm action."""
        s = self._state
        other_idx = len(s.options) - 1

        if s.cursor == other_idx and not s.in_text_mode:
            # Switch to text input mode for "Other"
            s.in_text_mode = True
        else:
            self._done = True

    def _fallback_numbered(self) -> str:
        """Non-TTY fallback: numbered list with input() selection."""
        s = self._state
        self._console.print()
        self._console.print(f"[brand.primary]{s.question}[/]")
        self._console.print()

        for i, opt in enumerate(s.options, 1):
            desc = f" \u2014 {opt.description}" if opt.description else ""
            rec = " [recommended]" if opt.recommended else ""
            self._console.print(f"  [text.dim]{i}.[/] {opt.label}{rec}{desc}")

        self._console.print()

        try:
            choice = input(f"Select [1-{len(s.options)}]: ").strip()
            idx = int(choice) - 1
            if 0 <= idx < len(s.options):
                other_idx = len(s.options) - 1
                if idx == other_idx:
                    answer = input("  Your answer: ").strip()
                    return answer if answer else s.options[0].label
                return s.options[idx].label
        except (ValueError, EOFError, KeyboardInterrupt):
            pass

        # Default: first option
        return s.options[0].label if s.options else ""
