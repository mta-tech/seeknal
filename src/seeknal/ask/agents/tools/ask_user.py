"""Interactive ask_user callback for pydantic-deep's plan mode.

Renders the agent's clarifying question + options as an arrow-key
navigable terminal menu using Rich Live + tty/termios. Falls back to
numbered input() when not in a TTY.

Wired into DeepAgentDeps.ask_user so the planner subagent's ask_user
tool triggers this UI.

This callback is async because pydantic-deep's plan toolset calls
``await callback(question, options)`` (toolset.py:189). The blocking
terminal I/O runs in a worker thread via ``asyncio.to_thread()``.
"""

from __future__ import annotations

import asyncio
import sys
from typing import Any


async def interactive_ask_user(question: str, options: list[dict[str, Any]]) -> str:
    """Present a question with selectable options in the terminal.

    Parameters
    ----------
    question:
        The question string from the agent.
    options:
        List of dicts with ``"label"`` and ``"description"`` keys.
        One may have ``"recommended": "true"``.
        If any has ``"multi_select": "true"``, multi-select mode is enabled.

    Returns
    -------
    The selected option's ``"label"`` string, free-text from the user,
    or comma-separated labels for multi-select.
    """
    if not options:
        return ""

    # Detect multi-select mode from options
    multi_select = any(
        str(opt.get("multi_select", "")).lower() == "true" for opt in options
    )

    if sys.stdout.isatty():
        return await asyncio.to_thread(
            _blocking_menu, question, options, multi_select
        )

    return await asyncio.to_thread(
        _fallback_numbered, question, options
    )


def _blocking_menu(
    question: str,
    options: list[dict[str, Any]],
    multi_select: bool = False,
) -> str:
    """Run the interactive menu in a blocking thread."""
    from seeknal.ui.console import get_console
    from seeknal.ui.interactive_menu import InteractiveMenu

    console = get_console()
    menu = InteractiveMenu(
        question=question,
        options=options,
        console=console,
        multi_select=multi_select,
    )
    return menu.run()


def _fallback_numbered(question: str, options: list[dict[str, Any]]) -> str:
    """Non-TTY fallback: numbered list with input() selection."""
    from seeknal.ui.console import get_console

    console = get_console()

    # Build display labels
    labels: list[str] = []
    for opt in options:
        label = opt.get("label", "")
        desc = opt.get("description", "")
        rec = " [recommended]" if str(opt.get("recommended", "")).lower() == "true" else ""
        labels.append(f"{label}{rec} \u2014 {desc}" if desc else f"{label}{rec}")

    # Always append free-text option
    labels.append("Other (type your own)")

    console.print()
    console.print(f"[brand.primary]{question}[/]")
    console.print()

    for i, label in enumerate(labels, 1):
        console.print(f"  [text.dim]{i}.[/] {label}")
    console.print()

    try:
        choice = input(f"Select [1-{len(labels)}]: ").strip()
        idx = int(choice) - 1
        if 0 <= idx < len(labels):
            if idx == len(labels) - 1:
                # "Other" selected — get free text
                answer = input("  Your answer: ").strip()
                return answer if answer else options[0].get("label", "")
            return options[idx].get("label", labels[idx])
    except (ValueError, EOFError, KeyboardInterrupt):
        pass

    # Default: first option
    return options[0].get("label", "") if options else ""
