"""Rich renderables for the interactive menu.

Produces themed panels with cursor indicators, selection marks,
help footer, and optional preview layout.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import rich.box
from rich.console import RenderableType
from rich.layout import Layout
from rich.markdown import Markdown
from rich.panel import Panel
from rich.text import Text

from seeknal.ui import figures

if TYPE_CHECKING:
    from seeknal.ui.menu_state import MenuState


def render_menu(state: MenuState, console_width: int = 80) -> RenderableType:
    """Render a menu question with options as a Rich Panel.

    If any option has preview content and the terminal is wide enough (>= 60),
    delegates to :func:`render_menu_with_preview` for side-by-side layout.
    """
    if state.has_preview and console_width >= 60:
        return render_menu_with_preview(state, console_width)

    content = _build_options_text(state)
    return Panel(
        content,
        title=state.question,
        title_align="left",
        border_style="brand.primary",
        box=rich.box.ROUNDED,
        padding=(1, 2),
    )


def render_menu_with_preview(state: MenuState, console_width: int = 80) -> RenderableType:
    """Render menu with side-by-side preview panel using Rich Layout."""
    options_text = _build_options_text(state, show_help=False)
    options_panel = Panel(
        options_text,
        title=state.question,
        title_align="left",
        border_style="brand.primary",
        box=rich.box.ROUNDED,
        padding=(1, 1),
    )

    # Get preview content for the focused option
    focused_opt = state.options[state.cursor]
    preview_content: RenderableType
    if focused_opt.preview:
        preview_content = Markdown(focused_opt.preview)
    else:
        preview_content = Text("No preview available", style="text.dim")

    preview_panel = Panel(
        preview_content,
        title="Preview",
        title_align="left",
        border_style="panel.border",
        box=rich.box.ROUNDED,
        padding=(1, 1),
    )

    layout = Layout()
    layout.split_row(
        Layout(options_panel, name="options", ratio=2),
        Layout(preview_panel, name="preview", ratio=3),
    )

    # Wrap in an outer panel with help footer
    help_text = _build_help_footer(state)
    outer = Text()
    outer.append_text(help_text)

    return Panel(
        layout,
        subtitle=str(help_text),
        subtitle_align="left",
        border_style="brand.primary",
        box=rich.box.ROUNDED,
    )


def _build_options_text(state: MenuState, show_help: bool = True) -> Text:
    """Build the options list as Rich Text with cursor and selection indicators."""
    diamond = figures.get("diamond")
    diamond_open = figures.get("diamond_open")
    text = Text()

    for i, opt in enumerate(state.options):
        is_focused = i == state.cursor
        is_selected = i in state.selected

        # Cursor indicator
        if is_focused:
            text.append("  \u203a ", style="brand.primary bold")
        else:
            text.append("    ")

        # Selection indicator
        if state.multi_select:
            if is_selected:
                text.append(f"{diamond} ", style="status.success")
            else:
                text.append(f"{diamond_open} ", style="text.dim")
        else:
            # Single-select: filled diamond for focused, open for others
            if is_focused:
                text.append(f"{diamond} ", style="brand.primary")
            else:
                text.append(f"{diamond_open} ", style="text.dim")

        # Label
        label_style = "brand.primary bold" if is_focused else ""
        text.append(opt.label, style=label_style)

        # Recommended badge
        if opt.recommended:
            text.append(" [recommended]", style="brand.accent")

        # Description
        if opt.description and opt.label != "Other":
            text.append(f" \u2014 {opt.description}", style="text.dim")
        elif opt.label == "Other":
            text.append(f" ({opt.description})", style="text.dim")

        text.append("\n")

    # Text input mode
    if state.in_text_mode:
        text.append("\n")
        text.append("    > ", style="brand.primary")
        text.append(state.text_input, style="brand.primary bold")
        text.append("\u2588", style="brand.primary")  # Block cursor
        text.append("\n")

    # Help footer
    if show_help:
        text.append("\n")
        text.append_text(_build_help_footer(state))

    return text


def _build_help_footer(state: MenuState) -> Text:
    """Build the help text showing available keybindings."""
    arrow_up = figures.get("arrow_up")
    arrow_down = figures.get("arrow_down")

    help_text = Text()
    help_text.append(f"  {arrow_up}{arrow_down} navigate", style="text.dim")

    if state.multi_select:
        help_text.append("  space toggle", style="text.dim")
    elif not state.in_text_mode:
        help_text.append("  space select", style="text.dim")

    if state.in_text_mode:
        help_text.append("  enter confirm  esc cancel", style="text.dim")
    else:
        help_text.append("  enter confirm", style="text.dim")

    return help_text
