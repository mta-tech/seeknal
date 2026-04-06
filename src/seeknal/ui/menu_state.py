"""Data model for the interactive menu.

Pure dataclasses with no side effects — all state transitions
are explicit mutations on the dataclass fields.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class OptionItem:
    """A single selectable option in the menu."""

    label: str
    description: str = ""
    preview: str | None = None
    recommended: bool = False


@dataclass
class MenuState:
    """State for a single interactive question.

    Tracks cursor position, selected items (for multi-select),
    text input buffer (for "Other"), and mode flags.
    """

    question: str
    options: list[OptionItem]
    cursor: int = 0
    selected: set[int] = field(default_factory=set)
    multi_select: bool = False
    text_input: str = ""
    in_text_mode: bool = False

    @staticmethod
    def from_options(
        question: str,
        options: list[dict[str, Any]],
        multi_select: bool = False,
    ) -> MenuState:
        """Build a MenuState from pydantic-deep's option dicts.

        Each dict should have ``"label"`` and optionally ``"description"``,
        ``"preview"``, and ``"recommended"`` keys. An "Other (type your own)"
        option is always appended.
        """
        items: list[OptionItem] = []
        initial_cursor = 0

        for i, opt in enumerate(options):
            item = OptionItem(
                label=opt.get("label", ""),
                description=opt.get("description", ""),
                preview=opt.get("preview"),
                recommended=str(opt.get("recommended", "")).lower() == "true",
            )
            if item.recommended and initial_cursor == 0:
                initial_cursor = i
            items.append(item)

        # Always append the free-text "Other" option
        items.append(OptionItem(label="Other", description="Type your own"))

        return MenuState(
            question=question,
            options=items,
            cursor=initial_cursor,
            multi_select=multi_select,
        )

    def get_answer(self) -> str:
        """Extract the answer string from the current state.

        For single-select: returns the focused option's label (or text input).
        For multi-select: returns comma-separated labels of selected options.
        """
        if self.multi_select:
            if not self.selected:
                # Nothing selected — return focused option's label
                return self.options[self.cursor].label
            labels = [self.options[i].label for i in sorted(self.selected)]
            # If "Other" is selected and text was typed, replace "Other" with the text
            other_idx = len(self.options) - 1
            if other_idx in self.selected and self.text_input.strip():
                labels = [
                    self.text_input.strip() if i == other_idx else self.options[i].label
                    for i in sorted(self.selected)
                ]
            return ", ".join(labels)

        # Single-select
        other_idx = len(self.options) - 1
        if self.cursor == other_idx and self.text_input.strip():
            return self.text_input.strip()

        return self.options[self.cursor].label

    @property
    def has_preview(self) -> bool:
        """Whether any option has preview content."""
        return any(opt.preview for opt in self.options)
