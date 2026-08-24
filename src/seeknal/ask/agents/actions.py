"""Typed committing actions for the opt-in IBA action-delivery path.

The IBA premises worker translates these payloads into DF-native browser events.
Seeknal intentionally does not render or execute them here.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field
from pydantic_ai import ToolOutput


class VisualizeAction(BaseModel):
    """DF core ``visualize`` action payload."""

    title: str
    code: str
    output_variable: str
    chart: dict[str, Any]
    subtitle: str = ""
    display_instruction: str = ""
    input_tables: list[str] = Field(default_factory=list)
    field_metadata: dict[str, Any] = Field(default_factory=dict)
    field_display_names: dict[str, str] = Field(default_factory=dict)


class AskUserQuestion(BaseModel):
    """One DF ``ask_user`` question payload."""

    text: str
    responseType: Literal["single_choice", "free_text"] | None = None
    required: bool | None = None
    options: list[str] | None = None


class AskUserAction(BaseModel):
    """DF core ``ask_user`` action payload and turn boundary."""

    questions: list[AskUserQuestion]
    thought: str = ""


def action_output_types() -> list[ToolOutput[Any]]:
    """Return the core action output tools offered to an enabled agent."""
    return [
        ToolOutput(VisualizeAction, name="visualize"),
        ToolOutput(AskUserAction, name="ask_user"),
    ]
