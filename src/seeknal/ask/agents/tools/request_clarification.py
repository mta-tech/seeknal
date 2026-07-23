"""request_clarification — emit a structured clarification form to the user.

Headless/worker counterpart to the interactive ``ask_user`` menu. Instead of
blocking for an arrow-key picker, this tool records the clarification prompts
on the turn context; the gateway streaming layer emits them as an ``ask_user``
event and ends the turn (Model B). The user's answer arrives as the next chat
message and is bound from conversation history on the following turn.

Registered only in non-interactive environments (gateway/telegram). The
interactive CLI keeps the blocking ``ask_user`` menu, so the two tools are
never present in the same toolset.
"""

from __future__ import annotations

from typing import Any


async def request_clarification(
    prompts: list[dict[str, Any]],
) -> str:
    """Present a structured clarification form (one or more questions) to the user.

    Use this when the user's request is genuinely ambiguous and the different
    interpretations would produce materially different answers — for example a
    concept that could map to more than one database code, scope, or system.
    This tool emits the form and ends the turn; do not call further tools after
    it. The conversation resumes on the next message.

    Each slot is rendered as a tab. The frontend always offers a "type your
    own" free-text option per slot, so do not add a free-text option yourself.

    Args:
        prompts: 1-3 clarification slots. Each slot is a dict with:
            - 'question': The clarification question (clear and specific).
            - 'options': 2-3 answer options. Each option is a dict with:
                - 'label': Short option text (1-5 words)
                - 'description': What this option means or implies
                - 'recommended': Set to 'true' to highlight as recommended (optional)
    """
    from seeknal.ask.agents.tools._context import get_tool_context

    if not prompts:
        return "No clarification prompts provided."

    ctx = get_tool_context()
    ctx.pending_clarification = list(prompts)
    return (
        "Clarification form emitted to the user. Stop here and produce no "
        "further tool calls or text; the user's response will arrive as the "
        "next message and the conversation will resume from there."
    )
