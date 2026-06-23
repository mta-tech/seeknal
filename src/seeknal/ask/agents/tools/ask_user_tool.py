"""Ask user tool — presents interactive options for the user to choose from.

This is a direct tool in the seeknal-ask toolset. It allows the main agent
to present arrow-key navigable menus for clarifying questions, brainstorming
choices, and strategic decisions.
"""

from __future__ import annotations

from typing import Any


async def ask_user(
    question: str,
    options: list[dict[str, Any]],
) -> str:
    """Confirm user intent BEFORE running data queries. Use this tool instead of writing text.

    This is the REQUIRED mechanism when you need to clarify which interpretation to use
    before querying the database. Calling this tool is a BLOCKING call — it pauses
    execution, presents the options to the user, and RETURNS their choice to you.
    You then bind that choice and continue. Think of it like execute_sql: you call it
    when you need information you cannot determine yourself.

    WHEN TO USE: The user's question contains a concept (status, category, type, scale)
    that could map to multiple database codes, and you have not yet confirmed which one
    they intend. Call this tool BEFORE running any data SQL for that concept.

    DO NOT: Write a text clarification instead of calling this tool. Writing text is only
    permitted as a fallback when this tool is NOT available in your current toolset.

    Guidelines:
    - Provide 2-4 concrete options from the live data_dictionary lookup result
    - Mark your recommended option with recommended="true" (string), not boolean true
    - CALL THIS TOOL, then use its return value as the binding for subsequent SQL

    Args:
        question: The clarification question. Should be clear and specific.
        options: 2-4 answer options. Each option is a dict with:
            - 'label': Short option text (1-5 words)
            - 'description': What this option means or implies
            - 'recommended': Set to 'true' to highlight as recommended (optional)
    """
    from seeknal.ask.agents.tools._context import get_tool_context, record_ask_user_response
    from seeknal.ask.agents.tools.ask_user import interactive_ask_user

    if not options:
        return "No options provided."

    # Use gateway/custom callback when available (e.g. WebSocket in gateway mode);
    # fall back to the interactive terminal UI for TTY/interactive sessions.
    try:
        _cb = getattr(get_tool_context(), "ask_user_callback", None)
    except RuntimeError:
        _cb = None
    _invoke = _cb if _cb is not None else interactive_ask_user
    answer = await _invoke(question, options)
    record_ask_user_response(options, answer)

    # Clear pending ambiguity — the user responded, gate can lift.
    try:
        _ctx = get_tool_context()
        _ctx.ask_user_called_this_turn = True
        _ctx.pending_dict_ambiguity = None
    except RuntimeError:
        pass  # No context in some test paths

    return answer
