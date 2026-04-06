"""Ask user tool — presents interactive options for the user to choose from.

This is a direct tool in the seeknal-ask toolset. It allows the main agent
to present arrow-key navigable menus for clarifying questions, brainstorming
choices, and strategic decisions.
"""

from __future__ import annotations

from typing import Any


async def ask_user(
    question: str,
    options: list[dict[str, str]],
) -> str:
    """Ask the user a question with interactive selectable options.

    The user sees an arrow-key navigable menu and can select an option
    or type a custom answer via "Other".

    Use this tool to:
    1. Scope a broad or strategic request before diving into analysis
    2. Present choices when multiple valid directions exist
    3. Gather preferences, priorities, or constraints from the user
    4. Offer next-step options after presenting findings

    Guidelines:
    - Provide 2-4 concrete options with clear descriptions
    - Mark your recommended option with recommended=true
    - Focus on things only the user can decide (not data lookups)
    - Ask BEFORE heavy analysis, not after

    Args:
        question: The question to ask. Should be clear and specific.
        options: 2-4 answer options. Each option is a dict with:
            - 'label': Short option text (1-5 words)
            - 'description': What this option means or implies
            - 'recommended': Set to 'true' to highlight as recommended (optional)
    """
    from seeknal.ask.agents.tools._context import record_ask_user_response
    from seeknal.ask.agents.tools.ask_user import interactive_ask_user

    if not options:
        return "No options provided."

    answer = await interactive_ask_user(question, options)
    record_ask_user_response(options, answer)
    return answer
