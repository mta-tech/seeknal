"""Submit plan tool — agent submits execution plan before acting."""

from langchain_core.tools import tool


@tool
def submit_plan(steps: list[str]) -> str:
    """Submit an execution plan before taking action.

    MUST be called as your FIRST action before any other tool.
    Each step should be a concise description of what you will do and why.

    Args:
        steps: Ordered list of plan steps
               (e.g., ["Profile data files to discover schemas", ...])
    """
    from seeknal.ask.agents.tools._context import get_tool_context

    ctx = get_tool_context()

    if not steps:
        return "Plan must have at least one step."

    ctx.plan_steps = list(steps)
    ctx.plan_step_index = 0

    return f"Plan submitted with {len(steps)} steps. Proceed with step 1."
