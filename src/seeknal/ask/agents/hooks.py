"""Security hooks for seeknal ask agent.

Implements Claude Code-style PRE_TOOL_USE hooks for SQL validation.
The SQL validation logic lives in security.py (unchanged) — this module
wires it into pydantic-deep's hook lifecycle.
"""

from pydantic_deep.capabilities.hooks import Hook, HookEvent, HookInput, HookResult


async def _sql_security_handler(hook_input: HookInput) -> HookResult:
    """Validate SQL queries before execution via PRE_TOOL_USE hook.

    Calls validate_sql_for_agent() from security.py. On validation failure,
    returns HookResult(allow=False) which triggers ModelRetry — the agent
    sees the denial reason and can adjust its query.

    The entire handler is wrapped in try/except to prevent unhandled
    exceptions from propagating up and failing the tool call entirely.
    """
    try:
        from seeknal.ask.security import validate_sql_for_agent

        sql = hook_input.tool_input.get("sql", "")
        validate_sql_for_agent(sql)
        return HookResult(allow=True)
    except ValueError as e:
        return HookResult(allow=False, reason=f"SQL validation error: {e}")
    except Exception as e:
        return HookResult(allow=False, reason=f"Unexpected validation error: {e}")


def get_security_hooks() -> list[Hook]:
    """Return the list of security hooks for the seeknal ask agent."""
    return [
        Hook(
            event=HookEvent.PRE_TOOL_USE,
            handler=_sql_security_handler,
            matcher="execute_sql",
        ),
    ]
