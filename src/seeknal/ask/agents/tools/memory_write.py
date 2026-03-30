"""Memory write tool — saves a memory entry to persistent storage."""

from langchain_core.tools import tool


@tool
def memory_write(content: str, category: str = "project_knowledge") -> str:
    """Save a memory to persistent storage for recall in future sessions.

    Use this to remember important facts, patterns, or decisions about the
    project's data, schemas, pipelines, or user preferences. Memories persist
    across sessions and are shared with all sessions for this project.

    Categories:
    - project_knowledge: Data patterns, schema conventions, business rules,
      data quality findings, pipeline decisions
    - user_preferences: SQL style, naming conventions, default entities/time
      columns, report preferences
    - operational_history: Past pipeline runs, error patterns, performance
      baselines, notable queries/reports

    Args:
        content: The memory text to save. Be specific and concise.
        category: One of project_knowledge, user_preferences, operational_history.
    """
    from seeknal.ask.agents.tools._context import get_tool_context

    ctx = get_tool_context()
    if ctx.memory_store is None:
        return "Memory store not available."

    return ctx.memory_store.write(content, category)
