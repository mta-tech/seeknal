"""Memory search tool — retrieves relevant memories from persistent storage."""

from langchain_core.tools import tool


@tool
def memory_search(query: str) -> str:
    """Search persistent memory for relevant past knowledge.

    Use this to recall facts, patterns, or decisions you've previously saved
    about this project. Searches across the memory index and recent daily logs
    using keyword matching.

    Args:
        query: Space-separated keywords to search for (e.g., "customer_id schema").
    """
    from seeknal.ask.agents.tools._context import get_tool_context

    ctx = get_tool_context()
    if ctx.memory_store is None:
        return "Memory store not available."

    results = ctx.memory_store.search(query)
    if not results:
        return "No memories found matching your query."

    formatted = "\n".join(f"- {r}" for r in results)
    return f"Found {len(results)} matching memories:\n{formatted}"
