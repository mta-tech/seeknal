"""Search pipelines tool — searches across pipeline definitions."""

from langchain_core.tools import tool


@tool
def search_pipelines(query: str) -> str:
    """Search across all seeknal pipeline definitions for matching content.

    Use this to find which pipeline defines a specific table, column,
    or calculation. Case-insensitive search.

    Args:
        query: Text to search for in pipeline files.
    """
    from seeknal.ask.agents.tools._context import get_tool_context

    ctx = get_tool_context()
    results = ctx.artifact_discovery.search_pipeline_content(query)

    if not results:
        return f"No pipeline files matching '{query}'."

    lines = [f"Found {len(results)} match(es):\n"]
    for r in results:
        lines.append(
            f"- **{r['name']}** ({r['kind']}) at "
            f"`{r['file_path']}` line {r['matched_line']}"
        )
        lines.append(f"  `{r['context']}`")

    return "\n".join(lines)
