"""Get entities tool — lists consolidated entities with metadata."""

from langchain_core.tools import tool


@tool
def get_entities() -> str:
    """List all consolidated entities in the seeknal project.

    Returns entity names, join keys, and feature group counts.
    Use get_entity_schema for detailed schema of a specific entity.
    """
    from seeknal.ask.agents.tools._context import get_tool_context

    ctx = get_tool_context()
    entities = ctx.artifact_discovery.get_entities_summary()

    if not entities:
        return (
            "No consolidated entities found. "
            "Run `seeknal run` to produce entity artifacts."
        )

    lines = ["Consolidated entities:\n"]
    for entity in entities:
        name = entity["name"]
        join_keys = ", ".join(entity["join_keys"])
        fg_count = entity["feature_group_count"]
        lines.append(
            f"- **{name}** (join keys: {join_keys}, "
            f"{fg_count} feature group{'s' if fg_count != 1 else ''})"
        )

    return "\n".join(lines)
