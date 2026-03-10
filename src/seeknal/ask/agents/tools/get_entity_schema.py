"""Get entity schema tool — detailed EntityCatalog for an entity."""

from langchain_core.tools import tool


@tool
def get_entity_schema(entity_name: str) -> str:
    """Get detailed schema for a consolidated entity.

    Shows all feature groups, their features with types, and join keys.
    Use this to understand the data model before writing queries.

    Args:
        entity_name: Name of the entity to describe.
    """
    import re

    from seeknal.ask.agents.tools._context import get_tool_context

    ctx = get_tool_context()

    # Validate entity name
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", entity_name):
        return f"Invalid entity name: '{entity_name}'"

    catalog = ctx.artifact_discovery.get_entity_catalog(entity_name)
    if catalog is None:
        return f"Entity '{entity_name}' not found. Use get_entities() to list available entities."

    lines = [f"## Entity: `{entity_name}`\n"]
    lines.append(f"**Join keys**: {', '.join(catalog.get('join_keys', []))}")
    lines.append(f"**Table name**: `{entity_name}` (DuckDB view)\n")

    feature_groups = catalog.get("feature_groups", {})
    for fg_name, fg_info in feature_groups.items():
        features = fg_info.get("features", {})
        lines.append(f"### Feature Group: `{fg_name}`")
        for fname, ftype in features.items():
            lines.append(f"  - `{fg_name}.{fname}` ({ftype})")
        lines.append("")

    return "\n".join(lines)
