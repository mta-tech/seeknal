"""Edit node tool — modifies an existing pipeline node definition."""

from langchain_core.tools import tool


@tool
def edit_node(
    node_path: str,
    new_content: str,
    confirmed: bool = False,
) -> str:
    """Edit an existing pipeline node definition file.

    Replaces the content of a YAML or Python file in the seeknal/ directory.
    Shows a diff before applying and requires confirmation.

    Args:
        node_path: Relative path to the node file
                   (e.g., 'seeknal/transforms/clean_orders.yml').
        new_content: The complete new file content to write.
        confirmed: Must be True to actually write. Set to False to preview diff.
    """
    import difflib

    from seeknal.ask.agents.tools._context import get_tool_context
    from seeknal.ask.agents.tools._write_security import validate_write_path

    ctx = get_tool_context()

    # Validate path
    try:
        resolved = validate_write_path(node_path, ctx.project_path)
    except ValueError as e:
        return f"Path validation error: {e}"

    if not resolved.exists():
        return f"Node file not found: {node_path}"

    # Read current content
    old_content = resolved.read_text()

    # Generate diff
    diff = difflib.unified_diff(
        old_content.splitlines(keepends=True),
        new_content.splitlines(keepends=True),
        fromfile=f"a/{node_path}",
        tofile=f"b/{node_path}",
    )
    diff_text = "".join(diff)

    if not diff_text:
        return "No changes detected — file content is identical."

    if not confirmed:
        return (
            f"Changes to {node_path}:\n\n"
            f"```diff\n{diff_text}```\n\n"
            f"Call edit_node('{node_path}', ..., confirmed=True) to apply."
        )

    # Write new content
    try:
        with ctx.fs_lock:
            resolved.write_text(new_content)

        # Refresh artifact discovery
        ctx.artifact_discovery.refresh()

        return (
            f"Updated {node_path} successfully.\n\n"
            f"Diff:\n```diff\n{diff_text}```\n\n"
            f"Run dry_run_draft or seeknal plan to verify."
        )
    except Exception as e:
        return f"Edit failed: {e}"
