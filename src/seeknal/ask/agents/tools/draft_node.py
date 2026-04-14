"""Draft node tool — creates a new pipeline node draft file."""


async def draft_node(node_type: str, name: str, python: bool = False) -> str:
    """Generate a pipeline-node draft template under .seeknal/drafts/.

    See the `build-pipeline-node` skill for the full draft → validate → apply
    → run workflow and the supported node types.

    Args:
        node_type: source, transform, feature_group, model, aggregation, rule,
            profile, exposure, semantic_model, metric (snake_case or hyphen).
        name: Alphanumeric + underscores only, ≤128 chars.
        python: True for a Python file template, False for YAML.
    """
    from seeknal.ask.agents.tools._context import get_tool_context
    from seeknal.ask.agents.tools._write_security import get_drafts_dir
    from seeknal.workflow.draft import (
        NODE_TYPES, TEMPLATE_FILES, PYTHON_TEMPLATE_FILES,
        get_template_env, generate_filename, render_template,
    )

    ctx = get_tool_context()
    drafts_dir = get_drafts_dir(ctx.project_path)

    # Validate node type
    if node_type not in NODE_TYPES:
        valid = ", ".join(sorted(set(NODE_TYPES.keys())))
        return f"Invalid node type '{node_type}'. Valid types: {valid}"

    normalized = NODE_TYPES[node_type]

    # Validate name
    if not name or "/" in name or "\\" in name or ".." in name:
        return "Invalid name. Use alphanumeric characters and underscores only."
    if len(name) > 128:
        return "Name cannot exceed 128 characters."

    # Check template exists
    template_map = PYTHON_TEMPLATE_FILES if python else TEMPLATE_FILES
    if normalized not in template_map:
        lang = "Python" if python else "YAML"
        return f"No {lang} template available for node type '{node_type}'."

    # Generate draft file
    filename = generate_filename(normalized, name, python=python)
    filepath = drafts_dir / filename

    if filepath.exists():
        return f"Draft file already exists: {filename}. Edit it or remove it first."

    try:
        env = get_template_env()
        content = render_template(env, normalized, name, description=None, python=python)

        with ctx.fs_lock:
            filepath.write_text(content)

        return (
            f"Created draft: {filename}\n\n"
            f"Next steps:\n"
            f"1. Edit the file to configure your {node_type}\n"
            f"2. Validate: use dry_run_draft('{filename}')\n"
            f"3. Apply: use apply_draft('{filename}')\n\n"
            f"Content:\n```\n{content}\n```"
        )
    except Exception as e:
        return f"Failed to create draft: {e}"
