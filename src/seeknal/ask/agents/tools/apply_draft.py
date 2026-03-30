"""Apply draft tool — moves a validated draft into the project."""

import ast
import shutil
from pathlib import Path

from langchain_core.tools import tool

# kind → subdirectory under seeknal/
_KIND_DIR_MAP = {
    "source": "sources",
    "transform": "transforms",
    "feature_group": "feature_groups",
    "model": "models",
    "second_order_aggregation": "second_order_aggregations",
}


def _resolve_yaml_target(
    content: str, project_path: Path
) -> tuple[str, str, Path]:
    """Determine kind, name, and target path from YAML draft content.

    Returns:
        (kind, name, target_path)

    Raises:
        ValueError: If kind/name missing or kind unknown.
    """
    import yaml

    data = yaml.safe_load(content)
    if not isinstance(data, dict):
        raise ValueError("YAML root must be a mapping")

    kind = data.get("kind")
    name = data.get("name")

    if not kind:
        raise ValueError("Missing required field: kind")
    if not name:
        raise ValueError("Missing required field: name")

    subdir = _KIND_DIR_MAP.get(kind)
    if not subdir:
        raise ValueError(
            f"Unknown kind '{kind}'. Expected one of: {', '.join(_KIND_DIR_MAP)}"
        )

    target = project_path / "seeknal" / subdir / f"{name}.yml"
    return kind, name, target


def _resolve_python_target(
    draft_path: Path, content: str, project_path: Path
) -> tuple[str, str, Path]:
    """Determine kind, name, and target path from Python draft content.

    Uses AST-based decorator extraction (no importlib).

    Returns:
        (kind, name, target_path)

    Raises:
        ValueError: If no pipeline decorator found or kind unknown.
    """
    from seeknal.workflow.dry_run import extract_decorators

    tree = ast.parse(content, filename=str(draft_path))
    info = extract_decorators(tree)

    if not info:
        raise ValueError(
            "No pipeline decorator found. "
            "Expected @source, @transform, @feature_group, or @second_order_aggregation"
        )

    kind = info["kind"]
    name = info["name"]

    subdir = _KIND_DIR_MAP.get(kind)
    if not subdir:
        raise ValueError(
            f"Unknown kind '{kind}'. Expected one of: {', '.join(_KIND_DIR_MAP)}"
        )

    target = project_path / "seeknal" / subdir / f"{name}.py"
    return kind, name, target


@tool
def apply_draft(file_path: str, confirmed: bool = False) -> str:
    """Apply a draft file to the seeknal project.

    Moves the draft file from the project root into the appropriate
    seeknal/ subdirectory. Manifest regeneration is deferred to
    plan_pipeline / run_pipeline.

    IMPORTANT: You must set confirmed=True to proceed. Show the user
    what will be applied and get their confirmation first.

    Args:
        file_path: Path to the draft file (e.g., 'draft_source_customers.yml').
        confirmed: Must be True to actually apply. Set to False to preview.
    """
    from seeknal.ask.agents.tools._context import get_tool_context
    from seeknal.ask.agents.tools._write_security import validate_draft_path

    ctx = get_tool_context()

    # Validate path
    try:
        resolved = validate_draft_path(file_path, ctx.project_path)
    except ValueError as e:
        return f"Path validation error: {e}"

    if not resolved.exists():
        return f"Draft file not found: {file_path}"

    # Read content for preview
    content = resolved.read_text()

    if not confirmed:
        return (
            f"Ready to apply {file_path}.\n\n"
            f"This will move the file into the seeknal/ directory.\n\n"
            f"Content to apply:\n```\n{content}\n```\n\n"
            f"Call apply_draft('{file_path}', confirmed=True) to proceed."
        )

    # Resolve target path from file content
    try:
        is_python = resolved.suffix == ".py"
        if is_python:
            kind, name, target_path = _resolve_python_target(
                resolved, content, ctx.project_path
            )
        else:
            kind, name, target_path = _resolve_yaml_target(
                content, ctx.project_path
            )
    except ValueError as e:
        return (
            f"Cannot resolve target for {file_path}: {e}\n\n"
            f"Run dry_run_draft('{file_path}') to diagnose issues."
        )

    # Move file under fs_lock
    try:
        with ctx.fs_lock:
            target_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(resolved), str(target_path))
    except OSError as e:
        return f"Failed to move {file_path} to {target_path}: {e}"

    # Refresh artifact discovery after apply
    ctx.artifact_discovery.refresh()

    # Save applied state snapshot (best-effort)
    try:
        from seeknal.workflow.diff_engine import write_applied_state_entry

        node_id = f"{kind}.{name}"
        yaml_content = target_path.read_text(encoding="utf-8")
        rel_path = str(target_path.relative_to(ctx.project_path))
        write_applied_state_entry(ctx.project_path, node_id, rel_path, yaml_content)
    except Exception:
        pass  # Non-critical

    return (
        f"Applied {file_path} → seeknal/{_KIND_DIR_MAP[kind]}/{name}"
        f".{'py' if is_python else 'yml'}\n\n"
        f"Manifest will be regenerated on next plan_pipeline or run_pipeline."
    )
