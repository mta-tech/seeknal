"""Dry-run draft tool — validates a draft file without applying it."""

import re

from langchain_core.tools import tool


def _check_python_deps(draft_path) -> str:
    """Check that PEP 723 dependencies can be resolved by uv."""
    from pathlib import Path

    if not str(draft_path).endswith(".py"):
        return ""

    try:
        from seeknal.workflow.executors.python_executor import check_pep723_deps
        warnings = check_pep723_deps(Path(draft_path))
        if warnings:
            return "WARNING: " + "\n".join(warnings)
    except Exception:
        pass  # Graceful degradation

    return ""


def _check_yaml_ref_consistency(draft_path) -> str:
    """Check that all ref() calls in SQL match declared inputs."""
    import yaml

    if not str(draft_path).endswith((".yml", ".yaml")):
        return ""

    try:
        data = yaml.safe_load(draft_path.read_text())
    except Exception:
        return ""

    transform_sql = data.get("transform", "")
    if not transform_sql:
        return ""

    # Extract refs from SQL
    sql_refs = set(re.findall(r"ref\(\s*['\"]([^'\"]+)['\"]\s*\)", transform_sql))
    if not sql_refs:
        return ""

    # Extract declared inputs
    declared_refs = set()
    for inp in data.get("inputs", []):
        if isinstance(inp, dict) and "ref" in inp:
            declared_refs.add(inp["ref"])

    # Find mismatches
    missing_inputs = sql_refs - declared_refs
    if missing_inputs:
        missing_list = ", ".join(sorted(missing_inputs))
        return (
            f"WARNING: SQL uses ref() calls not declared in inputs: {missing_list}\n"
            f"Add these to the inputs: list or the pipeline will fail at runtime.\n"
            f"Declared inputs: {sorted(declared_refs)}\n"
            f"SQL refs: {sorted(sql_refs)}"
        )
    return ""


@tool
def dry_run_draft(file_path: str) -> str:
    """Validate a draft file by running seeknal dry-run.

    Checks YAML syntax, schema validity, and SQL correctness without
    modifying the project. Always run this before apply_draft.

    Args:
        file_path: Path to the draft file (e.g., 'draft_source_customers.yml').
    """
    import subprocess

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

    # Run seeknal dry-run as subprocess
    try:
        result = subprocess.run(
            ["seeknal", "dry-run", str(resolved)],
            capture_output=True,
            text=True,
            cwd=str(ctx.project_path),
            timeout=60,
        )

        output = result.stdout.strip()
        errors = result.stderr.strip()

        if result.returncode == 0:
            # Additional validation checks
            ref_warnings = _check_yaml_ref_consistency(resolved)
            dep_warnings = _check_python_deps(resolved)
            suffix_parts = [w for w in [ref_warnings, dep_warnings] if w]
            suffix = "\n\n" + "\n\n".join(suffix_parts) if suffix_parts else ""
            return f"Validation PASSED for {file_path}\n\n{output}{suffix}"
        else:
            return (
                f"Validation FAILED for {file_path}\n\n"
                f"Errors:\n{errors}\n\n"
                f"Output:\n{output}"
            )
    except subprocess.TimeoutExpired:
        return f"Dry-run timed out after 60 seconds for {file_path}"
    except FileNotFoundError:
        return "seeknal CLI not found. Ensure seeknal is installed and on PATH."
    except Exception as e:
        return f"Dry-run failed: {e}"
