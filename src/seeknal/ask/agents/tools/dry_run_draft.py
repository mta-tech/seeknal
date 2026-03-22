"""Dry-run draft tool — validates a draft file without applying it."""

from langchain_core.tools import tool


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
            return f"Validation PASSED for {file_path}\n\n{output}"
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
