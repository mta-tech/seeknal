"""Apply draft tool — moves a validated draft into the project."""

from langchain_core.tools import tool


@tool
def apply_draft(file_path: str, confirmed: bool = False) -> str:
    """Apply a draft file to the seeknal project.

    Moves the draft file from the project root into the appropriate
    seeknal/ subdirectory and regenerates the manifest.

    IMPORTANT: You must set confirmed=True to proceed. Show the user
    what will be applied and get their confirmation first.

    Args:
        file_path: Path to the draft file (e.g., 'draft_source_customers.yml').
        confirmed: Must be True to actually apply. Set to False to preview.
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

    # Read content for preview
    content = resolved.read_text()

    if not confirmed:
        return (
            f"Ready to apply {file_path}.\n\n"
            f"This will move the file into the seeknal/ directory "
            f"and regenerate the manifest.\n\n"
            f"Content to apply:\n```\n{content}\n```\n\n"
            f"Call apply_draft('{file_path}', confirmed=True) to proceed."
        )

    # Apply with seeknal CLI
    try:
        with ctx.fs_lock:
            result = subprocess.run(
                ["seeknal", "apply", str(resolved)],
                capture_output=True,
                text=True,
                cwd=str(ctx.project_path),
                timeout=120,
            )

        output = result.stdout.strip()
        errors = result.stderr.strip()

        if result.returncode == 0:
            # Refresh artifact discovery after apply
            ctx.artifact_discovery.refresh()
            return f"Applied {file_path} successfully.\n\n{output}"
        else:
            return (
                f"Apply FAILED for {file_path}\n\n"
                f"Errors:\n{errors}\n\n"
                f"Output:\n{output}\n\n"
                f"Run dry_run_draft('{file_path}') to diagnose issues."
            )
    except subprocess.TimeoutExpired:
        return f"Apply timed out after 120 seconds for {file_path}"
    except FileNotFoundError:
        return "seeknal CLI not found. Ensure seeknal is installed and on PATH."
    except Exception as e:
        return f"Apply failed: {e}"
