"""Run pipeline tool — executes the seeknal pipeline."""

import os

from langchain_core.tools import tool

# Configurable timeout (default 300s = 5 minutes)
_RUN_TIMEOUT = int(os.environ.get("SEEKNAL_RUN_TIMEOUT", "300"))


@tool
def run_pipeline(
    nodes: str = "",
    full: bool = False,
    confirmed: bool = False,
) -> str:
    """Execute the seeknal pipeline.

    Runs all nodes (or a subset) in topological order. This executes
    real data transformations and may take time.

    IMPORTANT: You must set confirmed=True to proceed. Running a pipeline
    modifies output files and may write to external systems.

    Args:
        nodes: Optional comma-separated node IDs to run
               (e.g., 'transform.clean,transform.enrich').
               If empty, runs all nodes.
        full: If True, ignore cache and re-run everything.
        confirmed: Must be True to actually run. Set to False to preview.
    """
    import subprocess

    from seeknal.ask.agents.tools._context import get_tool_context

    ctx = get_tool_context()

    # Build command
    cmd = ["seeknal", "run"]
    if nodes:
        cmd.extend(["--nodes", nodes])
    if full:
        cmd.append("--full")

    cmd_str = " ".join(cmd)

    if not confirmed:
        return (
            f"Ready to execute: {cmd_str}\n\n"
            f"This will run data transformations and update output files.\n"
            f"Timeout: {_RUN_TIMEOUT}s\n\n"
            f"Call run_pipeline({_format_args(nodes, full)}, confirmed=True) to proceed."
        )

    try:
        with ctx.fs_lock:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                cwd=str(ctx.project_path),
                timeout=_RUN_TIMEOUT,
            )

        output = result.stdout.strip()
        errors = result.stderr.strip()

        if result.returncode == 0:
            # Refresh artifact discovery and re-register new parquet outputs
            ctx.artifact_discovery.refresh()
            ctx.repl._auto_register_project()
            return f"Pipeline executed successfully.\n\n{output}"
        else:
            return (
                f"Pipeline execution FAILED.\n\n"
                f"Errors:\n{errors}\n\n"
                f"Output:\n{output}"
            )
    except subprocess.TimeoutExpired:
        return f"Pipeline timed out after {_RUN_TIMEOUT} seconds. Set SEEKNAL_RUN_TIMEOUT env var to increase."
    except FileNotFoundError:
        return "seeknal CLI not found. Ensure seeknal is installed and on PATH."
    except Exception as e:
        return f"Pipeline run failed: {e}"


def _format_args(nodes: str, full: bool) -> str:
    """Format tool arguments for the confirmation message."""
    parts = []
    if nodes:
        parts.append(f"nodes='{nodes}'")
    if full:
        parts.append("full=True")
    return ", ".join(parts) if parts else ""
