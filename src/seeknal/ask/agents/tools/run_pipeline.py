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

    # Build command — always continue on error so independent nodes still execute
    cmd = ["seeknal", "run", "--continue-on-error"]
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
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                cwd=str(ctx.project_path),
            )

            stdout_lines: list[str] = []
            console = ctx.console  # May be None (sync/quiet mode)

            # Read stdout line-by-line for real-time progress
            for line in proc.stdout:
                stripped = line.rstrip()
                stdout_lines.append(stripped)
                if console and stripped:
                    try:
                        from rich.markup import escape

                        console.print(f"  [dim]{escape(stripped)}[/dim]")
                    except Exception:
                        pass  # Don't let display errors break the pipeline

            proc.wait(timeout=_RUN_TIMEOUT)
            stderr_output = proc.stderr.read()

        output = "\n".join(stdout_lines).strip()
        errors = stderr_output.strip()

        if proc.returncode == 0:
            # Refresh artifact discovery and re-register new parquet outputs
            ctx.artifact_discovery.refresh()
            ctx.repl._auto_register_project()

            # List available outputs for inspection
            intermediate = ctx.project_path / "target" / "intermediate"
            suggestions = ""
            if intermediate.exists():
                parquets = sorted(intermediate.glob("*.parquet"))
                if parquets:
                    node_names = [p.stem.replace("_", ".", 1) for p in parquets[:6]]
                    suggestions = (
                        "\n\nAvailable outputs to inspect:\n"
                        + "\n".join(f"  - inspect_output('{n}')" for n in node_names)
                    )

            return f"Pipeline executed successfully.\n\n{output}{suggestions}"
        else:
            # Extract the most useful error lines for the agent.
            # Full output can be very long; surface node-level failures
            # so the agent can self-correct without guessing.
            error_lines = []
            for line in (output + "\n" + errors).splitlines():
                low = line.lower()
                if any(kw in low for kw in [
                    "failed", "error", "exception", "traceback",
                    "not recognized", "does not exist", "invalid",
                ]):
                    error_lines.append(line.strip())
            error_summary = "\n".join(error_lines[:20]) if error_lines else "(no details captured)"

            # If a Python model failed, include its source for the agent to debug
            model_source = ""
            if "Process exited with code 1" in error_summary:
                import re
                for line in error_lines:
                    match = re.search(r"(?:FAILED|failed).*?(\w+\.py)", line)
                    if match:
                        model_path = ctx.project_path / "seeknal" / "transforms" / match.group(1)
                        if model_path.exists():
                            source = model_path.read_text()
                            source_lines = source.strip().splitlines()
                            model_source = "\n".join(source_lines[-40:])
                            break

            return (
                f"Pipeline execution FAILED.\n\n"
                f"Error summary:\n{error_summary}\n\n"
                + (f"Failed model source:\n```python\n{model_source}\n```\n\n" if model_source else "")
                + f"Full errors:\n{errors}"
            )
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
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
