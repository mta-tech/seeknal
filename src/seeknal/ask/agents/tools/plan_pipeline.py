"""Plan pipeline tool — shows the execution plan (DAG order)."""

from langchain_core.tools import tool


@tool
def plan_pipeline() -> str:
    """Show the seeknal execution plan (topological sort of the DAG).

    Returns the list of nodes in execution order, showing dependencies
    and what will run when 'seeknal run' is called.
    """
    import subprocess

    from seeknal.ask.agents.tools._context import get_tool_context

    ctx = get_tool_context()

    try:
        result = subprocess.run(
            ["seeknal", "plan"],
            capture_output=True,
            text=True,
            cwd=str(ctx.project_path),
            timeout=30,
        )

        output = result.stdout.strip()
        errors = result.stderr.strip()

        if result.returncode == 0:
            return f"Execution Plan:\n\n{output}"
        else:
            return f"Plan failed:\n{errors}\n\n{output}"
    except subprocess.TimeoutExpired:
        return "Plan command timed out after 30 seconds."
    except FileNotFoundError:
        return "seeknal CLI not found. Ensure seeknal is installed and on PATH."
    except Exception as e:
        return f"Plan failed: {e}"
