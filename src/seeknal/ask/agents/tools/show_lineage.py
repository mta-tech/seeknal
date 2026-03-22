"""Show lineage tool — displays the pipeline DAG as ASCII tree."""

from langchain_core.tools import tool


@tool
def show_lineage(node_id: str = "") -> str:
    """Show pipeline lineage as an ASCII tree.

    Displays the full DAG or a focused view centered on one node,
    showing upstream and downstream dependencies.

    Args:
        node_id: Optional node to focus on (e.g., 'transform.clean_orders').
                 If empty, shows the full pipeline lineage.
    """
    import subprocess

    from seeknal.ask.agents.tools._context import get_tool_context

    ctx = get_tool_context()

    cmd = ["seeknal", "lineage", "--ascii"]
    if node_id:
        cmd.insert(2, node_id)  # seeknal lineage <node_id> --ascii

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=str(ctx.project_path),
            timeout=30,
        )

        output = result.stdout.strip()
        errors = result.stderr.strip()

        if result.returncode == 0:
            label = f"Lineage for {node_id}" if node_id else "Full Pipeline Lineage"
            return f"{label}:\n\n{output}"
        else:
            return f"Lineage failed:\n{errors}\n\n{output}"
    except subprocess.TimeoutExpired:
        return "Lineage command timed out after 30 seconds."
    except FileNotFoundError:
        return "seeknal CLI not found. Ensure seeknal is installed and on PATH."
    except Exception as e:
        return f"Lineage failed: {e}"
