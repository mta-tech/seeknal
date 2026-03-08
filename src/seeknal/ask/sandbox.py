"""Subprocess sandbox for executing Python code in isolation.

Generates a runner script that:
1. Opens a fresh DuckDB connection
2. Auto-registers all project parquet files as views
3. Executes agent-generated code with AST last-expression capture
4. Returns results as JSON on stdout

Uses sys.executable for subprocess isolation — packages come from the
current venv, so no dependency download is needed at runtime.
"""

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Optional


def _find_seeknal_src() -> str:
    """Find the seeknal source root for sys.path injection."""
    import seeknal
    return str(Path(seeknal.__file__).parent.parent)


_RUNNER_TEMPLATE = '''\
import ast
import json
import os
import sys
import tempfile
import traceback
from io import StringIO
from pathlib import Path

# Add seeknal to sys.path so we can import helpers
sys.path.insert(0, {seeknal_src!r})

# --- Data loading ---

def load_project_data(project_path: str):
    """Create a DuckDB connection with all project data registered."""
    import duckdb

    conn = duckdb.connect(":memory:")
    conn.execute("SET memory_limit='256MB'")
    conn.execute("SET threads=2")

    root = Path(project_path)

    # Register intermediate parquets (transform_*, source_*, etc.)
    intermediate = root / "target" / "intermediate"
    if intermediate.exists():
        for pq in intermediate.rglob("*.parquet"):
            view_name = pq.stem
            safe_path = str(pq.resolve()).replace("'", "''")
            try:
                conn.execute(
                    f'CREATE VIEW "{{view_name}}" AS '
                    f"SELECT * FROM read_parquet('{{safe_path}}')"
                )
            except Exception:
                pass

    # Register legacy cache parquets
    cache = root / "target" / "cache"
    if cache.exists():
        registered = {{pq.stem for pq in intermediate.rglob("*.parquet")}} if intermediate.exists() else set()
        for pq in cache.rglob("*.parquet"):
            if pq.stem not in registered:
                safe_path = str(pq.resolve()).replace("'", "''")
                try:
                    conn.execute(
                        f'CREATE VIEW "{{pq.stem}}" AS '
                        f"SELECT * FROM read_parquet('{{safe_path}}')"
                    )
                except Exception:
                    pass

    # Register consolidated entity parquets
    feature_store = root / "target" / "feature_store"
    if feature_store.exists():
        for entity_dir in feature_store.iterdir():
            if entity_dir.is_dir():
                features_pq = entity_dir / "features.parquet"
                if features_pq.exists():
                    view_name = f"entity_{{entity_dir.name}}"
                    safe_path = str(features_pq.resolve()).replace("'", "''")
                    try:
                        conn.execute(
                            f'CREATE VIEW "{{view_name}}" AS '
                            f"SELECT * FROM read_parquet('{{safe_path}}')"
                        )
                    except Exception:
                        pass

    return conn


def _split_last_expression(code):
    """Split code into body and optional last expression for eval."""
    try:
        tree = ast.parse(code)
    except SyntaxError:
        return code, None

    if not tree.body:
        return code, None

    last_node = tree.body[-1]
    if not isinstance(last_node, ast.Expr):
        return code, None

    body = tree.body[:-1]
    if body:
        body_module = ast.Module(body=body, type_ignores=[])
        ast.fix_missing_locations(body_module)
        body_code = compile(body_module, "<agent>", "exec")
    else:
        body_code = None

    expr_node = ast.Expression(body=last_node.value)
    ast.fix_missing_locations(expr_node)
    expr_code = compile(expr_node, "<agent>", "eval")
    return body_code, expr_code


def _capture_plots(plot_dir):
    """Save open matplotlib figures to plot_dir."""
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        return []

    fig_nums = plt.get_fignums()
    if not fig_nums:
        return []

    paths = []
    for i, fig_num in enumerate(fig_nums):
        fig = plt.figure(fig_num)
        path = os.path.join(plot_dir, f"plot_{{i}}.png")
        fig.savefig(path, dpi=150, bbox_inches="tight", facecolor="white")
        paths.append(path)

    plt.close("all")
    return paths


# --- Main execution ---

project_path = {project_path!r}
plot_dir = {plot_dir!r}
os.makedirs(plot_dir, exist_ok=True)

# Set up namespace
conn = load_project_data(project_path)

try:
    import pandas as pd
except ImportError:
    pd = None

try:
    import numpy as np
except ImportError:
    np = None

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ImportError:
    plt = None
    matplotlib = None

namespace = {{
    "conn": conn,
    "pd": pd,
    "np": np,
    "plt": plt,
    "matplotlib": matplotlib,
}}

# The agent code to execute
agent_code = {agent_code!r}

# Execute with output capture
captured = StringIO()
namespace["print"] = lambda *a, **kw: print(*a, file=captured, **kw)

value = None
error = None
plots = []

body_code, expr_code = _split_last_expression(agent_code)

try:
    if isinstance(body_code, str):
        exec(compile(body_code, "<agent>", "exec"), namespace)
    else:
        if body_code is not None:
            exec(body_code, namespace)
        if expr_code is not None:
            value = eval(expr_code, namespace)
except Exception:
    error = traceback.format_exc()

# Capture plots
try:
    plots = _capture_plots(plot_dir)
except Exception:
    pass

# Format value
value_str = None
if error is None and value is not None:
    value_str = repr(value) if not isinstance(value, str) else value
    if len(value_str) > 5000:
        value_str = value_str[:5000] + "\\n... (truncated)"

# Output result as JSON on stdout
result = {{
    "value": value_str,
    "stdout": captured.getvalue(),
    "error": error,
    "plots": plots,
}}
json.dump(result, sys.stdout)
'''


def execute_in_sandbox(
    code: str,
    project_path: Path,
    timeout: int = 30,
) -> str:
    """Execute Python code in an isolated subprocess.

    Uses sys.executable to run a generated script in a separate process,
    providing process isolation with killable timeout. Packages come from
    the current venv — no dependency download needed.

    Args:
        code: Python code to execute.
        project_path: Path to the seeknal project root.
        timeout: Maximum execution time in seconds.

    Returns:
        Formatted result string for the agent.
    """
    if not code or not code.strip():
        return "No code provided."

    # Set up paths
    target_dir = project_path / "target"
    runner_dir = target_dir / ".ask_sandbox"
    runner_dir.mkdir(parents=True, exist_ok=True)
    plot_dir = str(target_dir / ".ask_plots")

    # Generate runner script
    runner_path = runner_dir / "_runner.py"
    script_content = _RUNNER_TEMPLATE.format(
        seeknal_src=_find_seeknal_src(),
        project_path=str(project_path),
        plot_dir=plot_dir,
        agent_code=code,
    )
    runner_path.write_text(script_content, encoding="utf-8")

    # Execute in isolated subprocess
    try:
        result = subprocess.run(
            [sys.executable, str(runner_path)],
            cwd=str(project_path),
            capture_output=True,
            text=True,
            timeout=timeout,
            env=_get_sandbox_env(),
        )
    except subprocess.TimeoutExpired:
        return f"Execution timed out after {timeout} seconds."
    except OSError as e:
        return f"Error launching subprocess: {e}"

    # Parse output
    if result.returncode != 0:
        # Check if there's JSON on stdout (code error, not process error)
        if result.stdout.strip():
            try:
                data = json.loads(result.stdout)
                if data.get("error"):
                    return _format_output(data)
            except json.JSONDecodeError:
                pass
        # Process-level error
        stderr = result.stderr.strip()
        if len(stderr) > 2000:
            stderr = stderr[-2000:]
        return f"Error:\n{stderr}" if stderr else f"Process exited with code {result.returncode}"

    # Parse JSON result from stdout
    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError:
        # Fallback: raw stdout
        return result.stdout[:5000] if result.stdout else "Code executed (no output)."

    return _format_output(data)


def _format_output(data: dict) -> str:
    """Format the JSON output from the subprocess into a string."""
    parts: list[str] = []

    stdout = data.get("stdout", "")
    if stdout:
        parts.append(stdout.rstrip())

    error = data.get("error")
    value = data.get("value")

    if error:
        parts.append(f"Error:\n{error}")
    elif value is not None:
        parts.append(value)

    plots = data.get("plots", [])
    if plots:
        plot_list = "\n".join(f"- {p}" for p in plots)
        parts.append(f"Plots saved:\n{plot_list}")

    return "\n\n".join(parts) if parts else "Code executed successfully (no output)."


def _get_sandbox_env() -> dict[str, str]:
    """Build environment for the sandbox subprocess.

    Passes through essential env vars but strips sensitive ones.
    """
    env = os.environ.copy()
    # Remove sensitive variables the LLM code shouldn't access
    for key in list(env.keys()):
        key_lower = key.lower()
        if any(s in key_lower for s in ("api_key", "secret", "token", "password", "credential")):
            del env[key]
    return env
