"""Execute Python tool — run Python code for data analysis.

Executes agent-generated Python in an isolated subprocess.
The subprocess gets its own DuckDB connection with project data registered
as views from parquet files — no access to the parent process.

Falls back to in-process threading if subprocess execution fails.
"""

import ast
import os
import tempfile
import threading
import traceback
from io import StringIO
from typing import Any

def _split_last_expression(code: str):
    """Split code into body and optional last expression.

    Returns (body_code, expr_code) where:
    - body_code is compiled code for exec (or None if no body)
    - expr_code is compiled code for eval (or None if last stmt isn't an expression)
    """
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


def _build_namespace(conn: Any) -> dict:
    """Build execution namespace with pre-loaded libraries."""
    from seeknal.ask.agents.tools._safe_connection import SafeConnection

    ns: dict[str, Any] = {"conn": SafeConnection(conn)}

    try:
        import pandas as pd
        ns["pd"] = pd
    except ImportError:
        pass

    try:
        import numpy as np
        ns["np"] = np
    except ImportError:
        pass

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        ns["plt"] = plt
        ns["matplotlib"] = matplotlib
    except ImportError:
        pass

    try:
        import sklearn
        ns["sklearn"] = sklearn
    except ImportError:
        pass

    try:
        import scipy
        ns["scipy"] = scipy
    except ImportError:
        pass

    return ns


def _capture_plots() -> list[str]:
    """Save any open matplotlib figures to temp files."""
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        return []

    fig_nums = plt.get_fignums()
    if not fig_nums:
        return []

    paths: list[str] = []
    for i, fig_num in enumerate(fig_nums):
        fig = plt.figure(fig_num)
        fd, path = tempfile.mkstemp(suffix=".png", prefix=f"plot_{i}_")
        os.close(fd)
        fig.savefig(path, dpi=150, bbox_inches="tight", facecolor="white")
        paths.append(path)

    plt.close("all")
    return paths


def _do_execute(code: str, conn: Any, timeout: int = 30) -> str:
    """In-process execution fallback (used when uv is not available)."""
    if not code or not code.strip():
        return "No code provided."

    namespace = _build_namespace(conn)
    body_code, expr_code = _split_last_expression(code)

    result_holder: dict[str, Any] = {
        "value": None, "stdout": "", "error": None, "plots": [],
    }

    def target():
        captured = StringIO()
        namespace["print"] = lambda *a, **kw: print(*a, file=captured, **kw)
        try:
            if isinstance(body_code, str):
                exec(compile(body_code, "<agent>", "exec"), namespace)
            else:
                if body_code is not None:
                    exec(body_code, namespace)
                if expr_code is not None:
                    result_holder["value"] = eval(expr_code, namespace)
            result_holder["stdout"] = captured.getvalue()
            result_holder["plots"] = _capture_plots()
        except Exception:
            result_holder["stdout"] = captured.getvalue()
            result_holder["error"] = traceback.format_exc()

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    thread.join(timeout)

    if thread.is_alive():
        return f"Execution timed out after {timeout} seconds."

    parts: list[str] = []

    if result_holder["stdout"]:
        parts.append(result_holder["stdout"].rstrip())

    if result_holder["error"]:
        parts.append(f"Error:\n{result_holder['error']}")
    elif result_holder["value"] is not None:
        val = result_holder["value"]
        val_str = repr(val) if not isinstance(val, str) else val
        if len(val_str) > 5000:
            val_str = val_str[:5000] + "\n... (truncated)"
        parts.append(val_str)

    if result_holder["plots"]:
        plot_list = "\n".join(f"- {p}" for p in result_holder["plots"])
        parts.append(f"Plots saved:\n{plot_list}")

    return "\n\n".join(parts) if parts else "Code executed successfully (no output)."


def _infer_error_hint(result: str, code: str) -> str | None:
    """Return a targeted hint based on common sandbox errors."""
    if "ModuleNotFoundError" in result:
        return (
            "Only pandas, numpy, matplotlib, scikit-learn, and scipy are "
            "available in the sandbox. Do NOT import statsmodels, xgboost, "
            "lightgbm, or other packages."
        )
    if "CatalogException" in result and "does not exist" in result:
        if "duckdb.connect" in code:
            return (
                "You called `duckdb.connect()` yourself — that creates a "
                "NEW empty in-memory database without your project's tables. "
                "The sandbox ALREADY provides a `conn` object with every "
                "project table registered as a view. Remove `import duckdb` "
                "and `conn = duckdb.connect(...)` entirely — the pre-loaded "
                "`conn` is ready to use: "
                "`df = conn.sql('SELECT * FROM transform_daily_revenue').df()`."
            )
        return (
            "The table name doesn't exist in the sandbox's `conn`. Run "
            "`list_tables` (via a separate tool call or `conn.sql(\"SHOW "
            "TABLES\").df()` inside execute_python) to see what's available. "
            "Views are registered by their full kind-prefixed name "
            "(e.g. `transform_daily_revenue`, `source_customers`)."
        )
    if "NameError" in result:
        return (
            "Each execute_python call runs in a fresh subprocess — variables "
            "from previous calls do not persist. Re-query the data with "
            "conn.sql('SELECT ...').df() at the start of this call."
        )
    if "ParserException" in result and "#" in code:
        return (
            "You likely put a Python # comment inside a SQL string. "
            "DuckDB does not recognize # as a comment. Remove all # comments "
            "from inside SQL strings, or use -- for SQL comments."
        )
    return None


async def execute_python(code: str) -> str:
    """Run Python in an isolated subprocess sandbox. Prefer execute_sql for simple queries.

    See the `execute-python-analysis` skill for the sandbox semantics (no
    cross-call persistence, limited package set), the available libs (conn,
    pd, np, plt, sklearn, scipy), the SQL-comment gotcha (`#` is not a DuckDB
    comment), plot capture, and error recovery.

    Args:
        code: Python code to execute. Last expression is captured Jupyter-style.
    """
    from seeknal.ask.agents.tools._context import get_tool_context
    from seeknal.ask.sandbox import async_execute_in_sandbox

    ctx = get_tool_context()
    result = await async_execute_in_sandbox(code, ctx.project_path)

    # Backgrounded results pass through directly
    from seeknal.ask.background import _BACKGROUNDED_PREFIX
    if result.startswith(_BACKGROUNDED_PREFIX):
        return result

    # Wrap error results in structured error JSON for agent self-correction
    from seeknal.ask.agents.tools.errors import (
        RETRYABLE_SYNTAX,
        TERMINAL_TIMEOUT,
        format_tool_error,
    )

    if result.startswith("Execution timed out"):
        return format_tool_error(TERMINAL_TIMEOUT, result)
    if result.startswith("Error launching subprocess:"):
        return format_tool_error(RETRYABLE_SYNTAX, result)
    if result.startswith("Error:\n") or result.startswith("Process exited with code"):
        hint = _infer_error_hint(result, code)
        return format_tool_error(RETRYABLE_SYNTAX, result, hint=hint)

    return result
