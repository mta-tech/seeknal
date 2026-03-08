"""Execute Python tool — run Python code for data analysis."""

import ast
import os
import tempfile
import threading
import traceback
from io import StringIO
from typing import Any

from langchain_core.tools import tool


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

    # Lazy-import data libraries
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
    """Core execution logic, testable without @tool decorator."""
    if not code or not code.strip():
        return "No code provided."

    namespace = _build_namespace(conn)

    # Split code for last-expression capture
    body_code, expr_code = _split_last_expression(code)

    # Execute with timeout — use per-thread StringIO instead of
    # reassigning sys.stdout (which is process-global and not thread-safe).
    result_holder: dict[str, Any] = {
        "value": None, "stdout": "", "error": None, "plots": [],
    }

    def target():
        captured = StringIO()
        # Override print in namespace to capture output without touching sys.stdout
        namespace["print"] = lambda *a, **kw: print(*a, file=captured, **kw)
        try:
            if isinstance(body_code, str):
                # Fallback: couldn't split, exec everything
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

    # Format output
    parts: list[str] = []

    if result_holder["stdout"]:
        parts.append(result_holder["stdout"].rstrip())

    if result_holder["error"]:
        parts.append(f"Error:\n{result_holder['error']}")
    elif result_holder["value"] is not None:
        val = result_holder["value"]
        val_str = repr(val) if not isinstance(val, str) else val
        # Truncate very large outputs
        if len(val_str) > 5000:
            val_str = val_str[:5000] + "\n... (truncated)"
        parts.append(val_str)

    if result_holder["plots"]:
        plot_list = "\n".join(f"- {p}" for p in result_holder["plots"])
        parts.append(f"Plots saved:\n{plot_list}")

    return "\n\n".join(parts) if parts else "Code executed successfully (no output)."


@tool
def execute_python(code: str) -> str:
    """Execute Python code for data analysis beyond what SQL can express.

    Use this for statistical modeling, visualization, pandas operations,
    and custom algorithms. Pre-loaded variables:
    - conn: DuckDB connection (query with conn.sql('SELECT ...').df())
    - pd: pandas
    - np: numpy
    - plt: matplotlib.pyplot

    Prefer execute_sql for simple data queries. Use this tool only when
    SQL cannot express the analysis (e.g., correlations, histograms,
    statistical tests, complex pandas operations).

    The last expression's value is captured and returned (like Jupyter).
    Use print() for intermediate output.

    Args:
        code: Python code to execute. Multi-line supported.
    """
    from seeknal.ask.agents.tools._context import get_tool_context

    ctx = get_tool_context()
    return _do_execute(code, ctx.repl.conn)
