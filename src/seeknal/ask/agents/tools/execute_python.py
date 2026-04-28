"""Execute Python tool — run Python code for data analysis.

Executes agent-generated Python in an isolated subprocess.
The subprocess gets its own DuckDB connection with project data registered
as views from parquet files — no access to the parent process.

Falls back to in-process threading if subprocess execution fails.
"""

import ast
import os
import re
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
        missing = _missing_module_from_error(result)
        if missing:
            return (
                f"`{missing}` is not installed in this Seeknal Python sandbox. "
                "Do not retry the same import. Continue with the pre-loaded "
                "libraries that are actually available, use SQL/pandas/numpy "
                "where possible, or provide a text/table answer when plotting "
                "is unavailable."
            )
        return (
            "The requested package is not installed in this Seeknal Python "
            "sandbox. Do NOT retry the same import; use SQL/pandas/numpy/"
            "scipy/sklearn if available, or provide a text/table answer."
        )
    if "'NoneType' object has no attribute" in result and re.search(r"\bplt\b|\bmatplotlib\b", code):
        return (
            "`plt`/`matplotlib` is not available in this Seeknal Python "
            "sandbox. Do not retry chart generation in this session; provide "
            "a text/table answer or use SQL/Python to compute non-visual "
            "statistics."
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


def _missing_module_from_error(result: str) -> str | None:
    """Extract the missing top-level module from a ModuleNotFoundError."""
    match = re.search(r"No module named ['\"]([^'\"]+)['\"]", result)
    if not match:
        return None
    return match.group(1).split(".")[0]


def _imported_top_level_modules(code: str) -> set[str]:
    """Return top-level module names imported by agent code."""
    try:
        tree = ast.parse(code)
    except SyntaxError:
        return set()

    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                modules.add(alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module.split(".")[0])
    return modules


async def execute_python(code: str) -> str:
    """Run Python in an isolated subprocess sandbox. Prefer execute_sql for simple queries.

    See the `execute-python-analysis` skill for the sandbox semantics (no
    cross-call persistence, limited package set), the available libs (conn,
    pd, np, plt, sklearn, scipy), the SQL-comment gotcha (`#` is not a DuckDB
    comment), plot capture, and error recovery.

    Args:
        code: Python code to execute. Last expression is captured Jupyter-style.
    """
    from seeknal.ask.agents.tools._context import (
        get_tool_context,
        record_tool_result,
        repeated_failure_message,
        visualization_requested,
    )
    from seeknal.ask.sandbox import async_execute_in_sandbox

    ctx = get_tool_context()
    prior_failure = repeated_failure_message("execute_python", {"code": code})

    # Wrap error results in structured error JSON for agent self-correction
    from seeknal.ask.agents.tools.errors import (
        TERMINAL_DEPENDENCY_UNAVAILABLE,
        RETRYABLE_SYNTAX,
        TERMINAL_TIMEOUT,
        format_tool_error,
    )

    if prior_failure:
        result = format_tool_error(
            TERMINAL_DEPENDENCY_UNAVAILABLE,
            prior_failure,
            hint=(
                "Do not retry the same Python path. Use SQL or a text/table "
                "answer from existing evidence."
            ),
        )
        record_tool_result("execute_python", result, args={"code": code})
        return result

    imported = _imported_top_level_modules(code)
    uses_plotting = bool({"matplotlib", "seaborn", "plotly"} & imported) or re.search(
        r"\bplt\.",
        code,
    )
    if uses_plotting and not visualization_requested(getattr(ctx, "current_question", None)):
        result = format_tool_error(
            TERMINAL_DEPENDENCY_UNAVAILABLE,
            "Visualization was not explicitly requested for this turn.",
            hint=(
                "Do not create charts by default for business trend questions. "
                "Use SQL/pandas for calculations and provide a text/table "
                "answer unless the user asks for a chart, plot, dashboard, or report."
            ),
        )
        record_tool_result("execute_python", result, args={"code": code})
        return result

    unavailable = getattr(ctx, "unavailable_python_modules", set())
    repeated_unavailable = sorted(imported & unavailable)
    if repeated_unavailable:
        module = repeated_unavailable[0]
        result = format_tool_error(
            TERMINAL_DEPENDENCY_UNAVAILABLE,
            f"Python module `{module}` is unavailable in this session.",
            hint=(
                f"`{module}` already failed to import. Do not retry the same "
                "library or plotting path; fall back to SQL/text tables or "
                "use another available pre-loaded library."
            ),
        )
        record_tool_result("execute_python", result, args={"code": code})
        return result

    result = await async_execute_in_sandbox(code, ctx.project_path)

    # Backgrounded results pass through directly
    from seeknal.ask.background import _BACKGROUNDED_PREFIX
    if result.startswith(_BACKGROUNDED_PREFIX):
        record_tool_result("execute_python", result, args={"code": code})
        return result

    missing_module = _missing_module_from_error(result)
    if missing_module:
        unavailable.add(missing_module)
        ctx.unavailable_python_modules = unavailable
        result = format_tool_error(
            TERMINAL_DEPENDENCY_UNAVAILABLE,
            result,
            hint=_infer_error_hint(result, code),
        )
        record_tool_result("execute_python", result, args={"code": code})
        return result
    if "'NoneType' object has no attribute" in result and re.search(r"\bplt\b|\bmatplotlib\b", code):
        unavailable.add("matplotlib")
        ctx.unavailable_python_modules = unavailable
        result = format_tool_error(
            TERMINAL_DEPENDENCY_UNAVAILABLE,
            result,
            hint=_infer_error_hint(result, code),
        )
        record_tool_result("execute_python", result, args={"code": code})
        return result

    if result.startswith("Execution timed out"):
        result = format_tool_error(TERMINAL_TIMEOUT, result)
        record_tool_result("execute_python", result, args={"code": code})
        return result
    if result.startswith("Error launching subprocess:"):
        result = format_tool_error(RETRYABLE_SYNTAX, result)
        record_tool_result("execute_python", result, args={"code": code})
        return result
    if result.startswith("Error:\n") or result.startswith("Process exited with code"):
        hint = _infer_error_hint(result, code)
        result = format_tool_error(RETRYABLE_SYNTAX, result, hint=hint)
        record_tool_result("execute_python", result, args={"code": code})
        return result

    record_tool_result("execute_python", result, args={"code": code})
    return result
