"""Execute Python via uv with optional PEP 723 dependencies."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any

from seeknal.ask.agents.tools._context import get_tool_context


def _find_seeknal_src() -> str:
    import seeknal

    return str(Path(seeknal.__file__).parent.parent)


def _normalize_deps(deps: list[str] | None) -> list[str]:
    seen: set[str] = set()
    normalized: list[str] = []
    for dep in deps or []:
        value = dep.strip()
        if not value or value in seen:
            continue
        normalized.append(value)
        seen.add(value)
    return normalized


def _pep723_header(deps: list[str]) -> str:
    lines = [
        "# /// script",
        '# requires-python = ">=3.11"',
        "# dependencies = [",
    ]
    for dep in deps:
        lines.append(f'#     "{dep}",')
    lines.extend(["# ]", "# ///", ""])
    return "\n".join(lines)


_RUNNER_TEMPLATE = """\
{pep723_header}import ast
import json
import os
import sys
import traceback
from io import StringIO
from pathlib import Path

sys.path.insert(0, {seeknal_src!r})
sys.path.insert(0, {project_path!r})


def load_project_data(project_path: str):
    import duckdb

    conn = duckdb.connect(":memory:")
    root = Path(project_path)
    intermediate = root / "target" / "intermediate"
    if intermediate.exists():
        for pq in intermediate.rglob("*.parquet"):
            safe_path = str(pq.resolve()).replace("'", "''")
            try:
                conn.execute(
                    f'CREATE VIEW "{{pq.stem}}" AS SELECT * FROM read_parquet(\'{{safe_path}}\')'
                )
            except Exception:
                pass
    return conn


def split_last_expression(code: str):
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
    body_code = None
    if body:
        body_module = ast.Module(body=body, type_ignores=[])
        ast.fix_missing_locations(body_module)
        body_code = compile(body_module, "<agent>", "exec")

    expr_node = ast.Expression(body=last_node.value)
    ast.fix_missing_locations(expr_node)
    expr_code = compile(expr_node, "<agent>", "eval")
    return body_code, expr_code


def capture_plots(plot_dir: str):
    try:
        import matplotlib.pyplot as plt
    except Exception:
        return []
    paths = []
    for i, fig_num in enumerate(plt.get_fignums()):
        fig = plt.figure(fig_num)
        path = os.path.join(plot_dir, f"plot_{{i}}.png")
        fig.savefig(path, dpi=150, bbox_inches="tight", facecolor="white")
        paths.append(path)
    plt.close("all")
    return paths


plot_dir = {plot_dir!r}
os.makedirs(plot_dir, exist_ok=True)
agent_code = {code!r}
conn = load_project_data({project_path!r})

try:
    import pandas as pd
except Exception:
    pd = None

try:
    import numpy as np
except Exception:
    np = None

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except Exception:
    matplotlib = None
    plt = None

namespace = {{
    "conn": conn,
    "pd": pd,
    "np": np,
    "plt": plt,
    "matplotlib": matplotlib,
}}

captured = StringIO()
namespace["print"] = lambda *a, **kw: print(*a, file=captured, **kw)
value = None
error = None

body_code, expr_code = split_last_expression(agent_code)
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

plots = capture_plots(plot_dir)
result = {{
    "stdout": captured.getvalue(),
    "value": value if isinstance(value, (str, int, float, bool, type(None), list, dict)) else repr(value),
    "error": error,
    "plots": plots,
}}
json.dump(result, sys.stdout)
"""


def _format_result(payload: dict[str, Any]) -> str:
    parts: list[str] = []
    if payload.get("stdout"):
        parts.append(str(payload["stdout"]).rstrip())
    if payload.get("error"):
        parts.append(f"Error:\n{payload['error']}")
    elif payload.get("value") not in (None, ""):
        parts.append(str(payload["value"]))
    if payload.get("plots"):
        parts.append("Plots saved:\n" + "\n".join(f"- {p}" for p in payload["plots"]))
    return "\n\n".join(parts) if parts else "Code executed successfully (no output)."


async def execute_uv_script(code: str, deps: list[str] | None = None) -> str:
    """Run Python via ``uv run`` with optional PEP 723 dependencies.

    The script receives:
    - ``conn``: DuckDB connection with project parquet views registered
    - ``pd`` / ``np`` / ``plt`` when available
    - Jupyter-style last-expression capture
    """
    if not code.strip():
        return "No code provided."
    if shutil.which("uv") is None:
        return "uv is required for execute_uv_script but was not found on PATH."

    ctx = get_tool_context()
    project_path = ctx.project_path.resolve()
    session_id = ctx.session_id
    deps = _normalize_deps(deps)

    base_dir = project_path / "target" / f".ask_uvscript_{session_id}"
    base_dir.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(dir=base_dir) as tmpdir:
        tmpdir_path = Path(tmpdir)
        plot_dir = tmpdir_path / "plots"
        script_path = tmpdir_path / "runner.py"
        script_path.write_text(
            _RUNNER_TEMPLATE.format(
                pep723_header=_pep723_header(deps),
                seeknal_src=_find_seeknal_src(),
                project_path=str(project_path),
                plot_dir=str(plot_dir),
                code=code,
            )
        )

        try:
            result = subprocess.run(
                ["uv", "run", str(script_path)],
                cwd=str(project_path),
                capture_output=True,
                text=True,
                timeout=120,
                env=os.environ.copy(),
            )
        except subprocess.TimeoutExpired:
            return "Execution timed out after 120 seconds."

        if result.returncode != 0:
            stderr = result.stderr.strip() or result.stdout.strip()
            return f"uv script failed (exit {result.returncode}):\n{stderr}"

        try:
            payload = json.loads(result.stdout)
        except json.JSONDecodeError:
            return result.stdout.strip() or "uv script executed but returned no structured output."

        return _format_result(payload)

