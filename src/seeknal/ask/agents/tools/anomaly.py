"""detect_anomaly — anomaly awareness tool.

Sibling to run_forecast: same 2-column SQL contract, same execution
pipeline, same engine. Explains which periods in a series look unusual and
why -- it never removes data and never returns forecast numbers.

Two ways an anomaly explanation reaches the user: the agent calls this tool
directly for an anomaly-intent question, or run_forecast's own response
already carries an automatic explanation when its backtest MAPE is poor
(the engine attaches it server-side). This tool is for the standalone,
on-demand case.
"""

from __future__ import annotations

from typing import Any

import httpx

# No engine URL/API key here: connectivity is resolved once at session init
# (agent.py) and injected via ToolContext.iba_anomaly_url/iba_forecast_api_key
# -- this module never reads os.environ. See _context.py's ToolContext.

_MIN_ROWS = 3  # matches the engine's own floor for anomaly detection
_ANOMALY_PULL_LIMIT = 500


def detect_anomaly(sql: str) -> str:
    """Explain which periods in a time series look unusual, and why.

    Executes the caller-supplied SQL (which MUST return exactly two columns:
    X = a time-grain timestamp, Y = a numeric value), and sends the series
    to the IBA forecast engine's anomaly-detection endpoint. This tool NEVER
    removes data and NEVER computes a forecast -- for a forecast, use
    run_forecast instead. Use this when the user asks specifically about
    anomalies/outliers/pencilan in a series, or wants to understand why a
    forecast's accuracy is poor.

    Construct the SQL the same way as for run_forecast (see
    context/forecast_guide.md): the first column is the time grain, the
    second is the aggregate.

    Args:
        sql: A SELECT that returns exactly two columns: ``[x, y]`` where
            ``x`` is a timestamp/DATE and ``y`` is a non-negative number.
            Must be ordered by ``x`` ascending and aggregated to one row
            per period.

    Returns:
        A markdown block listing flagged periods (date, value, and a
        plain-language reason hint), or a note that no anomalies were
        found. On local validation failure (wrong column count, too few
        rows) returns a ``## Kesalahan`` block describing the problem.
    """
    from seeknal.ask.agents.tools._context import (
        ENGINE_NOT_CONFIGURED,
        get_tool_context,
        make_tool_signature,
        record_tool_result,
    )
    from seeknal.ask.agents.tools.execute_sql import (
        _execute_oneshot_with_timeout,
        _repair_common_sql_before_execution,
    )
    from seeknal.ask.agents.tools.forecast import (
        _coerce_x,
        _coerce_y,
        _detect_forbidden_patterns,
        _format_policy_violation,
        _format_sql_exec_error,
    )

    ctx = get_tool_context()
    if ctx.iba_anomaly_url is None:
        return ENGINE_NOT_CONFIGURED

    sql = str(sql).strip().rstrip(";").strip()
    sql, _notices = _repair_common_sql_before_execution(sql)

    forbidden = _detect_forbidden_patterns(sql)
    if forbidden:
        record_tool_result(
            "detect_anomaly", "error",
            args={"sql_sig": make_tool_signature("detect_anomaly", {"sql": sql})},
        )
        return _format_policy_violation(sql, forbidden)

    try:
        columns, rows = _execute_oneshot_with_timeout(
            ctx, sql, limit=_ANOMALY_PULL_LIMIT
        )
    except Exception as exc:
        record_tool_result(
            "detect_anomaly", "error",
            args={"sql_sig": make_tool_signature("detect_anomaly", {"sql": sql})},
        )
        return _format_sql_exec_error(exc, sql)

    if len(columns) != 2:
        record_tool_result(
            "detect_anomaly", "error",
            args={"sql_sig": make_tool_signature("detect_anomaly", {"sql": sql})},
        )
        return (
            f"## Kesalahan\n\nSQL harus mengembalikan tepat 2 kolom "
            f"(X waktu, Y nilai); dapat {len(columns)}."
        )
    if len(rows) < _MIN_ROWS:
        record_tool_result(
            "detect_anomaly", "error",
            args={"sql_sig": make_tool_signature("detect_anomaly", {"sql": sql})},
        )
        return f"## Kesalahan\n\nData minimal {_MIN_ROWS} baris untuk deteksi anomali; dapat {len(rows)}."

    data = [[_coerce_x(r[0]), _coerce_y(r[1])] for r in rows]
    data = [[x, y] for x, y in data if x is not None and y is not None]
    if len(data) < _MIN_ROWS:
        return (
            f"## Kesalahan\n\nData minimal {_MIN_ROWS} baris valid untuk deteksi "
            f"anomali; dapat {len(data)} setelah filter NULL."
        )

    try:
        resp = httpx.post(
            ctx.iba_anomaly_url,
            json={"data": data},
            headers={"X-API-Key": ctx.iba_forecast_api_key},
            timeout=60.0,
        )
        resp.raise_for_status()
        result: dict[str, Any] = resp.json()
    except httpx.HTTPStatusError as exc:
        record_tool_result(
            "detect_anomaly", "error",
            args={"sql_sig": make_tool_signature("detect_anomaly", {"sql": sql})},
        )
        return f"## Kesalahan\n\nEngine error: HTTP {exc.response.status_code}."
    except (httpx.RequestError, httpx.HTTPError):
        record_tool_result(
            "detect_anomaly", "error",
            args={"sql_sig": make_tool_signature("detect_anomaly", {"sql": sql})},
        )
        return "## Kesalahan\n\nEngine tidak tersedia (timeout atau koneksi gagal)."

    record_tool_result(
        "detect_anomaly", "ok",
        args={"sql_sig": make_tool_signature("detect_anomaly", {"sql": sql})},
    )
    return _format_anomaly_markdown(result.get("anomalies") or [])


def _format_anomaly_markdown(anomalies: list[dict[str, Any]]) -> str:
    if not anomalies:
        return (
            "## Anomali\n\n"
            "Tidak ada periode yang menonjol sebagai anomali pada data ini."
        )
    lines = ["## Anomali", "", "Periode yang menonjol dibanding pola umum data:"]
    for a in anomalies:
        lines.append(f"- **{a.get('date')}**: {a.get('value')} -- {a.get('reason_hint')}")
    lines.append("")
    lines.append(f"Terdeteksi {len(anomalies)} periode anomali.")
    return "\n".join(lines)
