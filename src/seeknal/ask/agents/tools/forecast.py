"""run_forecast — deterministic forecast trigger tool.

Thin trigger that mirrors ``request_clarification`` (SEEK5): the agent
constructs a 2-column SQL from project context, and this tool owns the
deterministic EXECUTE → VALIDATE → INFER → POST → FORMAT pipeline. The IBA
forecast engine (FC2b) owns the AutoETS compute. The LLM does no forecast
arithmetic.

The tool is domain-neutral (``AGENTS.md``: *"Never hardcode customer/domain
SQL in the agent harness"*). All domain specifics live in project context and
arrive as a single ``sql`` argument the agent assembles.
"""

from __future__ import annotations

import os
import re
from datetime import datetime
from typing import Any

import httpx

_IBA_FORECAST_URL = os.environ.get("IBA_FORECAST_URL", "http://iba-forecast:6705")
_IBA_FORECAST_API_KEY = os.environ.get("IBA_FORECAST_API_KEY", "")

_MIN_ROWS = 10          # tool sendability floor (engine eligibility ≥24 is separate)
_MAX_HORIZON = 12       # hard cap; engine also enforces periods ≤ 12
_FORECAST_PULL_LIMIT = 500  # well under execute_sql's row/byte budget

# ── SQL shape policy (structural, not error-text-based) ────────────────
# The forecast tool accepts a flat single-table aggregate only. Detection is
# regex-based on SQL keywords and independent of any runtime error message,
# so it remains stable across DuckDB versions and error-wording changes.
# Each entry maps a forbidden pattern to an actionable, OpenAI-style English
# reason that explains WHY the pattern violates the contract and WHAT to do.
_FORBIDDEN_SQL_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (
        re.compile(r"\bJOIN\b", re.IGNORECASE),
        "JOIN clauses are not supported. Query a single table and aggregate "
        "to one row per period — no year-over-year self-joins, no date-spine "
        "joins, no joins to other tables.",
    ),
    (
        re.compile(r"\bGENERATE_SERIES\b", re.IGNORECASE),
        "GENERATE_SERIES is not supported. The forecast engine counts missing "
        "periods (gap_months) internally from the X-column spacing — do not "
        "pre-fill missing months in SQL.",
    ),
    (
        re.compile(r"\bWITH\s+RECURSIVE\b", re.IGNORECASE),
        "Recursive CTEs are not supported. Use a single flat SELECT.",
    ),
)


def run_forecast(sql: str, periods: int = 3) -> str:
    """Run a deterministic forecast via the IBA forecast engine.

    Executes the caller-supplied SQL (which MUST return exactly two columns:
    X = a time-grain timestamp, Y = a numeric value), validates the shape,
    infers the frequency from the X-column spacing, sends the series to the
    IBA forecast engine for AutoETS computation, and returns the result as a
    7-blok structured markdown answer.

    Construct the SQL from project context (``context/forecast_guide.md``):
    the first column is the time grain (e.g. ``date_trunc('month', ...)``),
    the second is the aggregate (e.g. ``COUNT(DISTINCT ...)``). Do NOT do
    forecast arithmetic yourself, and do NOT pass domain column names as
    separate tool arguments — they belong inside the SQL.

    Args:
        sql: A SELECT that returns exactly two columns: ``[x, y]`` where
            ``x`` is a timestamp/DATE and ``y`` is a non-negative number.
            Must be ordered by ``x`` ascending and aggregated to one row
            per period. Example shape::

                SELECT date_trunc('month', some_date::timestamp) AS x,
                       COUNT(DISTINCT some_id)                AS y
                FROM schema.table
                WHERE some_date::timestamp >= '2022-09-01'
                GROUP BY 1
                ORDER BY 1

        periods: Steps ahead to forecast (default 3). Clamped to a max of 12
            (matching the engine's hard cap).

    Returns:
        A 7-blok markdown string: Ringkasan, Kualitas Proyeksi, Kondisi Data,
        Historis & Proyeksi, Proyeksi Detail, Rentang Realistis, Tingkat
        Keyakinan, Metodologi.

        On local validation failure (wrong column count, <10 rows, irregular
        spacing) returns a ``## Kesalahan`` block describing the problem.
        If the engine refuses (insufficient history, event-driven series),
        returns a single ``## Ditolak`` block with the reason.
    """
    # Late imports keep the module light and match request_clarification.
    from seeknal.ask.agents.tools._context import (
        get_tool_context,
        make_tool_signature,
        record_tool_result,
    )
    from seeknal.ask.agents.tools.execute_sql import (
        _execute_oneshot_with_timeout,
        _repair_common_sql_before_execution,
    )

    ctx = get_tool_context()
    periods = max(1, min(int(periods), _MAX_HORIZON))

    # ── STEP 0: PREPARE SQL — same pipeline as execute_sql (r3) ────────────
    # Strip trailing semicolons (LLMs include them, DuckDB rejects).
    sql = str(sql).strip().rstrip(";").strip()
    # Repair + rewrite: _rewrite_for_pg_pushdown converts EXTRACT(YEAR…) to
    # date-range filters so DuckDB's postgres_scanner pushes the WHERE to
    # PostgreSQL. Without this, DuckDB pulls the entire table and processes
    # locally — different cast/NULL semantics → wrong results.
    sql, _notices = _repair_common_sql_before_execution(sql)

    # ── STEP 0.5: STRUCTURAL POLICY CHECK (r4) ────────────────────────────
    # Fail fast on forbidden SQL patterns BEFORE calling DuckDB. This is
    # structural detection (regex on SQL keywords) — independent of DuckDB's
    # error wording, so it stays stable across upstream version bumps. The
    # observed production crash ("Conversion Error: TIMESTAMP -> TIMESTAMP[]"
    # on LEFT JOIN with date_trunc) is prevented here without coupling to the
    # exact error text. See _FORBIDDEN_SQL_PATTERNS for the policy table.
    forbidden = _detect_forbidden_patterns(sql)
    if forbidden:
        record_tool_result(
            "run_forecast",
            "error",
            args={"sql_sig": make_tool_signature("run_forecast", {"sql": sql})},
        )
        return _format_policy_violation(sql, forbidden)

    # ── STEP 1: EXECUTE the caller's SQL (no domain columns hardcoded) ─────
    # Wrap in try/except (r4) — DuckDB errors that slip past the structural
    # pre-check (syntax errors, missing tables, type casts not tied to JOIN)
    # must NOT crash the Temporal activity. Return a structured ``## Kesalahan``
    # block so the agent can self-correct in the next loop iteration (FC2a
    # §3.4: local validation failures must return a markdown block, not raise).
    try:
        columns, rows = _execute_oneshot_with_timeout(
            ctx, sql, limit=_FORECAST_PULL_LIMIT
        )
    except Exception as exc:
        record_tool_result(
            "run_forecast",
            "error",
            args={"sql_sig": make_tool_signature("run_forecast", {"sql": sql})},
        )
        return _format_sql_exec_error(exc, sql)

    # ── STEP 2: VALIDATE + INFER ───────────────────────────────────────────
    if len(columns) != 2:
        record_tool_result(
            "run_forecast",
            "error",
            args={"sql_sig": make_tool_signature("run_forecast", {"sql": sql})},
        )
        return _format_error(
            f"SQL harus mengembalikan tepat 2 kolom (X waktu, Y nilai); "
            f"dapat {len(columns)}."
        )
    if len(rows) < _MIN_ROWS:
        record_tool_result(
            "run_forecast",
            "error",
            args={"sql_sig": make_tool_signature("run_forecast", {"sql": sql})},
        )
        return _format_error(
            f"Data minimal {_MIN_ROWS} baris untuk dikirim ke engine; dapat {len(rows)}."
        )

    # Coerce data + filter out rows with NULL/empty X or unparseable Y.
    # DuckDB may return NULL for rows where a cast (e.g. tanggal_bayar::timestamp)
    # failed — those rows must be excluded, not silently converted to 0.
    data = [[_coerce_x(r[0]), _coerce_y(r[1])] for r in rows]
    data = [[x, y] for x, y in data if x is not None and y is not None]
    if len(data) < _MIN_ROWS:
        return _format_error(
            f"Data minimal {_MIN_ROWS} baris valid untuk dikirim ke engine; "
            f"dapat {len(data)} setelah filter NULL."
        )

    freq = _infer_freq([row[0] for row in data])
    if freq is None:
        record_tool_result(
            "run_forecast",
            "error",
            args={"sql_sig": make_tool_signature("run_forecast", {"sql": sql})},
        )
        return _format_error(
            "Spacing X tidak konsisten; tidak bisa menentukan freq (MS/QS/YS)."
        )

    # ── STEP 3: POST → engine (generic payload) ────────────────────────────
    try:
        resp = httpx.post(
            f"{_IBA_FORECAST_URL}/forecast",
            json={"data": data, "periods": periods, "freq": freq},
            headers={"X-API-Key": _IBA_FORECAST_API_KEY},
            timeout=120.0,
        )
        resp.raise_for_status()
        result: dict[str, Any] = resp.json()
    except httpx.HTTPStatusError as exc:
        record_tool_result(
            "run_forecast",
            "error",
            args={"sql_sig": make_tool_signature("run_forecast", {"sql": sql})},
        )
        return _format_error(f"Engine error: HTTP {exc.response.status_code}.")
    except (httpx.RequestError, httpx.HTTPError):
        record_tool_result(
            "run_forecast",
            "error",
            args={"sql_sig": make_tool_signature("run_forecast", {"sql": sql})},
        )
        return _format_error("Engine tidak tersedia (timeout atau koneksi gagal).")

    status = result.get("status")
    record_tool_result(
        "run_forecast",
        "ok" if status == "ok" else "refused",
        args={"sql_sig": make_tool_signature("run_forecast", {"sql": sql, "periods": periods})},
    )

    if status != "ok":
        return _format_refused(result.get("reason", "Ditolak engine."))

    # ── STEP 4: FORMAT 7-blok markdown ─────────────────────────────────────
    return _format_forecast_markdown(result, data)


# ── helpers ─────────────────────────────────────────────────────────────────


def _infer_freq(x_values: list[str]) -> str | None:
    """Infer MS/QS/YS from X spacing. Return None on irregular gaps.

    Compares consecutive diffs in months. MS → all diffs ≈ 1 month;
    QS → ≈ 3 months; YS → ≈ 12 months. Anything outside the three bands
    (or mixed) → None (caller refuses locally; do not send garbage).
    """
    if len(x_values) < 2:
        return None
    try:
        dates = [datetime.fromisoformat(str(v)[:10]) for v in x_values]
    except (ValueError, TypeError):
        return None

    month_deltas: list[int] = []
    for a, b in zip(dates[:-1], dates[1:]):
        delta = (b.year - a.year) * 12 + (b.month - a.month)
        if delta <= 0:
            return None  # unsorted or duplicate period
        month_deltas.append(delta)

    unique = set(month_deltas)
    if unique == {1}:
        return "MS"
    if unique == {3}:
        return "QS"
    if unique == {12}:
        return "YS"
    return None


def _coerce_x(v: Any) -> str | None:
    """Normalise the X column to a YYYY-MM-DD string the engine can parse.

    Returns None for NULL/empty values so the caller can filter them out
    before sending to the engine (NULL dates from failed casts must be
    excluded, not silently converted to empty strings).
    """
    from datetime import date as _date

    if v is None:
        return None
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d")
    if isinstance(v, _date):  # DuckDB DATE type (not datetime.datetime)
        return v.isoformat()
    text = str(v)[:10]
    return text if text.strip() else None


def _coerce_y(v: Any) -> float | None:
    """Normalise the Y column to a non-negative number.

    Returns None for unparseable values so the caller can filter them out.
    """
    try:
        n = float(v)
    except (TypeError, ValueError):
        return None
    return max(0.0, n)


def _format_error(message: str) -> str:
    return f"## Kesalahan\n\n{message}"


def _detect_forbidden_patterns(sql: str) -> list[str]:
    """Return actionable reasons for each forbidden SQL pattern found.

    Detection is structural (regex on SQL keywords) and independent of any
    runtime error message, so it stays stable across DuckDB versions and
    error-wording changes. Returns a list of OpenAI-style English reasons
    (already bullet-prefixed) explaining why each matched pattern violates
    the forecast tool's flat 2-column contract.
    """
    return [
        f"- {reason}"
        for pattern, reason in _FORBIDDEN_SQL_PATTERNS
        if pattern.search(sql)
    ]


def _sql_preview(sql: str, limit: int = 200) -> str:
    """Truncate and flatten a SQL string for inclusion in error context.

    Whitespace is collapsed so multi-line SQL renders on one preview line,
    keeping the agent context budget bounded while still giving enough signal
    for debugging.
    """
    preview = sql[:limit].replace("\n", " ")
    if len(sql) > limit:
        preview += " ... (truncated)"
    return preview


def _format_policy_violation(sql: str, violations: list[str]) -> str:
    """Format a structural policy violation (failed BEFORE execution).

    Used when ``_detect_forbidden_patterns`` finds JOIN / generate_series /
    recursive CTE patterns in the SQL. Distinct from ``_format_sql_exec_error``
    (which handles runtime exceptions) because the diagnosis here is certain:
    we know which pattern is forbidden, so the message is authoritative rather
    than hedged.
    """
    return (
        "## Kesalahan\n\n"
        "**Forecast SQL rejected by policy check (STEP 0.5).**\n\n"
        "The SQL contains patterns that the forecast tool does not support:\n"
        f"{chr(10).join(violations)}\n\n"
        "Required SQL shape for run_forecast:\n"
        "```\n"
        "SELECT <time_grain_expr>      AS x,   -- e.g. date_trunc('month', col::timestamp)\n"
        "       <value_aggregate_expr> AS y    -- e.g. COUNT(DISTINCT id)\n"
        "FROM   <table>\n"
        "WHERE  <filters>\n"
        "GROUP  BY 1\n"
        "ORDER  BY 1\n"
        "```\n"
        "- Exactly 2 columns (X timestamp, Y non-negative number).\n"
        "- No JOINs, no generate_series, no recursive CTE.\n"
        "- One row per period; the engine handles gap-filling and eligibility.\n\n"
        f"SQL preview (after preprocessing): `{_sql_preview(sql)}`"
    )


def _format_sql_exec_error(exc: Exception, sql: str) -> str:
    """Format a SQL execution failure as a traceable, structured Kesalahan block.

    The message is English and separates three concerns for debuggability:

    1. **What happened** — the exception class and its first non-empty lines
       (verbatim, truncated to keep the agent context budget bounded). This is
       the debug trail and is reported as-is, never parsed for branching.
    2. **Why the SQL likely violates the tool contract** — detected
       structurally from the SQL itself, independent of the runtime error
       wording. Useful when DuckDB's error is opaque (e.g. "Conversion Error"
       with no hint about JOIN being the cause).
    3. **What to do** — the required flat 2-column shape.

    This separation keeps the formatter robust: even if DuckDB rephrases its
    errors or the SQL fails for an unanticipated reason, the structural hints
    still surface the most likely policy violation, and the verbatim exception
    still gives the debug trail. Detection logic never branches on error text.
    """
    exc_type = type(exc).__name__
    # First 3 non-empty lines — enough to capture DuckDB's ``LINE N: ...`` caret
    # without echoing a multi-KB traceback into the agent context.
    raw_lines = [ln.strip() for ln in str(exc).splitlines() if ln.strip()]
    detail = "\n".join(raw_lines[:3]) if raw_lines else str(exc)

    # Re-run structural detection for the hint section. The same patterns are
    # checked at STEP 0.5, but a SQL can still reach this handler if it failed
    # the pre-check (it shouldn't) or if a non-patterned error occurred.
    violations = _detect_forbidden_patterns(sql)
    if violations:
        cause = (
            "The SQL also contains patterns that the forecast tool does not "
            "support:\n" + "\n".join(violations)
        )
    else:
        cause = (
            "No unsupported pattern was detected in the SQL. The failure is "
            "likely a syntax error, a missing table or column, or a type cast "
            "issue. Re-check the SQL against the table schema."
        )

    return (
        "## Kesalahan\n\n"
        "**Forecast SQL execution failed (STEP 1: EXECUTE).**\n\n"
        f"Error type: `{exc_type}`\n"
        f"Detail:\n```\n{detail}\n```\n\n"
        f"{cause}\n\n"
        "Required SQL shape for run_forecast:\n"
        "```\n"
        "SELECT <time_grain_expr>      AS x,   -- e.g. date_trunc('month', col::timestamp)\n"
        "       <value_aggregate_expr> AS y    -- e.g. COUNT(DISTINCT id)\n"
        "FROM   <table>\n"
        "WHERE  <filters>\n"
        "GROUP  BY 1\n"
        "ORDER  BY 1\n"
        "```\n"
        "- Exactly 2 columns (X timestamp, Y non-negative number).\n"
        "- No JOINs, no generate_series, no recursive CTE.\n"
        "- One row per period; the engine handles gap-filling and eligibility.\n\n"
        f"SQL preview (after preprocessing): `{_sql_preview(sql)}`"
    )


def _format_refused(reason: str) -> str:
    return (
        "## Ditolak\n\n"
        f"{reason}\n\n"
        "Tidak bisa membuat proyeksi untuk deret ini. "
        "Tampilkan analisis historis sebagai gantinya."
    )


def _format_forecast_markdown(result: dict[str, Any], history: list[list[Any]]) -> str:
    forecast = result.get("forecast") or {}
    assessment = result.get("assessment") or {}
    provenance = result.get("provenance") or {}

    points = forecast.get("points") or []
    metrics = assessment.get("metrics") or {}
    elig = assessment.get("eligibility") or {}

    # History → date→value lookup for year-over-year side-by-side.
    actual_by_x = {str(x)[:10]: y for x, y in history}

    def _prior_year(period: str) -> str | None:
        try:
            return str(int(period[:4]) - 1) + period[4:]
        except (ValueError, IndexError):
            return None

    def _fmt(v: Any) -> str:
        try:
            return f"{int(v):,}"
        except (TypeError, ValueError):
            return "-"

    quality_label = assessment.get("quality_label", "-")
    mape = metrics.get("mape")
    mae = metrics.get("mae")
    mase = metrics.get("mase")

    # Historis & Proyeksi — YoY side-by-side, 80% bounds as separate columns.
    # The "Actual (thn lalu)" column shows the SAME CALENDAR PERIOD one year
    # before the forecast period (e.g. forecast 2026-07 → actual 2025-07).
    # Format: "Mon-YYYY: value" to avoid ambiguity about which year the
    # actual belongs to.
    yoy_lines = []
    for p in points:
        period = p.get("period", "")
        prior = _prior_year(period)
        actual = actual_by_x.get(prior) if prior else None
        # Show prior year explicitly: "Jul-2025: 6,804" not just "6,804"
        if actual is not None and prior:
            try:
                from datetime import datetime as _dt
                _dtobj = _dt.fromisoformat(prior)
                _mon = _dtobj.strftime("%b")  # e.g. "Jul"
                _yr = _dtobj.year
                actual_str = f"{_mon}-{_yr}: {_fmt(actual)}"
            except (ValueError, TypeError):
                actual_str = _fmt(actual)
        else:
            actual_str = "-"
        yoy_lines.append(
            f"| {period[:7]} | {actual_str} | {_fmt(p.get('point'))} | "
            f"{_fmt(p.get('upper_80'))} | {_fmt(p.get('lower_80'))} |"
        )

    # Proyeksi Detail — full 80% + 95% bounds per period.
    detail_lines = []
    for p in points:
        detail_lines.append(
            f"| {p.get('period', '')[:7]} | {_fmt(p.get('point'))} | "
            f"{_fmt(p.get('lower_80'))} | {_fmt(p.get('upper_80'))} | "
            f"{_fmt(p.get('lower_95'))} | {_fmt(p.get('upper_95'))} |"
        )

    yoy_table = (
        "\n".join(yoy_lines) if yoy_lines else "| - | - | - | - | - |"
    )
    detail_table = (
        "\n".join(detail_lines) if detail_lines else "| - | - | - | - | - | - |"
    )

    return f"""## Ringkasan
Forecast for the next {forecast.get('periods', '-')} periods using {provenance.get('model', '-')}.

## Kualitas Proyeksi
**{quality_label}** — MAPE {mape:.1f}% (avg error from 12-period backtest).
- {_quality_explain(quality_label)}
- MAE {(_fmt(mae) if mae is not None else '-')} (absolute error, in raw units) · MASE {(f'{mase:.3f}' if mase is not None else '-')} (<1 beats naive baseline)

## Kondisi Data
- Historical periods: {elig.get('n_months', '-')} (engine requires >= 24 to forecast)
- Average per period: {_fmt(elig.get('avg_monthly'))}
- Gap periods: {elig.get('gap_months', '-')} (missing periods in the series)
- Eligibility: {'passed' if elig.get('passed') else 'refused — ' + str(elig.get('reason', ''))}

## Historis & Proyeksi
Actual tahun sebelumnya vs forecast, side by side (80% bounds).
| Period  | Actual (thn lalu)  | Point  | Upper 80% | Lower 80% |
|---------|---------------------|--------|-----------|-----------|
{yoy_table}

## Proyeksi Detail
| Period | Point | Lower 80% | Upper 80% | Lower 95% | Upper 95% |
|---|---|---|---|---|---|
{detail_table}

## Rentang Realistis
Bounds widen with horizon (σ·√h). The Upper/Lower 80% columns in the
Historis & Proyeksi table use the 80% confidence band; Proyeksi Detail
adds the wider 95% band. Larger sigma (residual noise) = wider bounds.

## Tingkat Keyakinan
{quality_label} — {_confidence_explain(quality_label)}

## Metodologi
- Model: {provenance.get('model', '-')} → {provenance.get('sub_type', '-')} ({_subtype_explain(provenance.get('sub_type', ''))})
- Training window: {provenance.get('training_window', '-')} (the historical range that set the forecast level)
- Residual sigma: {(f'{provenance.get("sigma", 0):.1f}' if provenance.get('sigma') is not None else '-')} (drives the σ·√h bounds above)
- Methodology: deterministic AutoETS via statsforecast; the LLM does no forecast arithmetic.
"""


def _quality_explain(label: str) -> str:
    return {
        "BAIK": "trustworthy for planning",
        "CUKUP": "usable with caveats",
        "LEMAH": "rough estimate, hedge it",
        "TOLAK": "not reliable enough",
    }.get(label, "-")


def _confidence_explain(label: str) -> str:
    return {
        "BAIK": "MAPE ≤ 15% — high-confidence forecast.",
        "CUKUP": "MAPE 15–25% — directionally reliable, expect ±20% noise.",
        "LEMAH": "MAPE 25–35% — wide intervals, treat as a rough range.",
        "TOLAK": "MAPE > 35% — not reliable for planning.",
    }.get(label, "-")


def _subtype_explain(sub_type: str) -> str:
    if "ETS(A,N,N)" in str(sub_type):
        return "Simple Exponential Smoothing — level-only, correct for stationary non-seasonal data"
    return "AutoETS-selected flavor"
