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
from datetime import datetime
from typing import Any

import httpx

_IBA_FORECAST_URL = os.environ.get("IBA_FORECAST_URL", "http://iba-forecast:6705")
_IBA_FORECAST_API_KEY = os.environ.get("IBA_FORECAST_API_KEY", "")

_MIN_ROWS = 10          # tool sendability floor (engine eligibility ≥24 is separate)
_MAX_HORIZON = 12       # hard cap; engine also enforces periods ≤ 12
_FORECAST_PULL_LIMIT = 500  # well under execute_sql's row/byte budget


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
    from seeknal.ask.agents.tools.execute_sql import _execute_oneshot_with_timeout

    ctx = get_tool_context()
    periods = max(1, min(int(periods), _MAX_HORIZON))

    # ── STEP 1: EXECUTE the caller's SQL (no domain columns hardcoded) ─────
    columns, rows = _execute_oneshot_with_timeout(ctx, sql, limit=_FORECAST_PULL_LIMIT)

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

    data = [[_coerce_x(r[0]), _coerce_y(r[1])] for r in rows]
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


def _coerce_x(v: Any) -> str:
    """Normalise the X column to a YYYY-MM-DD string the engine can parse."""
    if v is None:
        return ""
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d")
    text = str(v)
    # DuckDB may return a date/datetime object or string; trim to date part.
    return text[:10]


def _coerce_y(v: Any) -> float:
    """Normalise the Y column to a non-negative number."""
    try:
        n = float(v)
    except (TypeError, ValueError):
        return 0.0
    return max(0.0, n)


def _format_error(message: str) -> str:
    return f"## Kesalahan\n\n{message}"


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
