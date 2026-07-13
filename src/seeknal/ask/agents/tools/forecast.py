"""run_forecast — deterministic forecast trigger tool.

Thin trigger that mirrors ``request_clarification`` (SEEK5): the agent
constructs a 2-column SQL from project context, and this tool owns the
deterministic EXECUTE → VALIDATE → INFER → POST → FORMAT pipeline. The IBA
forecast engine (FC2b) owns the seasonal-ETS compute (Holt-Winters, single
deterministic fit per request). The LLM does no forecast arithmetic.

The tool is domain-neutral (``AGENTS.md``: *"Never hardcode customer/domain
SQL in the agent harness"*). All domain specifics live in project context and
arrive as a single ``sql`` argument the agent assembles.
"""

from __future__ import annotations

import logging
import re
from collections import Counter
from datetime import datetime, timezone
from typing import Any

import httpx

logger = logging.getLogger(__name__)

# No engine URL/API key here: connectivity is resolved once at session init
# (agent.py) and injected via ToolContext.iba_engine_url/iba_engine_api_key
# -- this module never reads os.environ. See _context.py's ToolContext.

_MIN_ROWS = 10          # tool sendability floor, matches the engine's own eligibility floor
_MAX_HORIZON = 36       # grain-aware cap (e.g. "3 tahun" on monthly data = 36 steps); engine mirrors this
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
    IBA forecast engine for a seasonal-ETS computation, and returns the
    result as a 7-blok structured markdown answer.

    Construct the SQL from project context (``context/forecast_guide.md``):
    the first column is the time grain (e.g. ``date_trunc('month', ...)``),
    the second is the aggregate (e.g. ``COUNT(DISTINCT ...)``). Do NOT do
    forecast arithmetic yourself, and do NOT pass domain column names as
    separate tool arguments — they belong inside the SQL.

    ``periods`` is a step count on whatever grain the SQL produces, not a
    fixed unit. To honor a request in months/years, convert it to steps on
    the data's grain yourself (e.g. "3 tahun" on a monthly series = 36).

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

        periods: Steps ahead to forecast (default 3), on the grain the SQL
            produces. Clamped to a max of 36.

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
        ENGINE_NOT_CONFIGURED,
        get_tool_context,
        make_tool_signature,
        record_tool_result,
    )
    from seeknal.ask.agents.tools.execute_sql import (
        _execute_oneshot_with_timeout,
        _repair_common_sql_before_execution,
    )

    ctx = get_tool_context()
    if ctx.iba_engine_url is None:
        return ENGINE_NOT_CONFIGURED

    periods = max(1, min(int(periods), _MAX_HORIZON))

    # ── STEP 0: PREPARE SQL — same pipeline as execute_sql (r3) ────────────
    # Strip trailing semicolons (LLMs include them, DuckDB rejects).
    sql = str(sql).strip().rstrip(";").strip()
    # Repair + rewrite: _rewrite_for_pg_pushdown converts EXTRACT(YEAR…) to
    # date-range filters so DuckDB's postgres_scanner pushes the WHERE to
    # PostgreSQL. Without this, DuckDB pulls the entire table and processes
    # locally — different cast/NULL semantics → wrong results.
    sql, _notices = _repair_common_sql_before_execution(sql)

    # ── STEP 0.5: STRUCTURAL POLICY CHECK ──────────────────────────────────
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
    # Wrap in try/except — DuckDB errors that slip past the structural
    # pre-check (syntax errors, missing tables, type casts not tied to JOIN)
    # must NOT crash the Temporal activity. Return a structured ``## Kesalahan``
    # block so the agent can self-correct in the next loop iteration
    # (local validation failures must return a markdown block, not raise).
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
            f"{ctx.iba_engine_url}/forecast",
            json={"data": data, "periods": periods, "freq": freq},
            headers={"X-API-Key": ctx.iba_engine_api_key},
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

    # ── STEP 4: FORMAT structured markdown ────────────────────────────────
    markdown = _format_forecast_markdown(result, data, lang="id")

    # ── STEP 5: self-upload projection points ───────────────────────────────
    # Forecast points are computed here, not SQL-queryable, so they can't
    # ride execute_sql's export path. This tool exports them itself,
    # deterministically, reading straight from `result` (the engine's JSON
    # response, see STEP 3) rather than depending on the agent remembering
    # to call upload_to_s3 -- every successful forecast gets a Download
    # button, with no dependence on agent behavior.
    _upload_forecast_points(sql, result)

    return markdown


# ── helpers ─────────────────────────────────────────────────────────────────


def _infer_freq(x_values: list[str]) -> str | None:
    """Infer MS/QS/YS from the dominant X spacing. Tolerates gaps.

    Computes the month-delta between each consecutive pair, takes the most
    common delta as the base grain, and accepts the series as long as every
    delta is a positive integer multiple of that base (a gap -- e.g. one
    missing month in an otherwise-monthly series -- is just a multiple of
    the base grain, not a different grain). Only returns None when the base
    isn't month/quarter/year-aligned, or a delta genuinely doesn't fit that
    base at all (mixed granularity within one series).
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

    base = Counter(month_deltas).most_common(1)[0][0]
    if base not in (1, 3, 12):
        return None
    if any(d % base != 0 for d in month_deltas):
        return None

    return {1: "MS", 3: "QS", 12: "YS"}[base]


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


def _upload_forecast_points(sql: str, result: dict[str, Any]) -> None:
    """Best-effort export of the forecast projection points to CSV.

    Mirrors ``upload_to_s3`` Mode 2 (data=/columns=). Failures are swallowed
    -- an export hiccup must never break the forecast answer that already
    computed successfully. Lazy import avoids an import cycle (upload_to_s3
    pulls in execute_sql internals at call time).
    """
    points = (result.get("forecast") or {}).get("points") or []
    if not points:
        return
    columns = ["period", "point", "lower_80", "upper_80", "lower_95", "upper_95", "p10", "p90"]
    rows = [
        [p.get("period"), p.get("point"), p.get("lower_80"), p.get("upper_80"),
         p.get("lower_95"), p.get("upper_95"), p.get("p10"), p.get("p90")]
        for p in points
    ]
    filename = _derive_forecast_filename(sql)
    try:
        from seeknal.ask.agents.tools.upload_to_s3 import upload_to_s3

        upload_to_s3(filename, data=rows, columns=columns)
    except Exception:
        logger.exception("[run_forecast] projection-points auto-upload failed; answer unaffected")


def _derive_forecast_filename(sql: str) -> str:
    """Best-effort CSV filename derived from the forecast SQL's source table.

    Falls back to a generic timestamped name when introspection fails --
    matches the pattern in hooks.py's ``_derive_filename_from_sql``. The
    object key is namespaced under ``csv-exports/{uuid}/{filename}`` by
    upload_to_s3, so collisions are impossible regardless of this name.
    """
    table = "forecast"
    try:
        m = re.search(r"\bFROM\s+([\w\.]+)", sql, re.IGNORECASE)
        if m:
            table_full = m.group(1).strip("`\"'")
            table = table_full.split(".")[-1].lower()
            table = re.sub(r"^(t_|tbl_|table_|ft_)", "", table) or "forecast"
    except Exception:
        table = "forecast"
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    return f"forecast-{table}-{ts}.csv"


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


def _lang_key(lang: str) -> str:
    """Normalise any language code to the two supported keys: 'id' or 'en'."""
    return "en" if lang.startswith("en") else "id"


def _translator(lang: str):
    """Return a ``_t(id_text, en_text)`` closure for one-off inline phrases."""
    is_en = lang.startswith("en")

    def _t(id_text: str, en_text: str) -> str:
        return en_text if is_en else id_text

    return _t


def _fmt_number(v: Any) -> str:
    """Format a number with thousands separator. Returns '0' for None/missing."""
    try:
        return f"{int(round(float(v))):,}"
    except (TypeError, ValueError):
        return "0"


def _avg_y(rows: list[list[Any]]) -> float:
    vals = [float(r[1]) for r in rows if r and r[1] is not None]
    return sum(vals) / len(vals) if vals else 0.0


# Small lookup tables replace the five parallel id/en branch-per-function
# helpers this module used to have -- one table per concept, one shared
# lookup pattern, instead of five copies of the same if/else shape.

_QUALITY_SENTENCES = {
    "id": {
        "BAIK": "Proyeksi ini terbukti akurat pada data historis -- dapat diandalkan untuk perencanaan.",
        "CUKUP": "Proyeksi ini cukup dapat diandalkan, gunakan dengan sedikit ruang toleransi.",
        "LEMAH": "Proyeksi ini adalah perkiraan kasar -- gunakan dengan hati-hati.",
        "TOLAK": "Akurasi proyeksi ini rendah pada data historis -- gunakan dengan sangat hati-hati.",
        "UNKNOWN": "Akurasi historis proyeksi ini belum dapat diukur.",
        "_default": "Gunakan proyeksi ini dengan hati-hati.",
    },
    "en": {
        "BAIK": "This projection has proven accurate on historical data -- reliable for planning.",
        "CUKUP": "This projection is reasonably reliable, use with some tolerance.",
        "LEMAH": "This projection is a rough estimate -- use with caution.",
        "TOLAK": "This projection has low historical accuracy -- use with great caution.",
        "UNKNOWN": "Historical accuracy of this projection has not yet been measured.",
        "_default": "Use this projection with caution.",
    },
}

_CONFIDENCE_LABELS = {
    "id": {"BAIK": "Tinggi", "CUKUP": "Sedang", "LEMAH": "Rendah", "TOLAK": "Rendah", "UNKNOWN": "Sedang", "_default": "Sedang"},
    "en": {"BAIK": "High", "CUKUP": "Medium", "LEMAH": "Low", "TOLAK": "Low", "UNKNOWN": "Medium", "_default": "Medium"},
}

_CONFIDENCE_SENTENCES = {
    "id": {
        "Tinggi": "Tingkat keyakinan tinggi -- proyeksi didukung akurasi historis yang baik.",
        "Sedang": "Tingkat keyakinan sedang -- proyeksi cukup dapat digunakan dengan catatan.",
        "Rendah": "Tingkat keyakinan rendah -- sebaiknya gunakan proyeksi ini secara hati-hati.",
        "_default": "Gunakan proyeksi ini dengan hati-hati.",
    },
    "en": {
        "High": "High confidence -- projection is supported by good historical accuracy.",
        "Medium": "Medium confidence -- projection is usable with caveats.",
        "Low": "Low confidence -- use this projection with caution.",
        "_default": "Use this projection with caution.",
    },
}

# Ordered (max_cv, id_label, en_label) thresholds -- first match wins.
_STABILITY_THRESHOLDS: tuple[tuple[float, str, str], ...] = (
    (0.3, "stabil", "stable"),
    (0.5, "cukup stabil", "moderately stable"),
    (0.8, "fluktuatif", "volatile"),
)
_STABILITY_FALLBACK = ("sangat fluktuatif", "highly volatile")

_DIRECTION_WORDS = {"id": {"up": "meningkat", "down": "menurun", "flat": "stabil"},
                     "en": {"up": "increasing", "down": "decreasing", "flat": "stable"}}


def _quality_sentence(label: str, lang: str = "id") -> str:
    """Plain-language quality assessment in the requested language."""
    table = _QUALITY_SENTENCES[_lang_key(lang)]
    return table.get(label, table["_default"])


def _confidence_label(quality_label: str, lang: str = "id") -> str:
    """Map MAPE-based quality label to a plain-language confidence level.

    Every horizon step draws from the same trailing window (with trend
    projection), so there is no meaningfully different confidence per step.
    """
    table = _CONFIDENCE_LABELS[_lang_key(lang)]
    return table.get(quality_label, table["_default"])


def _confidence_sentence(confidence: str, lang: str = "id") -> str:
    table = _CONFIDENCE_SENTENCES[_lang_key(lang)]
    return table.get(confidence, table["_default"])


def _stability_label(cv: float, lang: str = "id") -> str:
    """Plain-language data stability -- never prints the raw coefficient of variation."""
    key = _lang_key(lang)
    idx = 0 if key == "id" else 1
    for max_cv, *labels in _STABILITY_THRESHOLDS:
        if cv <= max_cv:
            return labels[idx]
    return _STABILITY_FALLBACK[idx]


def _direction_word(hist_avg: float, proj_avg: float, lang: str = "id") -> tuple[str, float]:
    """Direction + percentage change of the projection vs. recent history."""
    words = _DIRECTION_WORDS[_lang_key(lang)]
    if hist_avg == 0:
        return words["flat"], 0.0
    pct = (proj_avg - hist_avg) / hist_avg * 100
    if pct > 5:
        return words["up"], pct
    if pct < -5:
        return words["down"], pct
    return words["flat"], pct


# ── _format_forecast_markdown block builders ────────────────────────────
#
# Each function builds exactly one ``##`` block. ``_format_forecast_markdown``
# extracts the shared values once, then calls these in order and joins them
# -- so the top-level function reads as a plain pipeline instead of one long
# block of inline string-building.


def _build_summary_block(_t, periods: int, hist_avg: float, proj_avg: float,
                          direction: str, pct: float) -> str:
    return (
        f"## {_t('Ringkasan', 'Summary')}\n\n"
        f"{_t('Rata-rata', 'Average')} {periods} "
        f"{_t('periode terakhir', 'last periods')}: {_fmt_number(hist_avg)}\n"
        f"{_t('Rata-rata', 'Average')} {periods} "
        f"{_t('periode proyeksi', 'projection periods')}: {_fmt_number(proj_avg)}\n"
        f"{_t('Arah', 'Direction')}: {direction} ({pct:+.1f}%)"
    )


def _build_quality_block(_t, quality_label: str, mape_str: str, lang: str) -> str:
    return (
        f"## {_t('Kualitas Proyeksi', 'Projection Quality')}\n\n"
        f"**{quality_label}** ({_t('akurasi historis', 'historical accuracy')}: MAPE {mape_str})\n"
        f"{_quality_sentence(quality_label, lang)}"
    )


def _build_data_condition_block(_t, n_months: int, gap_months: int, gap_caveat: str | None,
                                 cv: float, anomalies: list[Any] | None, lang: str) -> str:
    gap_icon = "⚠" if gap_caveat else "✓"
    stability_icon = "✓" if cv <= 0.5 else "⚠"
    lines = [
        f"## {_t('Kondisi Data', 'Data Condition')}",
        "",
        f"- ✓ {_t('Riwayat data', 'Data history')}: {n_months} {_t('bulan', 'months')}",
        f"- {gap_icon} {_t('Kelengkapan data', 'Data completeness')}: "
        f"{n_months - gap_months}/{n_months} {_t('periode tersedia', 'periods available')}",
        f"- {stability_icon} {_t('Stabilitas data', 'Data stability')}: {_stability_label(cv, lang)}",
    ]
    if anomalies:
        lines.append(
            f"- ⚠ {_t('Terdapat', 'Found')} {len(anomalies)} "
            f"{_t('periode tidak biasa dalam riwayat', 'unusual periods in history')}"
        )
    return "\n".join(lines)


def _build_history_block(_t, history: list[list[Any]], points: list[dict]) -> str:
    hist_display = history[-6:] if len(history) > 6 else history
    timeline_rows = [f"| {str(x)[:7]} | {_fmt_number(y)} |" for x, y in hist_display]
    timeline_rows.append(f"| {_t('── sekarang ──', '── now ──')} | |")
    for p in points:
        period = str(p.get("period", ""))[:7]
        timeline_rows.append(
            f"| {period} ({_t('proyeksi', 'forecast')}) | {_fmt_number(p.get('point'))} |"
        )
    timeline_table = "\n".join(timeline_rows) if timeline_rows else "| 0 | 0 |"

    hist_vals = [float(y) for _x, y in hist_display if y is not None]
    hist_stats = (
        f"{_t('Rata-rata', 'Average')}: {_fmt_number(_avg_y(hist_display))}, "
        f"{_t('Tertinggi', 'Highest')}: {_fmt_number(max(hist_vals))}, "
        f"{_t('Terendah', 'Lowest')}: {_fmt_number(min(hist_vals))}"
        if hist_vals else ""
    )
    return (
        f"## {_t('Historis & Proyeksi', 'History & Projection')}\n\n"
        f"| {_t('Periode', 'Period')} | {_t('Nilai', 'Value')} |\n"
        f"|---|---|\n"
        f"{timeline_table}\n\n"
        f"{hist_stats}"
    )


def _build_detail_block(_t, points: list[dict], sub_type: str, sigma: float) -> str:
    # capped bounds: sigma_h = sigma * sqrt(min(h,2))
    # 80% interval: z = 1.2816
    detail_rows = [
        f"| {str(p.get('period', ''))[:7]} | {_fmt_number(p.get('point'))} | "
        f"{_fmt_number(p.get('lower_80'))} | {_fmt_number(p.get('upper_80'))} |"
        for p in points
    ]
    return (
        f"## {_t('Proyeksi Detail', 'Projection Detail')}\n\n"
        f"{_t('Metode', 'Method')}: "
        f"{_t('ETS musiman', 'seasonal ETS')} "
        f"({sub_type}), "
        f"{_t('bounds', 'bounds')}: sigma*sqrt(min(h,2)) (sigma={sigma:.0f})\n\n"
        f"| {_t('Periode', 'Period')} | {_t('Prediksi', 'Forecast')} | "
        f"{_t('Bawah', 'Lower')} 80% | {_t('Atas', 'Upper')} 80% |\n"
        f"|---|---|---|---|\n"
        f"{chr(10).join(detail_rows) if detail_rows else '| 0 | 0 | 0 | 0 |'}"
    )


def _build_range_block(_t, points: list[dict]) -> str:
    first = points[0] if points else {}
    last = points[-1] if points else {}
    return (
        f"## {_t('Rentang Proyeksi', 'Projection Range')}\n\n"
        f"{_t('Interval 80% awal', '80% interval start')}: "
        f"{_fmt_number(first.get('lower_80'))} s/d {_fmt_number(first.get('upper_80'))}\n"
        f"{_t('Interval 80% akhir', '80% interval end')}: "
        f"{_fmt_number(last.get('lower_80'))} s/d {_fmt_number(last.get('upper_80'))}\n\n"
        f"{_t('Interval melebar sesuai horizon', 'Interval widens with horizon')}: "
        f"sigma * sqrt(h). "
        f"{_t('Semakin jauh proyeksi, semakin besar ketidakpastian', 'The further the projection, the greater the uncertainty')}."
    )


def _build_confidence_block(_t, quality_label: str, lang: str) -> str:
    confidence = _confidence_label(quality_label, lang)
    return (
        f"## {_t('Tingkat Keyakinan', 'Confidence Level')}\n\n"
        f"**{confidence}**\n\n"
        f"{_confidence_sentence(confidence, lang)}"
    )


def _build_meaning_block(_t, direction: str, pct: float, periods: int,
                          gap_caveat: str | None, anomalies: list[Any] | None) -> str:
    lines = [
        f"## {_t('Apa Artinya', 'What It Means')}",
        "",
        f"- {_t('Proyeksi', 'Projection')} {direction} "
        f"{_t('dibanding rata-rata', 'compared to average')} "
        f"{periods} {_t('bulan terakhir', 'last months')} ({pct:+.1f}%)",
        f"- {_t('Gunakan interval 80% untuk perencanaan', 'Use the 80% interval for planning')}",
    ]
    if gap_caveat:
        lines.append(f"- {gap_caveat}")
    if anomalies:
        lines.append(
            f"- {_t('Riwayat data memiliki periode tidak biasa', 'History contains unusual periods')}"
        )
    return "\n".join(lines)


def _build_methodology_block(_t, sub_type: str, sigma: float, window_used: Any,
                              window_selection: dict[str, Any], mape_str: str,
                              training_window: str) -> str:
    lines = [
        f"## {_t('Metodologi', 'Methodology')}",
        "",
        f"{_t('Metode', 'Method')}: {_t('ETS musiman (Holt-Winters)', 'seasonal ETS (Holt-Winters)')}",
        f"{_t('Konfigurasi', 'Configuration')}: {sub_type}",
        f"{_t('Jendela', 'Window')}: {window_used} {_t('periode (dipilih otomatis', 'periods (auto-selected')}: {window_selection.get('state', 'unknown')})",
        f"Sigma: {sigma:.0f} ({_t('residual std fit', 'fit residual std')})",
        f"Bounds: sigma * sqrt(min(h,2)), z=1.2816 (80%)",
        f"{_t('Akurasi backtest', 'Backtest accuracy')}: MAPE {mape_str}",
        f"{_t('Cakupan data', 'Data range')}: {training_window}",
    ]
    if window_selection.get("state") in ("partial", "fail"):
        lines.append(f"{_t('Catatan', 'Note')}: {window_selection.get('reason', '')}")
    return "\n".join(lines)


def _build_anomaly_block(_t, anomalies: list[dict[str, Any]]) -> str:
    lines = [
        f"## {_t('Anomali', 'Anomalies')}",
        "",
        _t(
            "Periode berikut menonjol dibanding pola umum data:",
            "The following periods stand out from the general data pattern:",
        ),
    ]
    for a in anomalies:
        lines.append(f"- **{a.get('date')}**: {_fmt_number(a.get('value'))} -- {a.get('reason_hint')}")
    return "\n".join(lines)


def _format_forecast_markdown(result: dict[str, Any], history: list[list[Any]], lang: str = "id") -> str:
    """Structured forecast markdown — adaptive language (id/en).

    Blocks: Summary, Projection Quality, Data Condition,
    History & Projection, Projection Detail, Projection Range,
    Confidence Level, What It Means, Methodology (+ Anomaly if attached).

    This is a technical intermediate block. The agent reads this raw
    tool_result and composes its own final prose around it.

    Bounds widen with horizon but the growth is capped:
        sigma_h = sigma * sqrt(min(h, 2))
        lower = point - z * sigma_h   (z = 1.2816 for 80%)
        upper = point + z * sigma_h
    All values clamped to >= 0.

    Args:
        result: Engine response dict.
        history: Historical data [[x, y], ...].
        lang: Language code — "id" (Indonesian) or "en" (English).
    """
    forecast = result.get("forecast") or {}
    assessment = result.get("assessment") or {}
    provenance = result.get("provenance") or {}
    eligibility = assessment.get("eligibility") or {}

    points = forecast.get("points") or []
    periods = forecast.get("periods") or len(points) or 1
    metrics = assessment.get("metrics") or {}

    quality_label = assessment.get("quality_label", "UNKNOWN")
    mape = metrics.get("mape")
    anomalies = assessment.get("anomalies")
    cv = eligibility.get("cv", 0.0)
    n_months = eligibility.get("n_months", len(history))
    gap_months = eligibility.get("gap_months", 0)
    gap_caveat = provenance.get("gap_caveat")
    sub_type = provenance.get("sub_type", "ETS(A,N,N)")
    sigma = provenance.get("sigma", 0.0)
    window_selection = provenance.get("window_selection") or {}
    window_used = window_selection.get("chosen")

    _t = _translator(lang)
    mape_str = f"{mape:.1f}%" if mape is not None else _t("tidak diketahui", "unknown")

    hist_window = history[-periods:] if len(history) >= periods else history
    hist_avg = _avg_y(hist_window)
    proj_avg = sum(float(p.get("point", 0)) for p in points) / len(points) if points else 0.0
    direction, pct = _direction_word(hist_avg, proj_avg, lang)

    blocks = [
        _build_summary_block(_t, periods, hist_avg, proj_avg, direction, pct),
        _build_quality_block(_t, quality_label, mape_str, lang),
        _build_data_condition_block(_t, n_months, gap_months, gap_caveat, cv, anomalies, lang),
        _build_history_block(_t, history, points),
        _build_detail_block(_t, points, sub_type, sigma),
        _build_range_block(_t, points),
        _build_confidence_block(_t, quality_label, lang),
        _build_meaning_block(_t, direction, pct, periods, gap_caveat, anomalies),
        _build_methodology_block(
            _t, sub_type, sigma, window_used, window_selection, mape_str,
            provenance.get("training_window", "N/A"),
        ),
    ]

    # Anomali only appears when the engine auto-attached one for a
    # poor-quality forecast.
    if anomalies:
        blocks.append(_build_anomaly_block(_t, anomalies))

    return "\n\n".join(blocks)
