"""visualize_chart — render one chart for the current answer in the chat.

Deterministic M9 tool, and the ONLY place a chart is ever built. No other tool
attaches a chart of its own: ``run_forecast``, ``detect_anomaly``, and
``execute_sql`` produce data, and the agent decides -- guided by the
``chart-visualize`` skill and project context -- whether that data deserves a
chart and which type fits it. Seeknal triggers, the frontend renders.

Three sources, chosen by what the data is:
  - **Mode 1 (SQL), the default:** ``visualize_chart(widget_type, widget_title,
    sql="SELECT ...")`` -- rows come from the structured SQL cache
    ``execute_sql`` already wrote under the same cache key, so the chart and the
    answer text are built from one dataset. Only on a cache miss does the tool
    run the SQL itself, through the same pipeline ``execute_sql`` uses.
  - **Exported CSV, for values SQL cannot return:** ``visualize_chart(
    widget_type, widget_title, columns=[x, y, series])`` with no ``sql=`` or
    ``data=`` -- charts the rows this turn exported as CSV. Values a tool
    computed in-process (a forecast projection) become chartable without anyone
    re-typing them, and the chart matches the Download button exactly.
  - **Mode 2 (data):** ``visualize_chart(widget_type, widget_title,
    data=[[...], ...], columns=[...])`` -- last resort, for values neither
    exported nor queryable. Transcribed numbers can drift from the answer, so
    prefer either source above.

Charting and exporting are independent: a failed CSV upload leaves the SQL path
untouched, so it must never be reported as a chart failure.

The renderer reads at most three columns and fixes each widget type's X
encoding, so a wrong shape renders wrong rather than failing. The shape is
therefore validated here, against ``_chart_payload.WIDGET_SPECS``, before any
payload is built -- an invalid shape is refused with the fix named.

Unlike ``run_forecast``/``detect_anomaly`` this tool makes no external call --
the chart "engine" is the frontend's own Vega-Lite renderer, which builds its
spec client-side from the emitted column order. Nothing here authors a spec.

One question yields at most one chart. That is enforced structurally --
``ctx.pending_visualization`` is a single slot, and a second call in the same
turn is refused rather than silently overwriting the first. This matters
because a tool like ``run_forecast`` may legitimately run several times in one
question: the agent chooses which result is worth charting, instead of each run
racing to attach a chart of its own.

Domain-neutral (``AGENTS.md``): the data, the chart type, and the title are the
agent's responsibility; the tool contains no domain-specific strings.
"""

from __future__ import annotations

from typing import Any

# Minimum columns for an actual chart: X and Y. ``big_number`` is the one
# exception -- it renders a single scalar and needs no second axis.
_MIN_CHART_COLUMNS = 2
_SCALAR_WIDGET_TYPE = "big_number"


def visualize_chart(
    widget_type: str,
    widget_title: str,
    sql: str = "",
    data: list[list[Any]] | None = None,
    columns: list[str] | None = None,
    widget_size: int = 1,
) -> str:
    """Render one chart for the answer you are about to give.

    You decide whether a chart helps and which type fits; the tool validates the
    shape but never substitutes a type for you. The ``chart-visualize`` skill
    covers which type suits which question and how to reshape data for it.

    **Column order IS the axis contract, for every type: first column = X,
    second = Y, optional third = the series that splits the chart by colour.**
    A third column turns one line into several, each with its own colour and a
    legend entry. Only three columns are ever read; extra ones are refused
    rather than dropped.

    Three sources; pick by what the data is, not by what ran first:

    Mode 1 (SQL):  ``visualize_chart(widget_type, widget_title, sql="SELECT ...")``
        The default whenever the numbers are queryable. Prefer SQL you already
        ran with ``execute_sql`` this turn -- the result is reused from cache,
        which saves a round-trip and guarantees the chart shows exactly the
        numbers your answer quotes.

    Exported CSV: ``visualize_chart(widget_type, widget_title, columns=[x, y, series])``
        No ``sql=``, no ``data=``. Charts the rows this turn exported as CSV,
        with ``columns=`` naming which of that file's columns to plot, in axis
        order. Use it for values a query cannot return -- a forecast's
        projection -- so the chart covers what the text describes.

    Mode 2 (data): ``visualize_chart(widget_type, widget_title, data=[[...]], columns=[...])``
        Last resort, for values neither exported nor queryable. Copy the
        numbers from the tool output verbatim; a chart that disagrees with the
        text is worse than no chart.

    ``sql=`` and ``data=`` are mutually exclusive, and passing neither selects
    the exported CSV. A failed CSV export never prevents charting from SQL --
    the download and the chart are independent outcomes.

    Args:
        widget_type: One of ``big_number``, ``bar_chart``, ``line_chart``,
            ``pie_chart``, ``horizontal_bar_chart``, ``grouped_bar_chart``,
            ``grouped_line_chart``, ``grouped_pie_chart``, ``area_chart``,
            ``scatter_plot``, ``heatmap``, ``box_plot``, ``treemap``.
        widget_title: Short chart title. Also serves as the accessible label,
            so make it describe the data, not the request.
        sql: A SELECT with columns ordered ``[x, y]`` or ``[x, y, series]``
            (Mode 1). For ``big_number`` a single column is enough.
        data: Pre-computed rows as a list of lists (Mode 2). Each inner list is
            one row and its length must match ``columns``.
        columns: Column names in axis order. With ``data=`` these name the
            supplied rows; with neither ``sql=`` nor ``data=`` they select
            columns from the exported CSV. Ignored in Mode 1.
        widget_size: Layout width hint for the frontend grid. Defaults to 1.

    Returns:
        A short confirmation naming the chart type, title, and row count, plus
        any deterministic reduction that was applied (top-N bucketing,
        downsampling, series trimming). Do NOT repeat the underlying table in
        your answer -- the chart renders on its own, and duplicating the data
        as markdown makes the reply noisy.
        ``"No rows to chart."`` when there is nothing to plot.
        On an unsupported ``widget_type``, an argument conflict, an unusable
        shape, or a second chart in the same turn: a short refusal explaining
        the fix, with no chart registered.
        On Mode 1 SQL execution failure: the same structured JSON error
        ``execute_sql`` returns, so the existing self-correction hook can
        enrich it with retry hints.
    """
    from seeknal.ask.agents.tools._chart_payload import (
        SUPPORTED_WIDGET_TYPES,
        build_chart_payload,
        chart_widget_id,
        validate_chart_shape,
    )
    from seeknal.ask.agents.tools._context import (
        get_tool_context,
        record_tool_result,
        repeated_failure_message,
    )
    from seeknal.ask.agents.tools.errors import RETRYABLE_SYNTAX, format_tool_error

    ctx = get_tool_context()
    signature_args = {"sql": sql, "widget_title": widget_title}

    # ── STEP 0: one chart per question (single-slot mutex) ─────────────────
    # Checked before anything else. A tool such as run_forecast may run several
    # times in one question, so "the last call wins" would make the rendered
    # chart depend on call order rather than on the agent's judgement.
    if ctx.pending_visualization is not None:
        result = (
            "A chart is already prepared for this answer. One question renders "
            "at most one chart, so this call was ignored. Present the existing "
            "chart, or reconsider which result deserves the chart before "
            "charting anything else."
        )
        record_tool_result("visualize_chart", result, args=signature_args)
        return result

    # ── STEP 0.5: validate the chart type against what the frontend renders ─
    # An unknown type would reach the frontend and render nothing at all, so it
    # is rejected here rather than emitted. The tool never picks a type on the
    # agent's behalf: choosing one is a reading of the data, which belongs in
    # the skill and project context, not in a lookup table here.
    widget_type = str(widget_type).strip()
    if widget_type not in SUPPORTED_WIDGET_TYPES:
        result = format_tool_error(
            RETRYABLE_SYNTAX,
            f"Unsupported widget_type '{widget_type}'.",
            hint="Supported types: " + ", ".join(SUPPORTED_WIDGET_TYPES) + ".",
        )
        record_tool_result("visualize_chart", result, args=signature_args)
        return result

    # ── REPEATED FAILURE CHECK — same pattern as execute_sql ───────────────
    prior_failure = repeated_failure_message("visualize_chart", signature_args)
    if prior_failure:
        result = format_tool_error(
            RETRYABLE_SYNTAX,
            prior_failure,
            hint=(
                "Do not retry the same chart. Fix the query or the data, or "
                "answer with a table instead of a chart."
            ),
        )
        record_tool_result("visualize_chart", result, args=signature_args)
        return result

    # ── STEP 1: RESOLVE (columns, rows) from one of the two modes ──────────
    resolved = _resolve_mode(sql=sql, data=data, columns=columns)
    if isinstance(resolved, str):
        record_tool_result("visualize_chart", resolved, args=signature_args)
        return resolved
    chart_columns, chart_rows, fingerprint = resolved

    # ── STEP 2: VALIDATE SHAPE ─────────────────────────────────────────────
    if not chart_rows:
        result = "No rows to chart."
        record_tool_result("visualize_chart", result, args=signature_args)
        return result

    minimum_columns = 1 if widget_type == _SCALAR_WIDGET_TYPE else _MIN_CHART_COLUMNS
    if len(chart_columns) < minimum_columns:
        result = format_tool_error(
            RETRYABLE_SYNTAX,
            f"'{widget_type}' needs at least {minimum_columns} column(s); "
            f"got {len(chart_columns)}.",
            hint=(
                "Put the X dimension first and the Y metric second, then call "
                "the tool again -- or pick a type that fits this shape."
            ),
        )
        record_tool_result("visualize_chart", result, args=signature_args)
        return result

    # The renderer reads three columns and fixes each type's X encoding, so a
    # wrong shape draws a wrong chart instead of failing. Refuse it here, where
    # the agent can still act on the reason.
    shape_problem = validate_chart_shape(widget_type, chart_columns, chart_rows)
    if shape_problem:
        result = format_tool_error(
            RETRYABLE_SYNTAX,
            shape_problem,
            hint=(
                "Fix the shape or the widget_type, then call the tool again. "
                "Do not chart data the chosen type cannot show."
            ),
        )
        record_tool_result("visualize_chart", result, args=signature_args)
        return result

    # ── STEP 3: BUILD PAYLOAD + REGISTER ───────────────────────────────────
    payload, notices = build_chart_payload(
        widget_type=widget_type,
        widget_title=str(widget_title).strip(),
        widget_id=chart_widget_id(ctx.session_id, fingerprint),
        columns=chart_columns,
        rows=chart_rows,
        widget_size=_coerce_widget_size(widget_size),
    )
    ctx.pending_visualization = payload

    result = _format_confirmation(payload, notices)
    record_tool_result("visualize_chart", result, args=signature_args)
    return result


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _resolve_exported_dataset(
    columns: list[str] | None,
) -> tuple[list[str], list[list[Any]], str] | str:
    """Chart this turn's exported CSV, projecting the requested columns.

    ``columns`` selects from the export by name and fixes the axis order, so
    the caller can chart three of a wider file -- the combined history and
    projection a forecast exports, for instance, whose eight columns carry the
    two series the chart needs. Names are matched case-insensitively because
    the agent reads them back from a tool result.

    Omitting ``columns`` charts the export's own first columns, which is right
    only when the file is already in axis order.

    Returns ``(columns, rows, fingerprint)`` or a ``## Kesalahan`` string.
    """
    from seeknal.ask.agents.tools._chart_payload import MAX_CHART_COLUMNS
    from seeknal.ask.agents.tools._context import get_tool_context

    dataset = get_tool_context().exported_dataset
    if not dataset or not dataset.get("rows"):
        return (
            "## Kesalahan\n\n"
            "**visualize_chart has nothing to chart.**\n\n"
            "No CSV has been exported in this turn, so there is no dataset to "
            "draw. Export the answer's data first, or pass ``sql=`` (Mode 1) "
            "or ``data=``+``columns=`` (Mode 2)."
        )

    source_columns = [str(name) for name in dataset.get("columns", [])]
    source_rows = dataset.get("rows", [])
    name = dataset.get("name", "")

    wanted = [str(column).strip() for column in (columns or []) if str(column).strip()]
    if not wanted:
        selected = source_columns[:MAX_CHART_COLUMNS]
        indexes = list(range(len(selected)))
    else:
        lookup = {column.lower(): index for index, column in enumerate(source_columns)}
        indexes = []
        for column in wanted:
            index = lookup.get(column.lower())
            if index is None:
                return (
                    "## Kesalahan\n\n"
                    f"**Column '{column}' is not in the exported CSV.**\n\n"
                    f"'{name}' has: {', '.join(source_columns)}. Name the "
                    "columns to chart in axis order (X, Y, then the optional "
                    "series), or pass ``sql=`` to chart a query instead."
                )
            indexes.append(index)
        selected = [source_columns[index] for index in indexes]

    rows = [[row[index] for index in indexes] for row in source_rows if len(row) > max(indexes, default=0)]
    return selected, rows, f"export:{name}:{','.join(selected)}"


def _resolve_mode(
    *,
    sql: str,
    data: list[list[Any]] | None,
    columns: list[str] | None,
) -> tuple[list[str], list[list[Any]], str] | str:
    """Dispatch to SQL mode or data mode based on which arguments were provided.

    Returns ``(columns, rows, fingerprint)`` on success, where ``fingerprint``
    is the stable string the deterministic widget id is derived from. Returns a
    ``## Kesalahan`` markdown block string on argument mismatch or SQL failure,
    which the caller returns as-is.

    Validation rules mirror ``upload_to_s3._resolve_mode``:
      - ``sql=`` and ``data=`` are mutually exclusive
      - Mode 2 requires BOTH ``data=`` and ``columns=``
      - Mode 2: every row length must equal ``len(columns)``, otherwise the
        column-order axis contract would silently bind the wrong values
      - an empty ``sql`` string counts as "not provided"
      - neither ``sql=`` nor ``data=`` selects this turn's exported CSV
    """
    from seeknal.ask.agents.tools._chart_payload import CHART_MAX_ROWS
    from seeknal.ask.agents.tools._context import (
        get_structured_sql_cache,
        get_tool_context,
    )
    from seeknal.ask.agents.tools.errors import classify_duckdb_error, format_tool_error
    from seeknal.ask.agents.tools.execute_sql import (
        _execute_oneshot_with_timeout,
        _repair_common_sql_before_execution,
        _sql_cache_key,
    )

    has_sql = bool(sql and sql.strip())
    has_data = data is not None
    has_columns = bool(columns)

    if has_sql and has_data:
        return (
            "## Kesalahan\n\n"
            "**visualize_chart argument conflict.**\n\n"
            "Provide either ``sql=`` (Mode 1) OR ``data=``+``columns=`` "
            "(Mode 2), not both. The two modes are mutually exclusive."
        )

    if has_data:
        if not has_columns:
            return (
                "## Kesalahan\n\n"
                "**visualize_chart is missing ``columns=``.**\n\n"
                "Mode 2 needs column names in axis order alongside ``data=``, "
                "because column order is what binds values to the X and Y axes."
            )
        rows = [list(row) for row in data or []]
        jagged = [index for index, row in enumerate(rows) if len(row) != len(columns or [])]
        if jagged:
            return (
                "## Kesalahan\n\n"
                f"**visualize_chart row length mismatch** at row index {jagged[0]}.\n\n"
                f"Every row must have exactly {len(columns or [])} values to match "
                "``columns=``; otherwise values bind to the wrong axis."
            )
        # Fingerprint from the column names plus the row count: stable across
        # repeated calls with the same computed series, without hashing the
        # whole dataset.
        return list(columns or []), rows, f"{','.join(columns or [])}:{len(rows)}"

    if not has_sql:
        return _resolve_exported_dataset(columns)

    # Mode 1: normalise the SQL through the same pipeline execute_sql uses, so
    # the cache key matches the one it stored and an already-answered query
    # charts without re-running.
    ctx = get_tool_context()
    sql = str(sql).strip().rstrip(";").strip()
    sql, _lint_notices = _repair_common_sql_before_execution(sql)

    cache_key = _sql_cache_key(sql)
    cached = get_structured_sql_cache(ctx).get(cache_key)
    if cached is not None:
        return list(cached.get("columns") or []), list(cached.get("rows") or []), sql

    # Cache miss: the agent is charting a query it has not shown. Pull up to the
    # chart's own row ceiling -- enough for top-N bucketing to be accurate,
    # bounded by the same number the payload caps enforce.
    try:
        sql_columns, sql_rows = _execute_oneshot_with_timeout(
            ctx, sql, limit=CHART_MAX_ROWS
        )
    except Exception as exc:
        return format_tool_error(classify_duckdb_error(str(exc)), str(exc))

    return list(sql_columns), [list(row) for row in sql_rows], sql


def _coerce_widget_size(widget_size: Any) -> int:
    """Return a usable layout size, falling back to 1 for unusable input.

    The frontend grid only understands small positive integers, and a bad size
    is never worth failing an otherwise-valid chart over.
    """
    try:
        return max(1, int(widget_size))
    except (TypeError, ValueError):
        return 1


def _format_confirmation(payload: list[dict[str, Any]], notices: list[str]) -> str:
    """Build the tool result the agent reads after a chart is registered."""
    chart = payload[0]["chart_json"]
    widget_data = chart["widgetData"]
    row_count = len(widget_data)
    lines = [
        f"Chart ready: {chart['widgetType']} \"{chart['widgetTitle']}\" "
        f"({row_count} rows). It renders automatically with your answer -- "
        f"do not repeat the underlying table in your reply."
    ]

    # Report the series so the agent can describe what the reader will see: a
    # multi-series chart draws one colour per series and a legend naming them,
    # and the answer text should match that rather than describe one line.
    series_names = _series_names(widget_data)
    if series_names:
        lines.append(
            f"Series drawn ({len(series_names)}), each its own colour with a "
            f"legend: {', '.join(series_names)}."
        )

    if notices:
        lines.append("Applied limits: " + "; ".join(notices) + ".")
    return "\n".join(lines)


def _series_names(widget_data: list[dict[str, Any]]) -> list[str]:
    """Return the distinct values of the series column, in first-seen order."""
    if not widget_data:
        return []
    keys = list(widget_data[0].keys())
    if len(keys) < 3:
        return []
    series_key = keys[2]
    seen: dict[str, None] = {}
    for row in widget_data:
        seen.setdefault(str(row.get(series_key)), None)
    return list(seen)
