"""Chart payload construction shared by ``visualize_chart`` and ``run_forecast``.

Single source of truth for the M9 chat-chart wire shape. Both the generic
SQL-driven tool and ``run_forecast``'s self-chart path build their payload here,
so the two can never drift apart -- one payload shape, two correctly scoped
sources of data.

The emitted shape is the array iba-web's chat renderer already parses::

    [{
        "chart_json": {
            "widgetId": str, "widgetType": str, "widgetTitle": str,
            "widgetData": [{col: value, ...}, ...], "widgetSize": int,
        },
        "chart_type": str,
    }]

Deliberately absent: ``vegaSpec``. iba-web builds its own Vega-Lite spec
client-side from ``widgetData``, so nothing on the wire carries a spec. Column
order IS the axis contract for every widget type: the first column is X, the
second is Y, and a third (when present) is the series/colour dimension.

Domain-neutral (``AGENTS.md``): callers supply the data and the widget type;
this module contains no domain-specific strings.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime
from decimal import Decimal
from typing import Any, TypeVar

# _downsample works on both raw rows and rendered widgetData dicts.
_Item = TypeVar("_Item")

# Widget types iba-web can render. The first twelve map to the chat renderer's
# client-side Vega-Lite spec generator; ``big_number`` is routed to a separate
# scalar component. Anything outside this list renders as nothing, so the tool
# validates against it before emitting.
SUPPORTED_WIDGET_TYPES: tuple[str, ...] = (
    "big_number",
    "bar_chart",
    "line_chart",
    "pie_chart",
    "horizontal_bar_chart",
    "grouped_bar_chart",
    "grouped_line_chart",
    "grouped_pie_chart",
    "area_chart",
    "scatter_plot",
    "heatmap",
    "box_plot",
    "treemap",
)

# Payload caps. Concrete values so two implementers cannot diverge; every
# reduction below is deterministic and reported back to the agent as a notice,
# never applied silently.
CHART_MAX_ROWS = 500        # hard ceiling on widgetData length
CHART_MAX_POINTS = 200      # max points on a single time-series line/area
CHART_TOP_N = 12            # categorical charts keep the top N, bucket the rest
CHART_MAX_SERIES = 8        # max distinct values in the third (series) column
CHART_MAX_PAYLOAD_BYTES = 100_000  # serialized ceiling; the payload is persisted

# Label for the bucket that absorbs categories beyond CHART_TOP_N. Explicit so
# the chart never silently hides part of the total.
OTHERS_LABEL = "Others"

_CATEGORICAL_TYPES = frozenset(
    {"bar_chart", "horizontal_bar_chart", "pie_chart", "grouped_pie_chart", "treemap"}
)
_TIME_SERIES_TYPES = frozenset({"line_chart", "area_chart", "grouped_line_chart"})

# ---------------------------------------------------------------------------
# Per-widget shape contracts
# ---------------------------------------------------------------------------
# The renderer reads AT MOST three columns -- ``keys[0]`` as X, ``keys[1]`` as Y
# and ``keys[2]`` as the colour/series dimension -- and silently ignores every
# column after the third. It also fixes the X encoding per widget type, so a
# label the encoding cannot parse yields an empty axis rather than an error.
#
# Encoding a wrong shape is therefore invisible at run time: the chart still
# renders, just wrong. These contracts move that failure forward to the tool
# call, where the agent can still fix it.
#
# ``x_kind`` mirrors the renderer's X encoding:
#   temporal      -- parsed as a date; non-ISO labels collapse the axis
#   categorical   -- nominal; any label works
#   quantitative  -- parsed as a number; non-numeric collapses the axis
# ``series`` is whether the renderer actually reads the third column.


class WidgetSpec:
    """The data shape one widget type can faithfully render."""

    __slots__ = ("x_kind", "series", "shows")

    def __init__(self, x_kind: str, series: bool, shows: str) -> None:
        self.x_kind = x_kind
        self.series = series
        self.shows = shows


WIDGET_SPECS: dict[str, WidgetSpec] = {
    "line_chart": WidgetSpec("temporal", True, "a metric over time"),
    "grouped_line_chart": WidgetSpec("temporal", True, "several metrics over time"),
    "area_chart": WidgetSpec("temporal", True, "a cumulative metric over time"),
    "bar_chart": WidgetSpec("categorical", False, "one metric across categories"),
    "grouped_bar_chart": WidgetSpec(
        "categorical", True, "several metrics across categories"
    ),
    "horizontal_bar_chart": WidgetSpec(
        "categorical", False, "one metric across categories with long labels"
    ),
    "pie_chart": WidgetSpec("categorical", False, "one metric's share of a total"),
    "grouped_pie_chart": WidgetSpec(
        "categorical", False, "one metric's share of a total"
    ),
    "treemap": WidgetSpec("categorical", False, "nested share of a total"),
    "box_plot": WidgetSpec("categorical", False, "a metric's spread per category"),
    "scatter_plot": WidgetSpec("quantitative", True, "two metrics correlated"),
    "heatmap": WidgetSpec("categorical", True, "a metric across two dimensions"),
    "big_number": WidgetSpec("categorical", False, "a single headline value"),
}

# Suggested replacement when the chosen type cannot carry a series column but
# the data has one. Types absent here have no series-capable equivalent.
_SERIES_ALTERNATIVE: dict[str, str] = {
    "bar_chart": "grouped_bar_chart",
    "horizontal_bar_chart": "grouped_bar_chart",
    "line_chart": "grouped_line_chart",
    "pie_chart": "grouped_bar_chart",
    "grouped_pie_chart": "grouped_bar_chart",
    "treemap": "grouped_bar_chart",
    "box_plot": "grouped_bar_chart",
}

# Widget types whose X axis tolerates period labels, offered when a temporal
# chart is asked to plot labels that are not parseable dates.
_CATEGORICAL_ALTERNATIVE: dict[str, str] = {
    "line_chart": "bar_chart",
    "grouped_line_chart": "grouped_bar_chart",
    "area_chart": "bar_chart",
}

# Accepted temporal literals: a year, a year-month, a date, or a full ISO
# timestamp. Deliberately narrower than the renderer's own date parsing, which
# accepts locale-dependent forms that differ between browsers.
_ISO_TEMPORAL = re.compile(
    r"^\d{4}(-\d{2}(-\d{2}([T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[+-]\d{2}:?\d{2})?)?)?)?$"
)

MAX_CHART_COLUMNS = 3


def chart_widget_id(session_id: str, sql: str) -> str:
    """Build a deterministic widget id for one chart.

    Same session and same SQL always produce the same id, so repeated
    inspection and debugging are stable. The id is not load-bearing for
    de-duplication -- one chart per turn is enforced by the single-slot
    ``ctx.pending_visualization``, not by comparing ids.

    Args:
        session_id: The ask session id from ``ToolContext``.
        sql: The SQL (or any stable fingerprint) behind this chart.

    Returns:
        An id of the form ``"{session_id}-{8-hex-digest}"``.
    """
    digest = hashlib.sha1(sql.strip().encode("utf-8")).hexdigest()[:8]
    return f"{session_id}-{digest}"


def validate_chart_shape(
    widget_type: str,
    columns: list[str],
    rows: list[list[Any]],
) -> str | None:
    """Check the data against the widget's shape contract.

    Returns ``None`` when the shape renders faithfully, otherwise one sentence
    naming the mismatch plus the concrete fix. Every rule here guards a failure
    the renderer would otherwise absorb silently -- a dropped column, a
    collapsed axis -- and produce a chart that disagrees with the answer.

    Args:
        widget_type: One of :data:`SUPPORTED_WIDGET_TYPES`.
        columns: Column names in axis order.
        rows: Row values aligned to ``columns``.

    Returns:
        ``None`` if valid, else the reason and the remedy.
    """
    spec = WIDGET_SPECS.get(widget_type)
    if spec is None or not rows:
        return None

    column_count = len(columns)

    # Rule 1 -- wide data. The 4th column onwards is dropped by the renderer,
    # so a wide table charts only its first metric and silently hides the rest.
    if column_count > MAX_CHART_COLUMNS:
        extra = ", ".join(str(name) for name in columns[MAX_CHART_COLUMNS:])
        return (
            f"{column_count} columns given; a chart reads at most "
            f"{MAX_CHART_COLUMNS} ({columns[0]} = X, {columns[1]} = Y, "
            f"{columns[2]} = series). These would be dropped: {extra}. "
            "Reshape the query from wide to long so every metric becomes rows "
            "of one series column -- SELECT period, 'metric name' AS series, "
            "metric AS value ... UNION ALL ... (or UNPIVOT) -- then chart it "
            "with grouped_bar_chart or grouped_line_chart to show all metrics "
            "at once."
        )

    # Rule 2 -- a series column this widget type cannot draw. The renderer only
    # reads keys[2] for the types marked series-capable; for the rest the third
    # column is dropped, so the chart shows one metric while the answer covers
    # several.
    if column_count == MAX_CHART_COLUMNS and not spec.series:
        alternative = _SERIES_ALTERNATIVE.get(widget_type)
        remedy = (
            f"Use '{alternative}' to draw all series, or drop the third column."
            if alternative
            else "Drop the third column."
        )
        return (
            f"'{widget_type}' draws {spec.shows} and ignores the third column "
            f"('{columns[2]}'), so that series would not appear. {remedy}"
        )

    # Rule 3 -- an X axis the encoding cannot parse. A temporal axis fed period
    # names, or a quantitative axis fed labels, renders an empty or flat chart
    # rather than failing.
    offender = _first_unparseable_x(spec.x_kind, rows)
    if offender is not None:
        if spec.x_kind == "temporal":
            alternative = _CATEGORICAL_ALTERNATIVE.get(widget_type, "bar_chart")
            return (
                f"'{widget_type}' plots X as a date, but '{columns[0]}' holds "
                f"{offender!r}, which is not one. Either return ISO periods "
                "(2024-01-01, or 2024-01 for months) so the axis is ordered by "
                f"time, or use '{alternative}', whose X axis takes labels."
            )
        return (
            f"'{widget_type}' plots X as a number, but '{columns[0]}' holds "
            f"{offender!r}. Put the numeric measure first, or use "
            "'bar_chart' for a labelled X axis."
        )

    return None


def _first_unparseable_x(x_kind: str, rows: list[list[Any]]) -> Any | None:
    """Return the first X value the given encoding cannot parse, else ``None``."""
    if x_kind == "categorical":
        return None

    for row in rows:
        if not row:
            continue
        value = row[0]
        if value is None:
            continue
        if x_kind == "temporal":
            if isinstance(value, (datetime, date)):
                continue
            if isinstance(value, str) and _ISO_TEMPORAL.match(value.strip()):
                continue
            return value
        if x_kind == "quantitative":
            if isinstance(value, bool):
                return value
            if isinstance(value, (int, float, Decimal)):
                continue
            try:
                float(str(value))
            except (TypeError, ValueError):
                return value
    return None


def build_chart_payload(
    *,
    widget_type: str,
    widget_title: str,
    widget_id: str,
    columns: list[str],
    rows: list[list[Any]],
    widget_size: int = 1,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Build the chart payload from tabular data, enforcing the payload caps.

    Args:
        widget_type: One of :data:`SUPPORTED_WIDGET_TYPES`. Callers validate
            this first; it is used here only to pick the right cap strategy.
        widget_title: Human-readable chart title, also the accessible label.
        widget_id: Deterministic id from :func:`chart_widget_id`.
        columns: Column names in axis order -- ``[x, y]`` or ``[x, y, series]``.
        rows: Row values aligned to ``columns``.
        widget_size: Layout hint consumed by the frontend grid.

    Returns:
        A ``(payload, notices)`` tuple. ``payload`` is the array described in
        the module docstring. ``notices`` describes every deterministic
        reduction applied (top-N bucketing, downsampling, series trimming) so
        the caller can tell the agent what the chart actually shows.
    """
    capped_rows, notices = _apply_caps(widget_type, rows)

    widget_data = [
        {str(column): _json_safe(cell) for column, cell in zip(columns, row)}
        for row in capped_rows
    ]
    widget_data, byte_notice = _fit_payload_bytes(widget_data)
    if byte_notice:
        notices.append(byte_notice)

    payload = [
        {
            "chart_json": {
                "widgetId": widget_id,
                "widgetType": widget_type,
                "widgetTitle": widget_title,
                "widgetData": widget_data,
                "widgetSize": widget_size,
            },
            "chart_type": widget_type,
        }
    ]
    return payload, notices


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _json_safe(value: Any) -> Any:
    """Coerce one DuckDB cell into a value ``json.dumps`` accepts.

    The payload is serialized onto the SSE stream and persisted into a JSON
    column, so a stray ``date``/``Decimal`` would break the whole turn rather
    than just the chart.
    """
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _as_number(value: Any) -> float:
    """Return ``value`` as a float, treating anything unparseable as zero.

    Used only for ranking and bucketing, where a non-numeric metric should sort
    last rather than raise.
    """
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _apply_caps(widget_type: str, rows: list[list[Any]]) -> tuple[list[list[Any]], list[str]]:
    """Apply the payload caps in a fixed order, returning rows plus notices.

    Order matters and is deliberate: trim series first (it removes whole rows),
    then bucket categories, then downsample, then enforce the absolute row
    ceiling. Running it the other way round would downsample rows that
    bucketing was about to merge anyway.
    """
    notices: list[str] = []

    rows, notice = _cap_series(rows)
    if notice:
        notices.append(notice)

    if widget_type in _CATEGORICAL_TYPES:
        rows, notice = _bucket_top_n(rows)
        if notice:
            notices.append(notice)

    if widget_type in _TIME_SERIES_TYPES:
        original = len(rows)
        rows = _downsample_per_series(rows, CHART_MAX_POINTS)
        if len(rows) != original:
            notices.append(
                f"downsampled {original} points to {len(rows)} "
                f"(cap {CHART_MAX_POINTS} points per series)"
            )

    if len(rows) > CHART_MAX_ROWS:
        original = len(rows)
        rows = _downsample(rows, CHART_MAX_ROWS)
        notices.append(
            f"reduced {original} rows to {len(rows)} (cap {CHART_MAX_ROWS} rows)"
        )

    return rows, notices


def _cap_series(rows: list[list[Any]]) -> tuple[list[list[Any]], str | None]:
    """Keep only the largest ``CHART_MAX_SERIES`` values of the series column.

    Applies only to three-column data, where the third column is the series
    dimension. Series are ranked by their summed metric so the dominant ones
    survive; rows belonging to dropped series are removed entirely rather than
    merged, because merging unrelated series would misrepresent the data.
    """
    if not rows or len(rows[0]) < 3:
        return rows, None

    totals: dict[Any, float] = {}
    for row in rows:
        totals[row[2]] = totals.get(row[2], 0.0) + _as_number(row[1])
    if len(totals) <= CHART_MAX_SERIES:
        return rows, None

    ranked = sorted(totals.items(), key=lambda item: item[1], reverse=True)
    keep = {name for name, _total in ranked[:CHART_MAX_SERIES]}
    notice = (
        f"kept the top {CHART_MAX_SERIES} of {len(totals)} series "
        f"(cap {CHART_MAX_SERIES} series)"
    )
    return [row for row in rows if row[2] in keep], notice


def _bucket_top_n(rows: list[list[Any]]) -> tuple[list[list[Any]], str | None]:
    """Keep the top ``CHART_TOP_N`` categories and bucket the rest as "Others".

    Only meaningful for two-column categorical data. The remainder is summed
    into one explicit row instead of being dropped, so the chart's total still
    matches the answer's total.
    """
    if len(rows) <= CHART_TOP_N or not rows or len(rows[0]) != 2:
        return rows, None

    ranked = sorted(rows, key=lambda row: _as_number(row[1]), reverse=True)
    kept = list(ranked[:CHART_TOP_N])
    remainder = sum(_as_number(row[1]) for row in ranked[CHART_TOP_N:])
    kept.append([OTHERS_LABEL, remainder])
    notice = (
        f"kept the top {CHART_TOP_N} of {len(rows)} categories and bucketed the "
        f'remainder into "{OTHERS_LABEL}"'
    )
    return kept, notice


def _downsample_per_series(
    rows: list[list[Any]], target: int
) -> list[list[Any]]:
    """Downsample a time series, keeping each series independently intact.

    Long-format multi-series rows interleave series, so a single global stride
    would keep different periods for different series and draw lines that jump
    between points the data never had. Each series is therefore strided on its
    own and the original row order is restored.
    """
    if not rows or len(rows[0]) < 3:
        return _downsample(rows, target)

    positions_by_series: dict[Any, list[int]] = {}
    for index, row in enumerate(rows):
        positions_by_series.setdefault(row[2], []).append(index)

    if all(len(positions) <= target for positions in positions_by_series.values()):
        return rows

    kept: set[int] = set()
    for positions in positions_by_series.values():
        kept.update(_downsample(positions, target))
    return [row for index, row in enumerate(rows) if index in kept]


def _downsample(rows: list[_Item], target: int) -> list[_Item]:
    """Reduce ``rows`` to at most ``target`` entries with an even stride.

    Deterministic (no sampling) and always keeps the last row, so a time series
    still ends where the data ends -- a downsampled trend that stops early
    would read as a drop that never happened.
    """
    if len(rows) <= target or target < 1:
        return rows

    step = len(rows) / float(target)
    indexes = sorted({min(len(rows) - 1, int(i * step)) for i in range(target)})
    if indexes[-1] != len(rows) - 1:
        indexes[-1] = len(rows) - 1
    return [rows[index] for index in indexes]


def _fit_payload_bytes(
    widget_data: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], str | None]:
    """Halve the row count until the serialized payload fits the byte ceiling.

    The last line of defence: wide rows can blow the ceiling even at a legal row
    count, and the payload is persisted into a JSON column and replayed on every
    history load.
    """
    if not widget_data:
        return widget_data, None

    original = len(widget_data)
    while len(widget_data) > 1 and _payload_bytes(widget_data) > CHART_MAX_PAYLOAD_BYTES:
        widget_data = _downsample(widget_data, max(1, len(widget_data) // 2))

    if len(widget_data) == original:
        return widget_data, None
    return widget_data, (
        f"reduced {original} rows to {len(widget_data)} to fit the "
        f"{CHART_MAX_PAYLOAD_BYTES // 1000} KB chart payload ceiling"
    )


def _payload_bytes(widget_data: list[Any]) -> int:
    """Return the serialized byte size of ``widget_data``."""
    return len(json.dumps(widget_data, ensure_ascii=False).encode("utf-8"))
