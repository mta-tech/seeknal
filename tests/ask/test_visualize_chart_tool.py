"""Tests for the visualize_chart tool — payload shape, caps, cache reuse, mutex.

No network and no engine: the tool is purely local, so these run against an
in-memory DuckDB.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import duckdb
import pytest

from seeknal.ask.agents.tools._chart_payload import (
    CHART_MAX_POINTS,
    CHART_MAX_SERIES,
    CHART_TOP_N,
    OTHERS_LABEL,
)
from seeknal.ask.agents.tools._context import (
    ToolContext,
    get_structured_sql_cache,
    reset_turn_governor,
    set_tool_context,
)
from seeknal.ask.agents.tools.visualize_chart import visualize_chart


class _REPLStub:
    def __init__(self) -> None:
        self.conn = duckdb.connect(":memory:")
        self.executions = 0

    def execute_oneshot(self, sql: str, limit=None):
        self.executions += 1
        if limit is not None:
            sql = f"SELECT * FROM ({sql}) AS _q LIMIT {int(limit)}"
        result = self.conn.execute(sql)
        if not result.description:
            return [], []
        cols = [d[0] for d in result.description]
        return cols, result.fetchall()


@pytest.fixture
def ctx(tmp_path: Path) -> ToolContext:
    repl = _REPLStub()
    repl.conn.execute(
        "CREATE TABLE sales AS "
        "SELECT DATE '2026-01-01' + INTERVAL (i) MONTH AS bulan, "
        "       (i + 1) * 10 AS total "
        "FROM generate_series(0, 5) AS t(i)"
    )
    ctx = ToolContext(
        repl=repl,
        artifact_discovery=MagicMock(),
        project_path=tmp_path,
        sql_timeout_seconds=0,
        request_limit=100,
    )
    set_tool_context(ctx)
    return ctx


SQL = "SELECT bulan, total FROM sales ORDER BY 1"


def _chart(ctx: ToolContext) -> dict:
    """Return the single chart_json the tool registered."""
    assert ctx.pending_visualization is not None
    assert len(ctx.pending_visualization) == 1
    return ctx.pending_visualization[0]["chart_json"]


# ---------------------------------------------------------------------------
# Payload shape
# ---------------------------------------------------------------------------


def test_registers_chart_in_legacy_array_shape(ctx: ToolContext) -> None:
    out = visualize_chart("line_chart", "Sales per month", sql=SQL)

    assert "Chart ready" in out
    payload = ctx.pending_visualization
    assert isinstance(payload, list) and len(payload) == 1
    assert set(payload[0]) == {"chart_json", "chart_type"}
    assert payload[0]["chart_type"] == "line_chart"
    assert set(payload[0]["chart_json"]) == {
        "widgetId",
        "widgetType",
        "widgetTitle",
        "widgetData",
        "widgetSize",
    }


def test_payload_carries_no_vega_spec(ctx: ToolContext) -> None:
    """The frontend builds its own spec; nothing spec-shaped goes on the wire."""
    visualize_chart("bar_chart", "Sales", sql=SQL)

    chart = _chart(ctx)
    for dropped in ("vegaSpec", "schemaVersion", "summary", "xField", "yField",
                    "seriesField", "sourceTool"):
        assert dropped not in chart


def test_widget_data_preserves_column_order(ctx: ToolContext) -> None:
    """Column order is the axis contract: keys[0] = X, keys[1] = Y."""
    visualize_chart("line_chart", "Sales", sql=SQL)

    rows = _chart(ctx)["widgetData"]
    assert list(rows[0].keys()) == ["bulan", "total"]


def test_widget_id_is_deterministic(ctx: ToolContext) -> None:
    visualize_chart("line_chart", "Sales", sql=SQL)
    first = _chart(ctx)["widgetId"]

    ctx.pending_visualization = None
    visualize_chart("line_chart", "Sales", sql=SQL)

    assert _chart(ctx)["widgetId"] == first


def test_payload_is_json_serializable(ctx: ToolContext) -> None:
    """DATE cells must survive json.dumps — the payload rides the SSE stream."""
    visualize_chart("line_chart", "Sales", sql=SQL)

    encoded = json.dumps(ctx.pending_visualization)
    assert "2026-01-01" in encoded


def test_decimal_and_bytes_cells_are_coerced(ctx: ToolContext) -> None:
    ctx.repl.conn.execute(
        "CREATE TABLE mixed AS SELECT 'a' AS k, CAST(1.5 AS DECIMAL(4,2)) AS v"
    )
    visualize_chart("bar_chart", "Mixed", sql="SELECT k, v FROM mixed")

    row = _chart(ctx)["widgetData"][0]
    assert row["v"] == pytest.approx(1.5)
    json.dumps(ctx.pending_visualization)


# ---------------------------------------------------------------------------
# Mode 2 — computed data
# ---------------------------------------------------------------------------


def test_data_mode_builds_chart_without_sql(ctx: ToolContext) -> None:
    out = visualize_chart(
        "line_chart",
        "History and projection",
        data=[["2026-01", 10, "history"], ["2026-02", 12, "projection"]],
        columns=["period", "value", "segment"],
    )

    assert "Chart ready" in out
    rows = _chart(ctx)["widgetData"]
    assert list(rows[0].keys()) == ["period", "value", "segment"]
    assert {row["segment"] for row in rows} == {"history", "projection"}
    assert ctx.repl.executions == 0


def test_data_mode_rejects_jagged_rows(ctx: ToolContext) -> None:
    out = visualize_chart(
        "bar_chart",
        "Broken",
        data=[["a", 1], ["b"]],
        columns=["k", "v"],
    )

    assert "row length mismatch" in out
    assert ctx.pending_visualization is None


def test_data_mode_requires_columns(ctx: ToolContext) -> None:
    out = visualize_chart("bar_chart", "Broken", data=[["a", 1]])

    assert "columns=" in out
    assert ctx.pending_visualization is None


def test_sql_and_data_together_are_refused(ctx: ToolContext) -> None:
    out = visualize_chart(
        "bar_chart", "Conflict", sql=SQL, data=[["a", 1]], columns=["k", "v"]
    )

    assert "argument conflict" in out
    assert ctx.pending_visualization is None


def test_no_source_at_all_is_refused(ctx: ToolContext) -> None:
    out = visualize_chart("bar_chart", "Nothing")

    assert "nothing to chart" in out
    assert ctx.pending_visualization is None


# ---------------------------------------------------------------------------
# Validation and refusals
# ---------------------------------------------------------------------------


def test_unsupported_widget_type_is_refused(ctx: ToolContext) -> None:
    out = visualize_chart("sankey_diagram", "Nope", sql=SQL)

    assert "Unsupported widget_type" in out
    assert ctx.pending_visualization is None


def test_empty_result_emits_no_chart(ctx: ToolContext) -> None:
    out = visualize_chart(
        "line_chart", "Empty", sql="SELECT bulan, total FROM sales WHERE total < 0"
    )

    assert out == "No rows to chart."
    assert ctx.pending_visualization is None


def test_single_column_is_refused_for_a_real_chart(ctx: ToolContext) -> None:
    out = visualize_chart("bar_chart", "One column", sql="SELECT total FROM sales")

    assert "needs at least 2 column(s)" in out
    assert ctx.pending_visualization is None


def test_single_column_is_accepted_for_big_number(ctx: ToolContext) -> None:
    out = visualize_chart(
        "big_number", "Total", sql="SELECT SUM(total) AS total FROM sales"
    )

    assert "Chart ready" in out
    assert _chart(ctx)["widgetType"] == "big_number"


def test_sql_failure_returns_structured_error(ctx: ToolContext) -> None:
    """Same JSON shape execute_sql returns, so the self-correction hook applies."""
    out = visualize_chart("bar_chart", "Broken", sql="SELECT * FROM no_such_table")

    error = json.loads(out)
    assert error["category"] == "retryable_missing_ref"
    assert error["retryable"] is True
    assert ctx.pending_visualization is None


# ---------------------------------------------------------------------------
# One chart per question
# ---------------------------------------------------------------------------


def test_second_chart_in_same_turn_is_refused(ctx: ToolContext) -> None:
    visualize_chart("line_chart", "First", sql=SQL)
    first = _chart(ctx)["widgetTitle"]

    out = visualize_chart("bar_chart", "Second", sql=SQL)

    assert "already prepared" in out
    assert _chart(ctx)["widgetTitle"] == first


def test_turn_reset_clears_pending_chart(ctx: ToolContext) -> None:
    visualize_chart("line_chart", "First", sql=SQL)
    assert ctx.pending_visualization is not None

    reset_turn_governor("next question")

    assert ctx.pending_visualization is None


# ---------------------------------------------------------------------------
# Structured cache reuse
# ---------------------------------------------------------------------------


def test_reuses_structured_cache_without_re_executing(ctx: ToolContext) -> None:
    """A cache hit is what keeps the chart and the answer text on one dataset."""
    from seeknal.ask.agents.tools.execute_sql import _sql_cache_key

    get_structured_sql_cache(ctx)[_sql_cache_key(SQL)] = {
        "columns": ["bulan", "total"],
        "rows": [[date(2026, 1, 1), 999]],
    }

    visualize_chart("line_chart", "Cached", sql=SQL)

    assert ctx.repl.executions == 0
    assert _chart(ctx)["widgetData"] == [{"bulan": "2026-01-01", "total": 999}]


def test_execute_sql_populates_the_structured_cache(ctx: ToolContext) -> None:
    from seeknal.ask.agents.tools.execute_sql import _sql_cache_key, execute_sql

    execute_sql(SQL)

    cached = get_structured_sql_cache(ctx)[_sql_cache_key(SQL)]
    assert cached["columns"] == ["bulan", "total"]
    assert len(cached["rows"]) == 6


# ---------------------------------------------------------------------------
# Payload caps
# ---------------------------------------------------------------------------


def test_categorical_chart_buckets_beyond_top_n(ctx: ToolContext) -> None:
    rows = [[f"cat-{i}", i] for i in range(CHART_TOP_N + 8)]
    visualize_chart("bar_chart", "Many categories", data=rows, columns=["k", "v"])

    widget_data = _chart(ctx)["widgetData"]
    assert len(widget_data) == CHART_TOP_N + 1
    assert widget_data[-1]["k"] == OTHERS_LABEL


def test_others_bucket_preserves_the_total(ctx: ToolContext) -> None:
    rows = [[f"cat-{i}", i] for i in range(CHART_TOP_N + 8)]
    visualize_chart("bar_chart", "Many categories", data=rows, columns=["k", "v"])

    charted = sum(row["v"] for row in _chart(ctx)["widgetData"])
    assert charted == sum(row[1] for row in rows)


def test_time_series_is_downsampled_and_keeps_the_last_point(ctx: ToolContext) -> None:
    start = date(2026, 1, 1)
    rows = [
        [(start + timedelta(days=i)).isoformat(), i]
        for i in range(CHART_MAX_POINTS + 120)
    ]
    visualize_chart("line_chart", "Long series", data=rows, columns=["period", "v"])

    widget_data = _chart(ctx)["widgetData"]
    assert len(widget_data) <= CHART_MAX_POINTS
    assert widget_data[-1]["period"] == rows[-1][0]


def test_series_column_is_capped(ctx: ToolContext) -> None:
    rows = [[f"p{i}", i, f"series-{i}"] for i in range(CHART_MAX_SERIES + 5)]
    visualize_chart(
        "grouped_bar_chart", "Many series", data=rows, columns=["p", "v", "s"]
    )

    series = {row["s"] for row in _chart(ctx)["widgetData"]}
    assert len(series) == CHART_MAX_SERIES


def test_applied_limits_are_reported_to_the_agent(ctx: ToolContext) -> None:
    rows = [[f"cat-{i}", i] for i in range(CHART_TOP_N + 8)]
    out = visualize_chart("bar_chart", "Many categories", data=rows, columns=["k", "v"])

    assert "Applied limits" in out
    assert OTHERS_LABEL in out
