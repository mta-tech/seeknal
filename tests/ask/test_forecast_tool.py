"""Tests for the run_forecast tool — validation gates, freq inference, markdown.

The engine HTTP call is mocked so these tests run without the IBA forecast
service. ``_REPLStub`` mirrors ``tests/ask/test_execute_sql.py``: a real
in-memory DuckDB so the EXECUTE step exercises a genuine SQL path.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from seeknal.ask.agents.tools._context import ToolContext, set_tool_context
from seeknal.ask.agents.tools.forecast import run_forecast


class _REPLStub:
    def __init__(self) -> None:
        self.conn = duckdb.connect(":memory:")

    def execute_oneshot(self, sql: str, limit=None):
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
    # 30-month monthly series (stationary ~5000/mo) — above the 24-period engine floor.
    repl.conn.execute(
        """
        CREATE TABLE monthly AS
        SELECT CAST(d AS DATE) AS d,
               5000 + ((ROW_NUMBER() OVER () - 1) % 7) * 13 AS y
        FROM generate_series(DATE '2022-01-01', DATE '2024-06-01', INTERVAL '1 month') AS t(d)
        """
    )
    ctx = ToolContext(
        repl=repl,
        artifact_discovery=MagicMock(),
        project_path=tmp_path,
        sql_timeout_seconds=0,
        # Engine connectivity: injected here exactly like a real session
        # (agent.py resolves these once at init and sets them on ToolContext);
        # run_forecast never reads os.environ itself. See
        # test_forecast_engine_not_configured for the unset-config path.
        iba_engine_url="http://test-engine",
        iba_engine_api_key="test-key",
    )
    set_tool_context(ctx)
    return ctx


def _ok_response(periods: int = 3) -> dict[str, Any]:
    return {
        "status": "ok",
        "reason": None,
        "forecast": {
            "first_period": "2025-01-01",
            "periods": periods,
            "points": [
                {
                    "period": "2025-01-01",
                    "point": 5022,
                    "lower_80": 4400,
                    "upper_80": 5644,
                    "lower_95": 4100,
                    "upper_95": 5944,
                },
                {
                    "period": "2025-02-01",
                    "point": 5022,
                    "lower_80": 4300,
                    "upper_80": 5744,
                    "lower_95": 3950,
                    "upper_95": 6094,
                },
                {
                    "period": "2025-03-01",
                    "point": 5022,
                    "lower_80": 4200,
                    "upper_80": 5844,
                    "lower_95": 3800,
                    "upper_95": 6244,
                },
            ],
        },
        "assessment": {
            "quality_label": "BAIK",
            "metrics": {"mape": 7.0, "mae": 343.5, "mase": 1.05, "coverage_80": None, "coverage_95": None},
            "eligibility": {"n_months": 30, "avg_monthly": 5039.0, "gap_months": 0, "passed": True, "reason": None},
        },
        "provenance": {
            "model": "autoets",
            "sub_type": "ETS(A,N,N)",
            "training_window": "2022-01-01..2024-06-01",
            "sigma": 328.5,
        },
    }


def _mock_post(response_json: dict[str, Any]):
    """Build a MagicMock standing in for httpx.post."""
    resp = MagicMock()
    resp.json.return_value = response_json
    resp.raise_for_status.return_value = None
    return resp


SQL_2COL = (
    "SELECT date_trunc('month', d::timestamp) AS x, y "
    "FROM monthly ORDER BY 1"
)


def test_forecast_ok_formats_7_blocks(ctx):
    with patch("seeknal.ask.agents.tools.forecast.httpx.post", return_value=_mock_post(_ok_response())):
        out = run_forecast(SQL_2COL, periods=3)
    # r5: simplified output — single continuous table under "## Proyeksi"
    for header in [
        "## Proyeksi",
        "| Period | Value | Lower 80% | Upper 80% |",
    ]:
        assert header in out, f"missing {header}"
    assert "BAIK" in out
    assert "Quality:" in out


def test_forecast_engine_not_configured(ctx):
    """No IBA_ENGINE_URL resolved at session init ->
    clear config error, never a guess at a docker-internal hostname, and no
    SQL/network attempt.
    """
    ctx.iba_engine_url = None
    with patch("seeknal.ask.agents.tools.forecast.httpx.post") as post:
        out = run_forecast(SQL_2COL, periods=3)
    assert "## Kesalahan" in out
    assert "belum dikonfigurasi" in out
    post.assert_not_called()


def test_forecast_validates_two_columns(ctx):
    # 3-column SQL → Kesalahan, no engine call.
    sql = "SELECT d AS x, y, d AS z FROM monthly ORDER BY 1"
    with patch("seeknal.ask.agents.tools.forecast.httpx.post") as post:
        out = run_forecast(sql, periods=3)
    assert "## Kesalahan" in out
    assert "tepat 2 kolom" in out
    post.assert_not_called()


def test_forecast_validates_min_rows(ctx):
    # Only 8 rows → below the 10-row sendability floor.
    sql = (
        "SELECT date_trunc('month', d::timestamp) AS x, y "
        "FROM (SELECT * FROM monthly ORDER BY d LIMIT 8) ORDER BY 1"
    )
    with patch("seeknal.ask.agents.tools.forecast.httpx.post") as post:
        out = run_forecast(sql, periods=3)
    assert "## Kesalahan" in out
    assert "minimal 10 baris" in out
    post.assert_not_called()


def test_forecast_engine_refusal_returns_ditolak(ctx):
    refused = {
        "status": "refused",
        "reason": "Data tidak cukup (tersedia 12 periode, minimum 24)",
        "forecast": None,
        "assessment": None,
        "provenance": None,
    }
    with patch("seeknal.ask.agents.tools.forecast.httpx.post", return_value=_mock_post(refused)):
        out = run_forecast(SQL_2COL, periods=3)
    assert "## Ditolak" in out
    assert "minimum 24" in out


def test_forecast_engine_unreachable_returns_kesalahan(ctx):
    import httpx as _httpx

    with patch("seeknal.ask.agents.tools.forecast.httpx.post", side_effect=_httpx.RequestError("boom")):
        out = run_forecast(SQL_2COL, periods=3)
    assert "## Kesalahan" in out
    assert "tidak tersedia" in out


def test_forecast_periods_clamped(ctx):
    captured = {}

    def fake_post(url, json=None, headers=None, timeout=None):
        captured["periods"] = json["periods"]
        return _mock_post(_ok_response())

    with patch("seeknal.ask.agents.tools.forecast.httpx.post", side_effect=fake_post):
        run_forecast(SQL_2COL, periods=99)
    assert captured["periods"] == 12  # clamped to _MAX_HORIZON


# ── r4: structural policy check (STEP 0.5) + SQL exec error handling (STEP 1) ──


def test_forecast_policy_check_rejects_join(ctx):
    """SQL containing a JOIN is rejected at STEP 0.5 BEFORE DuckDB is called.

    This is the regression test for the production crash:
    ``Conversion Error: Unimplemented type for cast (TIMESTAMP -> TIMESTAMP[])``
    on ``LEFT JOIN actuals a ON m.x = a.x``. The structural pre-check prevents
    DuckDB from ever seeing the query, so the systematic type-mismatch cannot
    recur regardless of how DuckDB phrases its error message.
    """
    # Mirror of the production failure: a date-spine LEFT JOIN with date_trunc.
    sql = (
        "SELECT m.x, a.y FROM monthly m "
        "LEFT JOIN monthly a ON m.x = a.x"
    )
    with patch("seeknal.ask.agents.tools.forecast.httpx.post") as post:
        out = run_forecast(sql, periods=3)
    assert "## Kesalahan" in out
    assert "STEP 0.5" in out  # policy check, not STEP 1 exec error
    assert "JOIN" in out  # hint mentions JOIN
    # Engine is never contacted (no POST), and no DuckDB execution needed.
    post.assert_not_called()


def test_forecast_policy_check_rejects_generate_series(ctx):
    """SQL containing generate_series is rejected at STEP 0.5.

    Mirrors RECIPE-F5/F6 legacy pattern. The engine counts gaps internally;
    SQL gap-filling is unnecessary AND triggers DuckDB type errors when mixed
    with date_trunc.
    """
    sql = (
        "SELECT date_trunc('month', d) AS x, 1 AS y "
        "FROM generate_series(DATE '2024-01-01', DATE '2024-12-01', "
        "INTERVAL '1 month') AS t(d)"
    )
    with patch("seeknal.ask.agents.tools.forecast.httpx.post") as post:
        out = run_forecast(sql, periods=3)
    assert "## Kesalahan" in out
    assert "STEP 0.5" in out
    assert "GENERATE_SERIES" in out.upper() or "generate_series" in out
    post.assert_not_called()


def test_forecast_sql_execution_error_returns_kesalahan(ctx):
    """SQL that passes the policy check but fails at DuckDB execution (STEP 1)
    returns a Kesalahan block — does NOT raise / crash the activity.

    Before r4: a runtime DuckDB exception propagated as raw
    ``{"error": "..."}`` JSON, breaking the Temporal activity. After r4: the
    exception is caught and formatted as a structured English Kesalahan block
    that the agent can self-correct from in the next loop iteration.

    Uses a SELECT against a nonexistent table — no JOIN / generate_series, so
    it passes STEP 0.5, but DuckDB raises ``Catalog Error`` at execution.
    """
    sql = (
        "SELECT date_trunc('month', d::timestamp) AS x, y "
        "FROM nonexistent_table_xyz ORDER BY 1"
    )
    with patch("seeknal.ask.agents.tools.forecast.httpx.post") as post:
        out = run_forecast(sql, periods=3)
    assert "## Kesalahan" in out
    assert "STEP 1" in out  # exec error, not policy check
    assert "Error type:" in out  # structured debug info
    # Engine is never contacted because execution failed first.
    post.assert_not_called()


def test_forecast_accepts_dynamic_schemas(ctx):
    """The tool is content-agnostic: SQLs with different table/value/filter
    content (but identical 2-column shape) all succeed end-to-end.

    Proves the agentic dynamic-SQL model works: the agent picks tables,
    columns, filters per question (per forecast_guide.md §1/§7), and the
    tool handles any flat 2-column input. This is the design invariant
    called out in FC2a r2: tool owns SHAPE, agent owns CONTENT.
    """
    # Schema A: COUNT(*) aggregate.
    sql_a = (
        "SELECT date_trunc('month', d::timestamp) AS x, COUNT(*) AS y "
        "FROM monthly GROUP BY 1 ORDER BY 1"
    )
    # Schema B: SUM(y) aggregate — same shape, different value expression.
    sql_b = (
        "SELECT date_trunc('month', d::timestamp) AS x, SUM(y) AS y "
        "FROM monthly GROUP BY 1 ORDER BY 1"
    )
    # Schema C: MAX(y) per quarter — same shape, different grain + aggregate.
    sql_c = (
        "SELECT date_trunc('quarter', d::timestamp) AS x, MAX(y) AS y "
        "FROM monthly GROUP BY 1 ORDER BY 1"
    )

    with patch(
        "seeknal.ask.agents.tools.forecast.httpx.post",
        return_value=_mock_post(_ok_response(periods=3)),
    ) as post:
        out_a = run_forecast(sql_a, periods=3)
        out_b = run_forecast(sql_b, periods=3)
        out_c = run_forecast(sql_c, periods=3)

    for label, out in (("A", out_a), ("B", out_b), ("C", out_c)):
        assert "## Proyeksi" in out, f"schema {label} did not produce r5 table output"
    # Engine called once per successful run — proves all three shapes reached it.
    assert post.call_count == 3
