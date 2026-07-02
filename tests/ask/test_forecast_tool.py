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
    for header in [
        "## Ringkasan",
        "## Kualitas Proyeksi",
        "## Kondisi Data",
        "## Historis & Proyeksi",
        "## Proyeksi Detail",
        "## Rentang Realistis",
        "## Tingkat Keyakinan",
        "## Metodologi",
    ]:
        assert header in out, f"missing {header}"
    assert "BAIK" in out
    assert "ETS(A,N,N)" in out


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
