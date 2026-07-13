"""Tests for the detect_anomaly tool — URL resolution, validation gates, markdown.

The engine HTTP call is mocked so these tests run without the IBA forecast
service. Mirrors tests/ask/test_forecast_tool.py: same ``_REPLStub`` pattern,
same engine (detect_anomaly is a sibling of run_forecast, same 2-column
SQL contract and execution pipeline).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from seeknal.ask.agents.tools._context import ToolContext, set_tool_context
from seeknal.ask.agents.tools.anomaly import detect_anomaly


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
        # detect_anomaly never reads os.environ itself. See
        # test_anomaly_engine_not_configured for the unset-config path.
        iba_engine_url="http://test-engine",
        iba_engine_api_key="test-key",
    )
    set_tool_context(ctx)
    return ctx


def _mock_post(response_json: dict[str, Any]):
    resp = MagicMock()
    resp.json.return_value = response_json
    resp.raise_for_status.return_value = None
    return resp


SQL_2COL = (
    "SELECT date_trunc('month', d::timestamp) AS x, y "
    "FROM monthly ORDER BY 1"
)


def test_anomaly_found_formats_markdown(ctx):
    response = {
        "status": "ok",
        "anomalies": [
            {"index": 5, "date": "2022-06-01", "value": 9000, "modified_z": 4.1,
             "reason_hint": "isolated spike"},
        ],
    }
    with patch("seeknal.ask.agents.tools.anomaly.httpx.post", return_value=_mock_post(response)):
        out = detect_anomaly(SQL_2COL)
    assert "## Anomali" in out
    assert "2022-06-01" in out
    assert "isolated spike" in out
    assert "Terdeteksi 1 periode anomali." in out


def test_anomaly_none_found(ctx):
    response = {"status": "ok", "anomalies": []}
    with patch("seeknal.ask.agents.tools.anomaly.httpx.post", return_value=_mock_post(response)):
        out = detect_anomaly(SQL_2COL)
    assert "## Anomali" in out
    assert "Tidak ada periode yang menonjol" in out


def test_anomaly_engine_not_configured(ctx):
    """No IBA_ENGINE_URL resolved at session init ->
    clear config error, never a guess at a docker-internal hostname, and no
    SQL/network attempt.
    """
    ctx.iba_engine_url = None
    with patch("seeknal.ask.agents.tools.anomaly.httpx.post") as post:
        out = detect_anomaly(SQL_2COL)
    assert "## Kesalahan" in out
    assert "belum dikonfigurasi" in out
    post.assert_not_called()


def test_anomaly_validates_two_columns(ctx):
    sql = "SELECT d AS x, y, d AS z FROM monthly ORDER BY 1"
    with patch("seeknal.ask.agents.tools.anomaly.httpx.post") as post:
        out = detect_anomaly(sql)
    assert "## Kesalahan" in out
    assert "tepat 2 kolom" in out
    post.assert_not_called()


def test_anomaly_validates_min_rows(ctx):
    sql = (
        "SELECT date_trunc('month', d::timestamp) AS x, y "
        "FROM (SELECT * FROM monthly ORDER BY d LIMIT 2) ORDER BY 1"
    )
    with patch("seeknal.ask.agents.tools.anomaly.httpx.post") as post:
        out = detect_anomaly(sql)
    assert "## Kesalahan" in out
    assert "minimal 3 baris" in out
    post.assert_not_called()


def test_anomaly_engine_unreachable_returns_kesalahan(ctx):
    import httpx as _httpx

    with patch("seeknal.ask.agents.tools.anomaly.httpx.post", side_effect=_httpx.RequestError("boom")):
        out = detect_anomaly(SQL_2COL)
    assert "## Kesalahan" in out
    assert "tidak tersedia" in out


def test_anomaly_policy_check_rejects_join(ctx):
    sql = "SELECT m.x, a.y FROM monthly m LEFT JOIN monthly a ON m.x = a.x"
    with patch("seeknal.ask.agents.tools.anomaly.httpx.post") as post:
        out = detect_anomaly(sql)
    assert "## Kesalahan" in out
    assert "JOIN" in out
    post.assert_not_called()
