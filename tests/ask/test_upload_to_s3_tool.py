"""Tests for the upload_to_s3 tool — CSV build, presigned-URL flow, pending_upload.

httpx calls are mocked so the tests run without iba-storage / SeaweedFS.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from seeknal.ask.agents.tools._context import ToolContext, set_tool_context
from seeknal.ask.agents.tools.upload_to_s3 import upload_to_s3


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
    repl.conn.execute("CREATE TABLE data AS SELECT i AS id, 'n' || i AS v FROM generate_series(0, 4) AS t(i)")
    ctx = ToolContext(
        repl=repl,
        artifact_discovery=MagicMock(),
        project_path=tmp_path,
        sql_timeout_seconds=0,
        request_limit=100,
        # Storage connectivity injected here exactly like a real session
        # (agent.py resolves these once at init and sets them on ToolContext);
        # upload_to_s3 never reads os.environ itself. See
        # test_upload_storage_unreachable_returns_error for the unset path.
        iba_storage_presign_url="http://storage/presign",
        iba_storage_api_key="test-key",
    )
    set_tool_context(ctx)
    return ctx


def _presign_response() -> dict:
    return {
        "upload_url": "http://storage/put/abc",
        "upload_url_expires_at": 1900000000,
        "download_url": "http://storage/get/abc/file.csv",
        "download_url_expires_at": 1900000000,
        "object_name": "csv-exports/abc/file.csv",
    }


def _mock_response(json_body: dict):
    resp = MagicMock()
    resp.json.return_value = json_body
    resp.raise_for_status.return_value = None
    return resp


SQL = "SELECT id, v FROM data"


def test_upload_sets_pending_upload_with_server_expiry(ctx):
    presign = _mock_response(_presign_response())
    put = MagicMock()
    put.raise_for_status.return_value = None
    with patch("seeknal.ask.agents.tools.upload_to_s3.httpx.post", return_value=presign), patch(
        "seeknal.ask.agents.tools.upload_to_s3.httpx.put", return_value=put
    ):
        out = upload_to_s3("file.csv", SQL)
    assert "Upload complete" in out
    assert ctx.pending_upload is not None
    # Mode 1 filenames are now derived from the user's question (falls back
    # to the SQL's table name when no question is set, as here) rather than
    # passed through verbatim — see test_filename_derived_from_question
    # below for the question-set case.
    assert ctx.pending_upload["file_name"].startswith("data-")
    assert ctx.pending_upload["file_name"].endswith(".csv")
    # expires_at must be sourced from the server's download_url_expires_at (ISO).
    assert ctx.pending_upload["object_name"] == "csv-exports/abc/file.csv"
    assert ctx.pending_upload["expires_at"].startswith("2030")  # 1900000000 ~ 2030-03


def test_filename_derived_from_question(ctx):
    """Mode 1 prefers a slug of ctx.current_question over the passed filename."""
    ctx.current_question = "What was the monthly trend last year?"
    presign = _mock_response(_presign_response())
    put = MagicMock()
    put.raise_for_status.return_value = None
    with patch("seeknal.ask.agents.tools.upload_to_s3.httpx.post", return_value=presign), patch(
        "seeknal.ask.agents.tools.upload_to_s3.httpx.put", return_value=put
    ):
        upload_to_s3("ignored-name.csv", SQL)
    file_name = ctx.pending_upload["file_name"]
    assert file_name.startswith("what-was-the-monthly-trend-last-year-")
    assert file_name.endswith(".csv")


def test_upload_storage_unreachable_returns_error(ctx):
    import httpx as _httpx

    with patch("seeknal.ask.agents.tools.upload_to_s3.httpx.post", side_effect=_httpx.RequestError("boom")):
        out = upload_to_s3("file.csv", SQL)
    assert "Storage unavailable" in out
    assert ctx.pending_upload is None


def test_upload_empty_result_returns_nothing(ctx):
    repl = ctx.repl
    repl.conn.execute("CREATE TABLE empty AS SELECT * FROM data WHERE 1=0")
    out = upload_to_s3("file.csv", "SELECT * FROM empty")
    assert "No rows" in out
    assert ctx.pending_upload is None


def test_upload_no_domain_strings():
    # Grep contract: the tool must contain no BPOM/domain terms as standalone
    # words. (Word boundaries avoid false positives like "erba" inside "verbatim".)
    import re

    import seeknal.ask.agents.tools.upload_to_s3 as m

    src = open(m.__file__).read()
    for bad in ("bpom", "erba", "trader_id", "tanggal_bayar"):
        assert not re.search(rf"\b{re.escape(bad)}\b", src, re.IGNORECASE), (
            f"domain term {bad!r} found in upload_to_s3.py"
        )


# ── r4: dual-mode (sql= | data=+columns=) ──────────────────────────────────


def test_upload_data_mode_builds_csv_and_sets_pending_upload(ctx):
    """T2: Mode 2 — agent provides computed rows + columns directly.

    Mirrors the forecast-points use case: forecast.points are computed by the
    engine, not SQL-queryable, so the agent must be able to pass them straight
    to upload_to_s3 instead of going through a SQL round-trip.
    """
    presign = _mock_response(_presign_response())
    put = MagicMock()
    put.raise_for_status.return_value = None

    forecast_points = [
        ["2026-07-01", 5890, 5100, 6680, 4800, 6980],
        ["2026-08-01", 5890, 4952, 6828, 4598, 7182],
        ["2026-09-01", 5890, 4824, 6956, 4430, 7350],
    ]
    cols = ["period", "point", "lower_80", "upper_80", "lower_95", "upper_95"]

    captured_put_bytes = []

    def _capture_put(url, content=None, **kw):
        captured_put_bytes.append(content)
        m = MagicMock()
        m.raise_for_status.return_value = None
        return m

    with patch("seeknal.ask.agents.tools.upload_to_s3.httpx.post", return_value=presign), patch(
        "seeknal.ask.agents.tools.upload_to_s3.httpx.put", side_effect=_capture_put
    ):
        out = upload_to_s3("forecast-2026-07.csv", data=forecast_points, columns=cols)

    assert "Upload complete" in out
    assert ctx.pending_upload is not None
    assert ctx.pending_upload["file_name"] == "forecast-2026-07.csv"
    assert ctx.pending_upload["file_size"] > 0

    # Verify the CSV actually contains the provided rows + header (not a SQL
    # execution, not empty). This is the byte stream PUT to SeaweedFS.
    csv_text = captured_put_bytes[0].decode("utf-8")
    assert "period,point,lower_80,upper_80,lower_95,upper_95" in csv_text
    assert "2026-07-01,5890,5100,6680,4800,6980" in csv_text
    assert "2026-09-01" in csv_text


def test_upload_data_mode_no_sql_call(ctx):
    """T2b: Mode 2 must NOT touch the SQL layer — no DuckDB execution.

    The whole point of data mode is that the agent already has the rows
    (computed in-process). Calling _execute_oneshot_with_timeout would fail
    or be wasteful. Verify by spying on ctx.repl.execute_oneshot.
    """
    presign = _mock_response(_presign_response())
    put = MagicMock()
    put.raise_for_status.return_value = None

    ctx.repl.execute_oneshot = MagicMock(return_value=([], []))

    with patch("seeknal.ask.agents.tools.upload_to_s3.httpx.post", return_value=presign), patch(
        "seeknal.ask.agents.tools.upload_to_s3.httpx.put", return_value=put
    ):
        upload_to_s3("f.csv", data=[[1, 2], [3, 4]], columns=["a", "b"])

    ctx.repl.execute_oneshot.assert_not_called()


def test_upload_missing_both_modes_returns_kesalahan(ctx):
    """T3: calling upload_to_s3(filename) without sql= or data= → clear Kesalahan."""
    out = upload_to_s3("file.csv")
    assert "## Kesalahan" in out
    assert "missing data source" in out.lower() or "missing" in out.lower()
    assert ctx.pending_upload is None


def test_upload_conflicting_modes_returns_kesalahan(ctx):
    """T3b: providing BOTH sql= and data= → clear Kesalahan (mutually exclusive)."""
    out = upload_to_s3("file.csv", sql="SELECT 1", data=[[1]], columns=["a"])
    assert "## Kesalahan" in out
    assert "conflict" in out.lower() or "mutually" in out.lower()
    assert ctx.pending_upload is None


def test_upload_data_mode_missing_columns_returns_kesalahan(ctx):
    """T3c: data= without columns= → Kesalahan with the fix."""
    out = upload_to_s3("file.csv", data=[[1, 2]])
    assert "## Kesalahan" in out
    assert "columns" in out.lower()
    assert ctx.pending_upload is None


def test_upload_data_mode_jagged_row_returns_kesalahan(ctx):
    """T3d: rows with mismatched lengths → Kesalahan (reject malformed CSV early)."""
    out = upload_to_s3(
        "file.csv",
        data=[[1, 2], [3, 4], [5]],  # third row has 1 cell, not 2
        columns=["a", "b"],
    )
    assert "## Kesalahan" in out
    assert "row 2" in out  # 0-indexed
    assert ctx.pending_upload is None


def test_upload_data_mode_empty_rows_returns_nothing(ctx):
    """T3e: data=[] → no rows to export, no PUT, no pending_upload."""
    out = upload_to_s3("file.csv", data=[], columns=["a"])
    assert "No rows" in out
    assert ctx.pending_upload is None
