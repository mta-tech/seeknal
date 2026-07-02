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
    # expires_at must be sourced from the server's download_url_expires_at (ISO).
    assert ctx.pending_upload["file_name"] == "file.csv"
    assert ctx.pending_upload["object_name"] == "csv-exports/abc/file.csv"
    assert ctx.pending_upload["expires_at"].startswith("2030")  # 1900000000 ~ 2030-03


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
