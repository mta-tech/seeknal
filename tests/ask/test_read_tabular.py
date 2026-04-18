"""Tests for read_tabular tool."""

from __future__ import annotations

import http.server
import json
import threading
from pathlib import Path
from unittest.mock import MagicMock

import duckdb
import pytest

from seeknal.ask.agents.tools._context import ToolContext, set_tool_context
from seeknal.ask.agents.tools.read_tabular import read_tabular


class _REPLStub:
    def __init__(self) -> None:
        self.conn = duckdb.connect(":memory:")


@pytest.fixture
def ctx(tmp_path: Path) -> ToolContext:
    context = ToolContext(
        repl=_REPLStub(),
        artifact_discovery=MagicMock(),
        project_path=tmp_path,
    )
    set_tool_context(context)
    return context


def _write_csv(path: Path) -> int:
    path.write_text(
        "invoice_id,date,amount\n1,2025-01-01,100\n2,2025-01-02,200\n",
        encoding="utf-8",
    )
    return 2


def _write_tsv(path: Path) -> int:
    path.write_text(
        "a\tb\tc\n1\t2\t3\n4\t5\t6\n",
        encoding="utf-8",
    )
    return 2


def _write_json(path: Path) -> int:
    rows = [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}, {"id": 3, "v": "c"}]
    path.write_text(json.dumps(rows), encoding="utf-8")
    return len(rows)


def test_csv_parse(tmp_path, ctx):
    csv = tmp_path / "sales.csv"
    _write_csv(csv)

    result = read_tabular(str(csv))
    assert "## Preview: sales.csv" in result
    assert "**Rows:** 2" in result
    assert "invoice_id" in result
    assert ctx.last_read_staging_path == str(csv.resolve())


def test_tsv_parse(tmp_path, ctx):
    tsv = tmp_path / "sample.tsv"
    _write_tsv(tsv)

    result = read_tabular(str(tsv))
    assert "**Rows:** 2" in result
    assert "## Preview: sample.tsv" in result


def test_json_parse(tmp_path, ctx):
    j = tmp_path / "items.json"
    _write_json(j)

    result = read_tabular(str(j))
    assert "## Preview: items.json" in result
    assert "**Rows:** 3" in result


def test_xlsx_parse(tmp_path, ctx):
    openpyxl = pytest.importorskip("openpyxl")
    import pandas as pd

    xlsx = tmp_path / "monthly.xlsx"
    df = pd.DataFrame({"id": [1, 2, 3], "amount": [10.0, 20.0, 30.0]})
    df.to_excel(xlsx, index=False, engine="openpyxl")

    result = read_tabular(str(xlsx))
    assert "## Preview: monthly.xlsx" in result
    assert "**Rows:** 3" in result
    # xlsx is converted to a staging parquet, so parsed path differs
    assert "Parsed as parquet at:" in result


def test_unsupported_format(tmp_path, ctx):
    pdf = tmp_path / "bad.pdf"
    pdf.write_bytes(b"%PDF-1.4 not really a pdf")
    out = read_tabular(str(pdf))
    assert out.startswith("Error:")
    assert ".pdf" in out


def test_file_not_found(tmp_path, ctx):
    out = read_tabular(str(tmp_path / "missing.csv"))
    assert out.startswith("Error:")
    assert "not found" in out


def test_file_size_limit(tmp_path, ctx, monkeypatch):
    csv = tmp_path / "big.csv"
    csv.write_text("a,b\n1,2\n", encoding="utf-8")

    # Pretend the file is huge
    monkeypatch.setattr(
        "seeknal.ask.agents.tools.read_tabular._MAX_FILE_BYTES", 1
    )
    out = read_tabular(str(csv))
    assert out.startswith("Error:")
    assert "200 MB" in out


def test_url_rejects_private_ip(tmp_path, ctx):
    # 127.0.0.1 is loopback and must be rejected by the SSRF guard.
    out = read_tabular("http://127.0.0.1:1/file.csv")
    assert out.startswith("Error:")
    assert "non-public" in out or "refusing" in out


def test_url_rejects_link_local_metadata(tmp_path, ctx):
    # AWS/GCP IMDS endpoint — must be refused.
    out = read_tabular("http://169.254.169.254/latest/meta-data/")
    assert out.startswith("Error:")
    assert "non-public" in out or "refusing" in out


def test_url_download(tmp_path, ctx):
    body = b"invoice_id,date,amount\n10,2025-01-01,99\n11,2025-01-02,100\n"

    class _Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            self.send_response(200)
            self.send_header("Content-Type", "text/csv")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args, **kwargs):
            return

    server = http.server.HTTPServer(("127.0.0.1", 0), _Handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    # Patch the SSRF guard to allow 127.0.0.1 just for this test. The guard's
    # production-time purpose is to block metadata / internal services; we
    # exercise the full download path over loopback.
    import seeknal.ask.agents.tools.read_tabular as mod
    real_reject = mod._reject_if_private
    mod._reject_if_private = lambda host: None
    try:
        url = f"http://127.0.0.1:{port}/download.csv"
        result = read_tabular(url)
        assert "**Rows:** 2" in result
        # staging path should exist under target/ask_ingest/_staging/
        assert "target/ask_ingest/_staging" in ctx.last_read_staging_path
    finally:
        mod._reject_if_private = real_reject
        server.shutdown()
        thread.join(timeout=2)


def test_content_disposition_path_traversal_blocked(tmp_path, ctx, monkeypatch):
    """A malicious server returning filename="../../.env" must not escape staging."""
    body = b"invoice_id,date,amount\n10,2025-01-01,99\n"

    class _EvilHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            self.send_response(200)
            self.send_header("Content-Type", "text/csv")
            # Attacker-controlled filename with traversal
            self.send_header(
                "Content-Disposition",
                'attachment; filename="../../../../etc/passwd.csv"',
            )
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args, **kwargs):
            return

    server = http.server.HTTPServer(("127.0.0.1", 0), _EvilHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    import seeknal.ask.agents.tools.read_tabular as mod
    monkeypatch.setattr(mod, "_reject_if_private", lambda host: None)
    try:
        url = f"http://127.0.0.1:{port}/download.csv"
        result = read_tabular(url)
        # Either way the request must succeed — but the staged file must live
        # under the UUID staging dir, never at ../../etc/passwd.csv on disk.
        assert "**Rows:** 1" in result
        staged = Path(ctx.last_read_staging_path)
        # The staged path must be inside the project tree
        assert staged.is_relative_to(tmp_path / "target" / "ask_ingest" / "_staging"), (
            f"traversal escaped containment: {staged}"
        )
        # The dangerous absolute path must not have been created
        assert not Path("/etc/passwd.csv").exists() or (
            Path("/etc/passwd.csv").exists()
            and Path("/etc/passwd.csv").stat().st_size != len(body)
        )
    finally:
        server.shutdown()
        thread.join(timeout=2)
