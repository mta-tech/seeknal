"""Tests for the gateway POST /upload endpoint."""

from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("starlette")

from starlette.testclient import TestClient

from seeknal.ask.gateway.server import create_gateway_app


def _app(project_path: Path):
    return create_gateway_app(project_path=project_path)


def test_upload_multipart_roundtrip(tmp_path):
    app = _app(tmp_path)
    with TestClient(app) as client:
        body = b"invoice_id,date,amount\n1,2025-01-01,100\n"
        res = client.post(
            "/upload",
            files={"file": ("jan.csv", body, "text/csv")},
        )
    assert res.status_code == 200, res.text
    payload = res.json()
    assert payload["filename"] == "jan.csv"
    assert payload["size"] == len(body)

    staged = Path(payload["path"])
    assert staged.exists()
    assert staged.read_bytes() == body
    assert staged.is_relative_to(tmp_path / "target" / "ask_ingest" / "_staging")


def test_upload_rejects_unsupported_extension(tmp_path):
    app = _app(tmp_path)
    with TestClient(app) as client:
        res = client.post(
            "/upload",
            files={"file": ("evil.exe", b"MZ", "application/octet-stream")},
        )
    assert res.status_code == 400
    assert "unsupported" in res.json()["error"].lower()


def test_upload_size_limit(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "seeknal.ask.gateway.server._UPLOAD_MAX_BYTES", 16
    )
    app = _app(tmp_path)
    with TestClient(app) as client:
        res = client.post(
            "/upload",
            files={"file": ("big.csv", b"x" * 1024, "text/csv")},
        )
    assert res.status_code == 413
    assert "200 MB" in res.json()["error"] or "limit" in res.json()["error"]


def test_upload_rejects_when_backend_only(tmp_path):
    app = create_gateway_app(project_path=None, sessions_dir=tmp_path)
    with TestClient(app) as client:
        res = client.post(
            "/upload",
            files={"file": ("jan.csv", b"a,b\n1,2\n", "text/csv")},
        )
    # backend-only mode does not register /upload; Starlette returns 404
    assert res.status_code in (404, 405)
