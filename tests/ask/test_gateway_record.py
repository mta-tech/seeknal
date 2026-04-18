"""Tests for the gateway POST /record endpoint."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

pytest.importorskip("starlette")
from starlette.testclient import TestClient

from seeknal.ask.gateway.server import create_gateway_app


async def _fake_stream_single_answer(*args, **kwargs):
    # Minimal async generator that emits a single answer event — mimics the
    # shape of _run_agent_streaming without spinning up a real agent.
    yield {"type": "answer", "data": "Recorded one row into ingest_orders."}


def test_post_record_happy_path(tmp_path):
    app = create_gateway_app(project_path=tmp_path)
    with TestClient(app) as client, patch(
        "seeknal.ask.gateway.server._run_agent_streaming",
        side_effect=_fake_stream_single_answer,
    ):
        res = client.post(
            "/record",
            json={"text": "/record fitra, 1 mie ayam", "session_id": "t1"},
        )
    assert res.status_code == 200, res.text
    payload = res.json()
    assert "Recorded" in payload["answer"]
    assert any(e["type"] == "answer" for e in payload["events"])


def test_post_record_rejects_missing_text(tmp_path):
    app = create_gateway_app(project_path=tmp_path)
    with TestClient(app) as client:
        res = client.post("/record", json={"session_id": "t1"})
    assert res.status_code == 400
    assert "text" in res.json()["error"]


def test_post_record_rejects_bad_session_id(tmp_path):
    app = create_gateway_app(project_path=tmp_path)
    with TestClient(app) as client:
        res = client.post(
            "/record",
            json={"text": "/record x", "session_id": "has spaces!!"},
        )
    assert res.status_code == 400


def test_post_record_backend_only_returns_404(tmp_path):
    # Backend-only mode does not register /record
    app = create_gateway_app(project_path=None, sessions_dir=tmp_path)
    with TestClient(app) as client:
        res = client.post("/record", json={"text": "x"})
    assert res.status_code in (404, 405)


def test_upload_image_returns_kind_image(tmp_path):
    app = create_gateway_app(project_path=tmp_path)
    body = b"\x89PNG\r\n\x1a\n" + b"\x00" * 64
    with TestClient(app) as client:
        res = client.post(
            "/upload",
            files={"file": ("proof.png", body, "image/png")},
        )
    assert res.status_code == 200, res.text
    payload = res.json()
    assert payload["kind"] == "image"
    assert payload["filename"] == "proof.png"
    assert "POST /record" in payload["next"]
    assert Path(payload["path"]).exists()


def test_upload_image_over_limit_rejected(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "seeknal.ask.gateway.server._IMAGE_MAX_BYTES", 32
    )
    app = create_gateway_app(project_path=tmp_path)
    with TestClient(app) as client:
        res = client.post(
            "/upload",
            files={"file": ("huge.jpg", b"x" * 1024, "image/jpeg")},
        )
    assert res.status_code == 413
