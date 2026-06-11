"""
Tests for ``seeknal gov request-access``.

These exercise the CLI command in isolation by monkeypatching ``httpx.post`` so no
network call is made. The behaviours under test: the POST body carries the expected
keys, the created request id is printed on a 201, and a non-2xx response exits 1.
"""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest
from typer.testing import CliRunner

from seeknal.cli import gov as gov_module
from seeknal.cli.main import app

runner = CliRunner()


def _captured_post(
    captured: dict[str, Any],
    *,
    status_code: int,
    json_body: dict[str, Any] | None = None,
    text: str = "",
):
    """Build a fake ``httpx.post`` that records its call and returns a canned response."""

    def fake_post(url: str, *, json: dict[str, Any], headers: dict[str, str], timeout: float):
        captured["url"] = url
        captured["json"] = json
        captured["headers"] = headers
        captured["timeout"] = timeout
        request = httpx.Request("POST", url)
        if json_body is not None:
            return httpx.Response(status_code, json=json_body, request=request)
        return httpx.Response(status_code, text=text, request=request)

    return fake_post


def test_request_access_posts_expected_body(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SEEKNAL_API_URL", "https://atlas.example.com")
    monkeypatch.delenv("ATLAS_API_URL", raising=False)
    monkeypatch.setattr(gov_module, "user_email_from_credentials", lambda: "alice@example.com")
    monkeypatch.setattr(gov_module, "user_token_from_credentials", lambda: "tok-123")

    captured: dict[str, Any] = {}
    monkeypatch.setattr(
        httpx,
        "post",
        _captured_post(
            captured,
            status_code=201,
            json_body={"id": "AR-7", "status": "pending"},
        ),
    )

    result = runner.invoke(
        app,
        [
            "gov",
            "request-access",
            "warehouse.namespace.table",
            "--reason",
            "need it for the Q3 model",
            "--access",
            "read",
            "--duration",
            "90d",
            "--priority",
            "medium",
        ],
    )

    assert result.exit_code == 0, result.output
    assert captured["url"] == "https://atlas.example.com/governance/access-requests"
    body = captured["json"]
    assert body == {
        "requester": "alice@example.com",
        "entityName": "warehouse.namespace.table",
        "entityUrn": "",
        "requestedAccess": "read",
        "reason": "need it for the Q3 model",
        "duration": "90d",
        "priority": "medium",
        "type": "dataset",
    }
    assert captured["headers"]["Authorization"] == "Bearer tok-123"
    assert "AR-7" in result.output


def test_request_access_includes_urn_when_given(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SEEKNAL_API_URL", "https://atlas.example.com")
    monkeypatch.setattr(gov_module, "user_email_from_credentials", lambda: "bob@example.com")
    monkeypatch.setattr(gov_module, "user_token_from_credentials", lambda: None)

    captured: dict[str, Any] = {}
    monkeypatch.setattr(
        httpx,
        "post",
        _captured_post(captured, status_code=201, json_body={"id": "AR-9", "status": "pending"}),
    )

    result = runner.invoke(
        app,
        [
            "gov",
            "request-access",
            "prod.gold.customer",
            "--reason",
            "audit",
            "--urn",
            "urn:li:dataset:(x,prod.gold.customer,PROD)",
        ],
    )

    assert result.exit_code == 0, result.output
    assert captured["json"]["entityUrn"] == "urn:li:dataset:(x,prod.gold.customer,PROD)"
    # No credentials token → no Authorization header.
    assert "Authorization" not in captured["headers"]


def test_request_access_non_2xx_exits_1(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SEEKNAL_API_URL", "https://atlas.example.com")
    monkeypatch.setattr(gov_module, "user_email_from_credentials", lambda: "alice@example.com")
    monkeypatch.setattr(gov_module, "user_token_from_credentials", lambda: None)

    captured: dict[str, Any] = {}
    monkeypatch.setattr(
        httpx,
        "post",
        _captured_post(captured, status_code=403, text="forbidden"),
    )

    result = runner.invoke(
        app,
        ["gov", "request-access", "prod.gold.finance", "--reason", "nope"],
    )

    assert result.exit_code == 1
    assert "403" in result.output


def test_request_access_transport_error_exits_1(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SEEKNAL_API_URL", "https://atlas.example.com")
    monkeypatch.setattr(gov_module, "user_email_from_credentials", lambda: "alice@example.com")
    monkeypatch.setattr(gov_module, "user_token_from_credentials", lambda: None)

    def boom(url: str, *, json: dict[str, Any], headers: dict[str, str], timeout: float):
        raise httpx.ConnectError("atlas down", request=httpx.Request("POST", url))

    monkeypatch.setattr(httpx, "post", boom)

    result = runner.invoke(
        app,
        ["gov", "request-access", "prod.gold.finance", "--reason", "x"],
    )

    assert result.exit_code == 1
