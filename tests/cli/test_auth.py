"""
Tests for the ``seeknal auth`` command group.

These cover the pure, testable PKCE/OIDC helpers (no network, no browser, no live
loopback) plus the ``status`` and ``logout`` commands via ``CliRunner`` against a
temporary credentials file. ``webbrowser.open`` and the local loopback server are
mocked; the live redirect round-trip is intentionally not exercised here.
"""

from __future__ import annotations

import base64
import hashlib
import json
from typing import Any

import httpx
import pytest
from typer.testing import CliRunner

from seeknal.cli import auth as auth_module
from seeknal.cli.main import app

runner = CliRunner()


# -- PKCE / URL / token-exchange helpers ------------------------------------


def test_pkce_pair_s256_relationship() -> None:
    verifier, challenge = auth_module._pkce_pair()

    # Verifier is non-trivial and URL-safe (no '=' padding, no '+'/'/').
    assert len(verifier) >= 43
    assert "=" not in verifier and "+" not in verifier and "/" not in verifier

    # Challenge is base64url(SHA256(verifier)) with padding stripped.
    expected = (
        base64.urlsafe_b64encode(hashlib.sha256(verifier.encode("ascii")).digest())
        .rstrip(b"=")
        .decode("ascii")
    )
    assert challenge == expected
    assert "=" not in challenge

    # Distinct invocations produce distinct verifiers.
    other_verifier, _ = auth_module._pkce_pair()
    assert other_verifier != verifier


def test_build_authorize_url_carries_pkce_and_flow_params() -> None:
    endpoints = {
        "authorization_endpoint": "http://localhost:8080/realms/atlas/protocol/openid-connect/auth",
        "token_endpoint": "http://localhost:8080/realms/atlas/protocol/openid-connect/token",
    }
    url = auth_module._build_authorize_url(
        endpoints,
        client_id="seeknal-cli",
        redirect_uri="http://127.0.0.1:8765/callback",
        code_challenge="abc123",
        state="state-xyz",
    )

    assert url.startswith(endpoints["authorization_endpoint"] + "?")
    from urllib.parse import parse_qs, urlparse

    query = parse_qs(urlparse(url).query)
    assert query["response_type"] == ["code"]
    assert query["client_id"] == ["seeknal-cli"]
    assert query["code_challenge"] == ["abc123"]
    assert query["code_challenge_method"] == ["S256"]
    assert query["redirect_uri"] == ["http://127.0.0.1:8765/callback"]
    assert query["state"] == ["state-xyz"]


def test_exchange_code_posts_pkce_and_returns_tokens() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["body"] = request.content.decode("utf-8")
        return httpx.Response(
            200,
            json={
                "access_token": "at-1",
                "refresh_token": "rt-1",
                "expires_in": 300,
                "refresh_expires_in": 1800,
            },
        )

    client = httpx.Client(transport=httpx.MockTransport(handler))
    tokens = auth_module._exchange_code(
        "http://localhost:8080/realms/atlas/protocol/openid-connect/token",
        code="the-code",
        code_verifier="the-verifier",
        client_id="seeknal-cli",
        redirect_uri="http://127.0.0.1:8765/callback",
        client=client,
    )

    assert tokens["access_token"] == "at-1"
    assert tokens["refresh_token"] == "rt-1"

    # PKCE verifier + authorization_code grant are sent; public client (no secret).
    from urllib.parse import parse_qs

    sent = parse_qs(captured["body"])
    assert sent["grant_type"] == ["authorization_code"]
    assert sent["code"] == ["the-code"]
    assert sent["code_verifier"] == ["the-verifier"]
    assert sent["client_id"] == ["seeknal-cli"]
    assert "client_secret" not in sent


def test_write_credentials_to_tmp_path(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    creds = tmp_path / "nested" / "credentials.json"
    monkeypatch.setenv("SEEKNAL_CREDENTIALS_PATH", str(creds))

    path = auth_module._write_credentials(
        {
            "access_token": "at-1",
            "refresh_token": "rt-1",
            "expires_in": 300,
            "refresh_expires_in": 1800,
            "extra_ignored": "x",
        }
    )

    assert path == creds
    data = json.loads(creds.read_text(encoding="utf-8"))
    assert data == {
        "access_token": "at-1",
        "refresh_token": "rt-1",
        "expires_in": 300,
        "refresh_expires_in": 1800,
    }
    # File written 0600 (owner read/write only).
    assert (creds.stat().st_mode & 0o777) == 0o600


# -- status / logout via CliRunner ------------------------------------------


def _unsigned_jwt(payload: dict[str, Any]) -> str:
    """Build an unsigned-but-decodable JWT for ``user_*_from_credentials`` to read."""

    def seg(obj: dict[str, Any]) -> str:
        raw = json.dumps(obj).encode("utf-8")
        return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")

    return f"{seg({'alg': 'none'})}.{seg(payload)}.sig"


def _write_creds_file(tmp_path, monkeypatch: pytest.MonkeyPatch, *, sub: str, email: str) -> None:
    token = _unsigned_jwt({"sub": sub, "email": email})
    creds = tmp_path / "credentials.json"
    creds.write_text(json.dumps({"access_token": token}), encoding="utf-8")
    monkeypatch.setenv("SEEKNAL_CREDENTIALS_PATH", str(creds))


def test_status_authenticated(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write_creds_file(tmp_path, monkeypatch, sub="user-123", email="alice@example.com")

    result = runner.invoke(app, ["auth", "status"])

    assert result.exit_code == 0, result.output
    assert "user-123" in result.output
    assert "alice@example.com" in result.output


def test_status_not_authenticated_exits_1(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SEEKNAL_CREDENTIALS_PATH", str(tmp_path / "missing.json"))

    result = runner.invoke(app, ["auth", "status"])

    assert result.exit_code == 1
    assert "no" in result.output.lower()


def test_logout_removes_credentials(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    creds = tmp_path / "credentials.json"
    creds.write_text(json.dumps({"access_token": "at"}), encoding="utf-8")
    monkeypatch.setenv("SEEKNAL_CREDENTIALS_PATH", str(creds))

    result = runner.invoke(app, ["auth", "logout"])

    assert result.exit_code == 0, result.output
    assert not creds.exists()


def test_logout_when_missing_is_noop(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SEEKNAL_CREDENTIALS_PATH", str(tmp_path / "missing.json"))

    result = runner.invoke(app, ["auth", "logout"])

    assert result.exit_code == 0, result.output


def test_login_no_browser_prints_url_and_writes_credentials(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    creds = tmp_path / "credentials.json"
    monkeypatch.setenv("SEEKNAL_CREDENTIALS_PATH", str(creds))

    # A real token so the success line can echo the decoded email.
    access_token = _unsigned_jwt({"sub": "user-9", "email": "bob@example.com"})

    monkeypatch.setattr(
        auth_module,
        "_discover_endpoints",
        lambda issuer, client=None: {
            "authorization_endpoint": "http://kc/auth",
            "token_endpoint": "http://kc/token",
        },
    )
    captured_state: dict[str, Any] = {}

    def fake_capture(port: int, **kwargs: Any) -> dict[str, Any]:
        # Echo back the state generated inside login() by reading the authorize URL.
        return {"code": "auth-code", "state": captured_state["state"], "error": None}

    def fake_build_url(endpoints, *, state: str, **kwargs: Any) -> str:
        captured_state["state"] = state
        return "http://kc/auth?state=" + state

    monkeypatch.setattr(auth_module, "_build_authorize_url", fake_build_url)
    monkeypatch.setattr(auth_module, "_capture_authorization_code", fake_capture)
    monkeypatch.setattr(
        auth_module,
        "_exchange_code",
        lambda token_endpoint, **kwargs: {
            "access_token": access_token,
            "refresh_token": "rt",
            "expires_in": 300,
            "refresh_expires_in": 1800,
        },
    )

    opened: list[str] = []
    monkeypatch.setattr(auth_module.webbrowser, "open", lambda url: opened.append(url))

    result = runner.invoke(app, ["auth", "login", "--no-browser"])

    assert result.exit_code == 0, result.output
    # --no-browser: URL printed, browser not opened.
    assert opened == []
    assert "http://kc/auth" in result.output
    assert "bob@example.com" in result.output

    data = json.loads(creds.read_text(encoding="utf-8"))
    assert data["access_token"] == access_token
    assert data["refresh_token"] == "rt"
