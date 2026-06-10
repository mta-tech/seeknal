"""Tests for the apply-time Atlas contract client's authentication plumbing.

These pin the fix for the "Missing authentication token" apply failure: the
contract client must pick up the logged-in user's token from the credentials
file (like the governance gate and catalog client already do), transparently
refresh it once on a 401, and otherwise surface an actionable
"run `seeknal auth login`" hint instead of the raw backend error.
"""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest

from seeknal.integrations.atlas_client import (
    NOT_LOGGED_IN_HINT,
    SESSION_EXPIRED_HINT,
    AtlasAuthError,
    AtlasContractClient,
    AtlasContractConfig,
    AtlasContractError,
    create_atlas_contract_client_from_env,
)

BASE_URL = "http://atlas.test"


def _write_creds(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, access_token: str) -> Path:
    """Write a credentials file and point SEEKNAL_CREDENTIALS_PATH at it."""

    creds = tmp_path / "credentials.json"
    creds.write_text(
        json.dumps({"access_token": access_token, "refresh_token": "refresh-1"}),
        encoding="utf-8",
    )
    monkeypatch.setenv("SEEKNAL_CREDENTIALS_PATH", str(creds))
    return creds


def _isolate_atlas_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep factory tests away from the developer's real config/credentials."""

    monkeypatch.setenv("SEEKNAL_ATLAS_CONFIG_PATH", str(tmp_path / "no-atlas.json"))
    monkeypatch.setenv("SEEKNAL_CREDENTIALS_PATH", str(tmp_path / "no-credentials.json"))
    monkeypatch.delenv("ATLAS_API_URL", raising=False)
    monkeypatch.delenv("ATLAS_API_TOKEN", raising=False)


# ---------------------------------------------------------------------------
# Token resolution in create_atlas_contract_client_from_env
# ---------------------------------------------------------------------------


def test_client_from_env_uses_credentials_token_after_login(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _isolate_atlas_env(tmp_path, monkeypatch)
    monkeypatch.setenv("ATLAS_API_URL", BASE_URL)
    _write_creds(tmp_path, monkeypatch, "user-token")

    client = create_atlas_contract_client_from_env()

    assert client is not None
    assert client.config.token == "user-token"


def test_client_from_env_prefers_explicit_service_token(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _isolate_atlas_env(tmp_path, monkeypatch)
    monkeypatch.setenv("ATLAS_API_URL", BASE_URL)
    monkeypatch.setenv("ATLAS_API_TOKEN", "service-token")
    _write_creds(tmp_path, monkeypatch, "user-token")

    client = create_atlas_contract_client_from_env()

    assert client is not None
    assert client.config.token == "service-token"


def test_client_from_env_has_no_token_when_logged_out(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _isolate_atlas_env(tmp_path, monkeypatch)
    monkeypatch.setenv("ATLAS_API_URL", BASE_URL)

    client = create_atlas_contract_client_from_env()

    assert client is not None
    assert client.config.token is None


def test_client_from_env_inactive_without_atlas_url(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _isolate_atlas_env(tmp_path, monkeypatch)

    assert create_atlas_contract_client_from_env() is None


# ---------------------------------------------------------------------------
# Bearer header + 401 refresh/retry in _post
# ---------------------------------------------------------------------------


def _client(
    handler,
    *,
    token: str | None,
    token_refresher=None,
) -> AtlasContractClient:
    """Build a contract client whose HTTP calls are served via MockTransport."""

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    config = AtlasContractConfig(base_url=BASE_URL, token=token, timeout_seconds=5.0)
    return AtlasContractClient(config, client=http_client, token_refresher=token_refresher)


def test_post_sends_bearer_token() -> None:
    seen_auth: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_auth.append(request.headers.get("Authorization"))
        return httpx.Response(200, json={"allowed": True})

    client = _client(handler, token="user-token")
    result = client._post("/api/contracts/policy-check", {"operation": "apply"})

    assert result == {"allowed": True}
    assert seen_auth == ["Bearer user-token"]


def test_post_refreshes_token_once_on_401_and_retries() -> None:
    seen_auth: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_auth.append(request.headers.get("Authorization"))
        if request.headers.get("Authorization") == "Bearer fresh-token":
            return httpx.Response(200, json={"allowed": True})
        return httpx.Response(401, json={"detail": "Missing authentication token"})

    client = _client(handler, token="stale-token", token_refresher=lambda: "fresh-token")
    result = client._post("/api/contracts/policy-check", {"operation": "apply"})

    assert result == {"allowed": True}
    assert seen_auth == ["Bearer stale-token", "Bearer fresh-token"]


def test_post_raises_session_expired_when_refresh_fails() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={"detail": "Missing authentication token"})

    client = _client(handler, token="stale-token", token_refresher=lambda: None)

    with pytest.raises(AtlasAuthError) as excinfo:
        client._post("/api/contracts/policy-check", {"operation": "apply"})
    assert str(excinfo.value) == SESSION_EXPIRED_HINT


def test_post_raises_login_hint_when_no_token_at_all() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={"detail": "Missing authentication token"})

    client = _client(handler, token=None, token_refresher=lambda: None)

    with pytest.raises(AtlasAuthError) as excinfo:
        client._post("/api/contracts/policy-check", {"operation": "apply"})
    assert str(excinfo.value) == NOT_LOGGED_IN_HINT


def test_post_non_401_errors_keep_generic_contract_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="boom")

    client = _client(handler, token="user-token")

    with pytest.raises(AtlasContractError) as excinfo:
        client._post("/api/contracts/policy-check", {"operation": "apply"})
    assert not isinstance(excinfo.value, AtlasAuthError)
    assert "boom" in str(excinfo.value)


def test_preflight_apply_authenticates_with_refreshed_token(tmp_path: Path) -> None:
    """End-to-end: a stale session is refreshed mid-preflight and apply proceeds."""

    seen: list[tuple[str, str | None]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.url.path, request.headers.get("Authorization")))
        if request.headers.get("Authorization") != "Bearer fresh-token":
            return httpx.Response(401, json={"detail": "Missing authentication token"})
        return httpx.Response(200, json={"allowed": True, "reason": "ok"})

    client = _client(handler, token="stale-token", token_refresher=lambda: "fresh-token")
    context = client.preflight_apply(
        node_type="source",
        name="raw_orders",
        yaml_data={"kind": "source", "name": "raw_orders"},
        target_path=tmp_path / "seeknal" / "sources" / "raw_orders.yml",
        project_path=tmp_path,
    )

    assert context.asset["name"] == "raw_orders"
    assert seen[0] == ("/api/contracts/policy-check", "Bearer stale-token")
    assert seen[1] == ("/api/contracts/policy-check", "Bearer fresh-token")
