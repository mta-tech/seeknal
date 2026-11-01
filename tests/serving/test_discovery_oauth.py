"""Tests for Lakekeeper OAuth2 client credentials discovery source."""

from __future__ import annotations

import stat
import urllib.parse
from typing import Any

import httpx
import pytest

from seeknal.serving.discovery.config import (
    DiscoveryConfig,
    DiscoveryConfigError,
    DiscoveryError,
    LakekeeperConfig,
    LakekeeperOAuthConfig,
)
from seeknal.serving.discovery.lakekeeper import LakekeeperSource
from seeknal.serving.discovery.oauth import ClientCredentialsTokenSource


def test_oauth_token_fetched_exact_form_fields_and_cached():
    token_url = "https://auth.example.com/oauth/token"
    captured_requests: list[httpx.Request] = []

    def mock_handler(request: httpx.Request) -> httpx.Response:
        captured_requests.append(request)
        if request.url == token_url:
            body = request.content.decode("utf-8")
            form = urllib.parse.parse_qs(body)
            assert form["grant_type"] == ["client_credentials"]
            assert form["client_id"] == ["client-123"]
            assert form["client_secret"] == ["secret-abc"]
            assert form["scope"] == ["catalog:read"]
            assert form["audience"] == ["lakekeeper"]
            return httpx.Response(
                200,
                json={"access_token": "token-xyz-1", "expires_in": 3600},
            )
        return httpx.Response(404)

    client = httpx.Client(transport=httpx.MockTransport(mock_handler))
    cfg = LakekeeperOAuthConfig(
        token_url=token_url,
        client_id="client-123",
        client_secret_env="LAKEKEEPER_CLIENT_SECRET",
        scope="catalog:read",
        audience="lakekeeper",
    )
    env = {"LAKEKEEPER_CLIENT_SECRET": "secret-abc"}
    source = ClientCredentialsTokenSource(cfg, client=client, env=env)

    # First fetch
    token1 = source.token()
    assert token1 == "token-xyz-1"
    assert len(captured_requests) == 1
    req = captured_requests[0]
    assert req.method == "POST"
    assert "application/x-www-form-urlencoded" in req.headers["Content-Type"]

    # Second fetch returns cached token without new HTTP call
    token2 = source.token()
    assert token2 == "token-xyz-1"
    assert len(captured_requests) == 1


def test_oauth_token_refreshed_after_expires_in_minus_30():
    token_url = "https://auth.example.com/oauth/token"
    call_count = 0
    now = 1000.0

    def fake_clock() -> float:
        return now

    def mock_handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return httpx.Response(
            200,
            json={"access_token": f"token-{call_count}", "expires_in": 100},
        )

    client = httpx.Client(transport=httpx.MockTransport(mock_handler))
    cfg = LakekeeperOAuthConfig(
        token_url=token_url,
        client_id="client-123",
        client_secret_env="LAKEKEEPER_CLIENT_SECRET",
    )
    env = {"LAKEKEEPER_CLIENT_SECRET": "secret-abc"}
    source = ClientCredentialsTokenSource(
        cfg, client=client, env=env, clock=fake_clock
    )

    # 1. At t=1000: token-1 fetched (cached until 1000 + 100 - 30 = 1070)
    assert source.token() == "token-1"
    assert call_count == 1

    # 2. At t=1069: still cached
    now = 1069.0
    assert source.token() == "token-1"
    assert call_count == 1

    # 3. At t=1070: cache expired (now >= 1070), refreshes to token-2
    now = 1070.0
    assert source.token() == "token-2"
    assert call_count == 2


def test_oauth_token_missing_expires_in_gives_60s_cache():
    token_url = "https://auth.example.com/oauth/token"
    call_count = 0
    now = 500.0

    def fake_clock() -> float:
        return now

    def mock_handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return httpx.Response(
            200,
            json={"access_token": f"token-noexp-{call_count}"},
        )

    client = httpx.Client(transport=httpx.MockTransport(mock_handler))
    cfg = LakekeeperOAuthConfig(
        token_url=token_url,
        client_id="client-123",
        client_secret_env="LAKEKEEPER_CLIENT_SECRET",
    )
    env = {"LAKEKEEPER_CLIENT_SECRET": "secret-abc"}
    source = ClientCredentialsTokenSource(
        cfg, client=client, env=env, clock=fake_clock
    )

    # At t=500: fetched, cached until 500 + 60 = 560
    assert source.token() == "token-noexp-1"
    assert call_count == 1

    # At t=559: cached
    now = 559.0
    assert source.token() == "token-noexp-1"
    assert call_count == 1

    # At t=560: cache expired, refreshes
    now = 560.0
    assert source.token() == "token-noexp-2"
    assert call_count == 2


def test_oauth_token_endpoint_error_raises_discovery_error_no_secret_or_body():
    token_url = "https://auth.example.com/oauth/token"
    secret = "SUPER_SECRET_CLIENT_PASSWORD_XYZ"
    leaked_body = (
        '{"error": "invalid_client", "error_description": "body details leaked"}'
    )

    def mock_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, content=leaked_body.encode("utf-8"))

    client = httpx.Client(transport=httpx.MockTransport(mock_handler))
    cfg = LakekeeperOAuthConfig(
        token_url=token_url,
        client_id="client-123",
        client_secret_env="LAKEKEEPER_CLIENT_SECRET",
    )
    env = {"LAKEKEEPER_CLIENT_SECRET": secret}
    source = ClientCredentialsTokenSource(cfg, client=client, env=env)

    with pytest.raises(DiscoveryError) as exc_info:
        source.token()

    err_str = str(exc_info.value)
    assert secret not in err_str
    assert "body details leaked" not in err_str
    assert "invalid_client" not in err_str


def test_lakekeeper_401_triggers_one_invalidate_and_retry_then_success():
    token_url = "https://auth.example.com/oauth/token"
    lk_base = "https://lk.example.com"
    token_counter = 0
    lk_401_count = 0

    def mock_handler(request: httpx.Request) -> httpx.Response:
        nonlocal token_counter, lk_401_count
        if request.url == token_url:
            token_counter += 1
            return httpx.Response(
                200, json={"access_token": f"token-{token_counter}"}
            )
        if request.url == f"{lk_base}/v1/config?warehouse=test_wh":
            auth = request.headers.get("Authorization")
            if auth == "Bearer token-1":
                lk_401_count += 1
                return httpx.Response(401, json={"message": "token expired"})
            elif auth == "Bearer token-2":
                return httpx.Response(200, json={"defaults": {}})
        if request.url == f"{lk_base}/v1/namespaces":
            return httpx.Response(200, json={"namespaces": []})
        return httpx.Response(404)

    client = httpx.Client(transport=httpx.MockTransport(mock_handler))
    lk_cfg = LakekeeperConfig(
        url=lk_base,
        warehouse="test_wh",
        oauth=LakekeeperOAuthConfig(
            token_url=token_url,
            client_id="client-123",
            client_secret_env="LAKEKEEPER_CLIENT_SECRET",
        ),
    )
    env = {"LAKEKEEPER_CLIENT_SECRET": "secret-abc"}
    source = LakekeeperSource(lk_cfg, client=client, env=env)

    tables = source.discover()
    assert tables == []
    assert lk_401_count == 1
    assert token_counter == 2  # invalidated and re-fetched


def test_lakekeeper_two_401s_raise_discovery_error():
    token_url = "https://auth.example.com/oauth/token"
    lk_base = "https://lk.example.com"
    token_counter = 0

    def mock_handler(request: httpx.Request) -> httpx.Response:
        nonlocal token_counter
        if request.url == token_url:
            token_counter += 1
            return httpx.Response(
                200, json={"access_token": f"token-{token_counter}"}
            )
        if request.url == f"{lk_base}/v1/config?warehouse=test_wh":
            return httpx.Response(401, json={"message": "permanently unauthorized"})
        return httpx.Response(404)

    client = httpx.Client(transport=httpx.MockTransport(mock_handler))
    lk_cfg = LakekeeperConfig(
        url=lk_base,
        warehouse="test_wh",
        oauth=LakekeeperOAuthConfig(
            token_url=token_url,
            client_id="client-123",
            client_secret_env="LAKEKEEPER_CLIENT_SECRET",
        ),
    )
    env = {"LAKEKEEPER_CLIENT_SECRET": "secret-abc"}
    source = LakekeeperSource(lk_cfg, client=client, env=env)

    with pytest.raises(DiscoveryError, match="401"):
        source.discover()

    assert token_counter == 2  # attempted one retry, then raised


def test_token_env_and_oauth_both_set_raise_discovery_config_error():
    data = {
        "default_catalog": "lakekeeper",
        "lakekeeper": {
            "url": "https://lk.example.com",
            "warehouse": "test_wh",
            "token_env": "STATIC_TOKEN",
            "oauth": {
                "token_url": "https://auth.example.com/token",
                "client_id": "client-123",
                "client_secret_env": "SECRET_ENV",
            },
        },
    }
    with pytest.raises(
        DiscoveryConfigError, match="cannot have both 'token_env' and 'oauth'"
    ):
        DiscoveryConfig.from_dict(data)


def test_loose_client_secret_file_is_refused(tmp_path):
    sec_file = tmp_path / "loose_secret.txt"
    sec_file.write_text("my-secret\n")
    # Set group and world readable mode (0644)
    sec_file.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH)

    data = {
        "default_catalog": "lakekeeper",
        "lakekeeper": {
            "url": "https://lk.example.com",
            "warehouse": "test_wh",
            "oauth": {
                "token_url": "https://auth.example.com/token",
                "client_id": "client-123",
                "client_secret_file": str(sec_file),
            },
        },
    }
    with pytest.raises(
        DiscoveryConfigError, match="group- or world-readable"
    ):
        DiscoveryConfig.from_dict(data)

    # Also test ClientCredentialsTokenSource._read_secret refuses it
    cfg = LakekeeperOAuthConfig(
        token_url="https://auth.example.com/token",
        client_id="client-123",
        client_secret_file=str(sec_file),
    )
    client = httpx.Client()
    source = ClientCredentialsTokenSource(cfg, client=client)
    with pytest.raises(DiscoveryConfigError, match="group- or world-readable"):
        source.token()


def test_only_token_endpoint_gets_post_lakekeeper_calls_stay_get():
    token_url = "https://auth.example.com/oauth/token"
    lk_base = "https://lk.example.com"
    recorded_methods: list[tuple[str, str]] = []  # (method, url)

    def mock_handler(request: httpx.Request) -> httpx.Response:
        recorded_methods.append((request.method, str(request.url)))
        if request.url == token_url:
            return httpx.Response(200, json={"access_token": "token-1"})
        if str(request.url).startswith(f"{lk_base}/v1/config"):
            return httpx.Response(200, json={"defaults": {}})
        if str(request.url).startswith(f"{lk_base}/v1/namespaces"):
            if "tables" in str(request.url):
                return httpx.Response(200, json={"identifiers": []})
            return httpx.Response(200, json={"namespaces": [["ns1"]]})
        return httpx.Response(404)

    client = httpx.Client(transport=httpx.MockTransport(mock_handler))
    lk_cfg = LakekeeperConfig(
        url=lk_base,
        warehouse="test_wh",
        oauth=LakekeeperOAuthConfig(
            token_url=token_url,
            client_id="client-123",
            client_secret_env="LAKEKEEPER_CLIENT_SECRET",
        ),
    )
    env = {"LAKEKEEPER_CLIENT_SECRET": "secret-abc"}
    source = LakekeeperSource(lk_cfg, client=client, env=env)
    source.discover()

    assert len(recorded_methods) >= 3
    for method, url in recorded_methods:
        if url == token_url:
            assert method == "POST"
        else:
            assert method == "GET"
