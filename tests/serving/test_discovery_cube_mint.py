"""Tests for minted Cube meta discovery token."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import stat

import httpx
import pytest

from seeknal.serving.discovery.config import (
    DiscoveryConfig,
    DiscoveryConfigError,
    DiscoveryError,
    EngineConfig,
)
from seeknal.serving.discovery.cube import CubeSource, _mint_cube_token


def _b64url_decode(s: str) -> bytes:
    rem = len(s) % 4
    padded = s + ("=" * ((4 - rem) % 4))
    return base64.urlsafe_b64decode(padded.encode("ascii"))


def test_minted_hs256_token_verified_independently():
    secret = "my-cube-api-secret-12345"
    now = 1700000000.0
    token = _mint_cube_token(
        secret=secret,
        claims={"role": "analyst"},
        now=now,
    )

    parts = token.split(".")
    assert len(parts) == 3
    h_b64, p_b64, sig_b64 = parts

    # Independent header verification
    header = json.loads(_b64url_decode(h_b64))
    assert header == {"alg": "HS256", "typ": "JWT"}

    # Independent payload verification
    payload = json.loads(_b64url_decode(p_b64))
    assert payload["role"] == "analyst"
    assert payload["iat"] == 1700000000
    assert payload["exp"] == 1700000300

    # Independent HMAC verification
    signing_input = f"{h_b64}.{p_b64}".encode("ascii")
    expected_sig = hmac.new(
        secret.encode("utf-8"), signing_input, hashlib.sha256
    ).digest()
    actual_sig = _b64url_decode(sig_b64)
    assert actual_sig == expected_sig


def test_minted_token_exp_minus_iat_is_300():
    secret = "secret-xyz"
    now = 1234567.89
    token = _mint_cube_token(secret=secret, claims=None, now=now)

    _, p_b64, _ = token.split(".")
    payload = json.loads(_b64url_decode(p_b64))
    assert payload["exp"] - payload["iat"] == 300


def test_api_claims_merged_cannot_override_exp_iat():
    secret = "secret-xyz"
    # User attempts to override iat and exp
    malicious_claims = {
        "role": "admin",
        "tenant": "tenant-42",
        "iat": 999999999,
        "exp": 111111111,
    }
    now = 1000.0
    token = _mint_cube_token(secret=secret, claims=malicious_claims, now=now)

    _, p_b64, _ = token.split(".")
    payload = json.loads(_b64url_decode(p_b64))
    assert payload["role"] == "admin"
    assert payload["tenant"] == "tenant-42"
    assert payload["iat"] == 1000
    assert payload["exp"] == 1300
    assert payload["exp"] - payload["iat"] == 300


def test_authorization_header_carries_token():
    captured_auth: list[str] = []

    def mock_handler(request: httpx.Request) -> httpx.Response:
        auth = request.headers.get("Authorization")
        if auth:
            captured_auth.append(auth)
        return httpx.Response(
            200,
            json={
                "cubes": [
                    {
                        "name": "orders",
                        "measures": [{"name": "orders.count"}],
                        "dimensions": [{"name": "orders.status"}],
                    }
                ]
            },
        )

    client = httpx.Client(transport=httpx.MockTransport(mock_handler))
    eng = EngineConfig(
        name="cube",
        kind="cube",
        dialect="postgres",
        url="http://cube.example.com",
        api_secret_env="CUBEJS_API_SECRET",
        api_claims={"role": "service"},
    )
    env = {"CUBEJS_API_SECRET": "test-cubejs-secret-888"}
    source = CubeSource(eng, client=client, env=env, clock=lambda: 2000.0)

    metas = source.discover()
    assert len(metas) == 1
    assert metas[0].name == "orders"

    assert len(captured_auth) == 1
    token = captured_auth[0]
    # Header format is Authorization: <token> (not Bearer)
    assert not token.startswith("Bearer ")
    parts = token.split(".")
    assert len(parts) == 3


def test_token_env_and_api_secret_env_both_set_refused():
    data = {
        "default_catalog": "lakekeeper",
        "engines": {
            "cube": {
                "kind": "cube",
                "dialect": "postgres",
                "url": "http://cube.example.com",
                "token_env": "STATIC_TOKEN",
                "api_secret_env": "CUBEJS_API_SECRET",
            }
        },
    }
    with pytest.raises(
        DiscoveryConfigError,
        match="cannot have both 'token_env' and 'api_secret_env'",
    ):
        DiscoveryConfig.from_dict(data)


def test_token_not_in_caplog(caplog):
    caplog.set_level(logging.DEBUG)

    captured_token: list[str] = []

    def mock_handler(request: httpx.Request) -> httpx.Response:
        auth = request.headers.get("Authorization")
        if auth:
            captured_token.append(auth)
        # Return an error to test error handling
        return httpx.Response(500, content=b"Internal Server Error")

    client = httpx.Client(transport=httpx.MockTransport(mock_handler))
    secret_val = "SECRET_SUPER_SPECIAL_VALUE_999"
    eng = EngineConfig(
        name="cube",
        kind="cube",
        dialect="postgres",
        url="http://cube.example.com",
        api_secret_env="CUBEJS_API_SECRET",
    )
    env = {"CUBEJS_API_SECRET": secret_val}
    source = CubeSource(eng, client=client, env=env, clock=lambda: 5000.0)

    with pytest.raises(DiscoveryError) as exc_info:
        source.discover()

    assert len(captured_token) == 1
    minted = captured_token[0]

    # Minted token and secret must not appear in caplog
    assert minted not in caplog.text
    assert secret_val not in caplog.text

    # Minted token and secret must not appear in error message
    assert minted not in str(exc_info.value)
    assert secret_val not in str(exc_info.value)


def test_api_secret_file_permissions(tmp_path):
    sec_file = tmp_path / "cube_secret.txt"
    sec_file.write_text("cube-secret-file-content\n")
    # Make loose (0644)
    sec_file.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH)

    data = {
        "default_catalog": "lakekeeper",
        "engines": {
            "cube": {
                "kind": "cube",
                "dialect": "postgres",
                "url": "http://cube.example.com",
                "api_secret_file": str(sec_file),
            }
        },
    }
    with pytest.raises(DiscoveryConfigError, match="group- or world-readable"):
        DiscoveryConfig.from_dict(data)

    # Restrict to 0600
    sec_file.chmod(stat.S_IRUSR | stat.S_IWUSR)
    cfg = DiscoveryConfig.from_dict(data)
    assert cfg.engines["cube"].api_secret_file == str(sec_file)
