"""Tests for server --check validation of discovery secrets."""

from __future__ import annotations

import base64
import json
import os
import stat
from cryptography.hazmat.primitives.asymmetric import ec
import pytest
import yaml

from seeknal.serving.__main__ import main


@pytest.fixture
def ec_jwks_file(tmp_path):
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()
    pn = pub.public_numbers()
    x_b64 = (
        base64.urlsafe_b64encode(pn.x.to_bytes(32, "big")).rstrip(b"=").decode("ascii")
    )
    y_b64 = (
        base64.urlsafe_b64encode(pn.y.to_bytes(32, "big")).rstrip(b"=").decode("ascii")
    )
    jwk = {
        "kty": "EC",
        "crv": "P-256",
        "x": x_b64,
        "y": y_b64,
        "kid": "check-key-1",
    }
    jwks_path = tmp_path / "keys.jwks.json"
    jwks_path.write_text(json.dumps({"keys": [jwk]}))
    return str(jwks_path)


def test_server_check_missing_client_secret_env_exits_2_naming_variable(
    tmp_path, ec_jwks_file, capsys, monkeypatch
):
    monkeypatch.delenv("LAKEKEEPER_CLIENT_SECRET", raising=False)

    cfg_dict = {
        "listen": {"host": "127.0.0.1", "port": 8765},
        "grant_keys": {"jwks_file": ec_jwks_file},
        "placement": {
            "discovery": {
                "default_catalog": "lakekeeper",
                "lakekeeper": {
                    "url": "https://lk.internal",
                    "warehouse": "atlas",
                    "oauth": {
                        "token_url": "https://auth.internal/token",
                        "client_id": "seeknal-serving-discovery",
                        "client_secret_env": "LAKEKEEPER_CLIENT_SECRET",
                    },
                },
            }
        },
        "connections": {
            "duck": {"kind": "duckdb", "path": ":memory:"},
        },
    }
    cfg_file = tmp_path / "server.yaml"
    with open(cfg_file, "w", encoding="utf-8") as f:
        yaml.safe_dump(cfg_dict, f)

    ret = main(["--config", str(cfg_file), "--check"])
    assert ret == 2

    captured = capsys.readouterr()
    assert "LAKEKEEPER_CLIENT_SECRET" in captured.err
    assert "Error:" in captured.err


def test_server_check_passes_when_client_secret_in_0600_secrets_env_file(
    tmp_path, ec_jwks_file, capsys, monkeypatch
):
    monkeypatch.delenv("LAKEKEEPER_CLIENT_SECRET", raising=False)

    sec_env_file = tmp_path / "secrets.env"
    sec_env_file.write_text("LAKEKEEPER_CLIENT_SECRET=super-secret-oauth-val\n")
    sec_env_file.chmod(stat.S_IRUSR | stat.S_IWUSR)  # 0600

    cfg_dict = {
        "listen": {"host": "127.0.0.1", "port": 8765},
        "grant_keys": {"jwks_file": ec_jwks_file},
        "secrets_env_file": str(sec_env_file),
        "placement": {
            "discovery": {
                "default_catalog": "lakekeeper",
                "lakekeeper": {
                    "url": "https://lk.internal",
                    "warehouse": "atlas",
                    "oauth": {
                        "token_url": "https://auth.internal/token",
                        "client_id": "seeknal-serving-discovery",
                        "client_secret_env": "LAKEKEEPER_CLIENT_SECRET",
                    },
                },
            }
        },
        "connections": {
            "duck": {"kind": "duckdb", "path": ":memory:"},
        },
    }
    cfg_file = tmp_path / "server.yaml"
    with open(cfg_file, "w", encoding="utf-8") as f:
        yaml.safe_dump(cfg_dict, f)

    ret = main(["--config", str(cfg_file), "--check"])
    assert ret == 0

    captured = capsys.readouterr()
    assert "config ok" in captured.out


def test_server_check_missing_cube_api_secret_env_exits_2_naming_variable(
    tmp_path, ec_jwks_file, capsys, monkeypatch
):
    monkeypatch.delenv("CUBEJS_API_SECRET", raising=False)

    cfg_dict = {
        "listen": {"host": "127.0.0.1", "port": 8765},
        "grant_keys": {"jwks_file": ec_jwks_file},
        "placement": {
            "discovery": {
                "default_catalog": "lakekeeper",
                "engines": {
                    "cube": {
                        "kind": "cube",
                        "dialect": "postgres",
                        "url": "http://cube:4000",
                        "api_secret_env": "CUBEJS_API_SECRET",
                    }
                },
            }
        },
        "connections": {
            "duck": {"kind": "duckdb", "path": ":memory:"},
        },
    }
    cfg_file = tmp_path / "server.yaml"
    with open(cfg_file, "w", encoding="utf-8") as f:
        yaml.safe_dump(cfg_dict, f)

    ret = main(["--config", str(cfg_file), "--check"])
    assert ret == 2

    captured = capsys.readouterr()
    assert "CUBEJS_API_SECRET" in captured.err


def test_server_check_refuses_loose_secret_files(tmp_path, ec_jwks_file, capsys):
    secret_file = tmp_path / "loose_oauth_sec.txt"
    secret_file.write_text("my-secret\n")
    secret_file.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH)  # 0644

    cfg_dict = {
        "listen": {"host": "127.0.0.1", "port": 8765},
        "grant_keys": {"jwks_file": ec_jwks_file},
        "placement": {
            "discovery": {
                "default_catalog": "lakekeeper",
                "lakekeeper": {
                    "url": "https://lk.internal",
                    "warehouse": "atlas",
                    "oauth": {
                        "token_url": "https://auth.internal/token",
                        "client_id": "client-1",
                        "client_secret_file": str(secret_file),
                    },
                },
            }
        },
        "connections": {
            "duck": {"kind": "duckdb", "path": ":memory:"},
        },
    }
    cfg_file = tmp_path / "server.yaml"
    with open(cfg_file, "w", encoding="utf-8") as f:
        yaml.safe_dump(cfg_dict, f)

    ret = main(["--config", str(cfg_file), "--check"])
    assert ret == 2

    captured = capsys.readouterr()
    assert "group- or world-readable" in captured.err
