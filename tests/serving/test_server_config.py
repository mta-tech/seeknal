"""Tests for Seeknal Serving server configuration."""

from __future__ import annotations

import base64
import json
import os
import stat
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec

from seeknal.serving.server.config import (
    ServerConfig,
    ServerConfigError,
    load_grant_keys,
    parse_jwks,
)


@pytest.fixture
def ec_jwk():
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
        "kid": "atlas-2026-test",
    }
    pem = pub.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode("ascii")
    return priv, pub, jwk, pem


@pytest.fixture
def valid_jwks_file(tmp_path, ec_jwk):
    _, _, jwk, _ = ec_jwk
    jwks_path = tmp_path / "catalog-grants.jwks.json"
    jwks_path.write_text(json.dumps({"keys": [jwk]}))
    return str(jwks_path)


@pytest.fixture
def valid_placement_files(tmp_path):
    pf = tmp_path / "placement.yaml"
    pf.write_text("""
engines:
  sr: {kind: starrocks, dialect: starrocks}
  lb: {kind: lakebase, dialect: postgres}
  cube: {kind: cube, dialect: postgres}
  duck: {kind: duckdb, dialect: duckdb}
tables:
  lakekeeper.users:
    keys: [id]
    row_count: 100
    serve:
      duck: users
cubes: {}
""")
    sf = tmp_path / "schemas.json"
    sf.write_text(json.dumps({"lakekeeper.users": ["id", "name"]}))
    return str(pf), str(sf)


@pytest.fixture
def valid_secrets_file(tmp_path):
    secret_path = tmp_path / "starrocks-workloads.env"
    secret_path.write_text(
        "SEEKNAL_DEMO_QUERY_PASSWORD=secret123\nATLAS_BI_PASSWORD=secret456\n"
    )
    os.chmod(secret_path, stat.S_IRUSR | stat.S_IWUSR)  # 0600
    return str(secret_path)


@pytest.fixture
def valid_password_file(tmp_path):
    pwd_path = tmp_path / "report_pw.txt"
    pwd_path.write_text("secret789\n")
    os.chmod(pwd_path, stat.S_IRUSR | stat.S_IWUSR)  # 0600
    return str(pwd_path)


def _build_full_valid_dict(
    jwks_file: str,
    placement_file: str,
    schemas_file: str,
    secrets_file: str,
    password_file: str,
) -> dict:
    return {
        "listen": {"host": "0.0.0.0", "port": 8765},
        "tls": {
            "cert_file": "/tmp/cert.pem",
            "key_file": "/tmp/key.pem",
            "client_ca_file": "/tmp/ca.pem",
            "require_client_cert": True,
        },
        "grant_keys": {"jwks_file": jwks_file},
        "secrets_env_file": secrets_file,
        "logging": {"level": "INFO", "format": "json", "file": None},
        "require_grant_typ": True,
        "default_catalog": "lakekeeper",
        "cube_mode": "jwt",
        "placement": {
            "static": {
                "placement_file": placement_file,
                "schemas_file": schemas_file,
            }
        },
        "connections": {
            "sr": {
                "kind": "starrocks",
                "accounts": {
                    "interactive": {
                        "host": "starrocks-fe",
                        "port": 9030,
                        "user": "seeknal_demo",
                        "password_env": "SEEKNAL_DEMO_QUERY_PASSWORD",
                    },
                    "bi": {
                        "host": "starrocks-fe",
                        "port": 9030,
                        "user": "atlas_bi",
                        "password_env": "ATLAS_BI_PASSWORD",
                    },
                    "report": {
                        "host": "starrocks-fe",
                        "port": 9030,
                        "user": "atlas_reports",
                        "password_file": password_file,
                    },
                },
            },
            "lb": {"kind": "lakebase", "dsn_env": "LAKEBASE_DSN"},
            "cube": {
                "kind": "cube",
                "host": "cube",
                "port": 15432,
                "dbname": "db",
                "sql_user": "seeknal",
                "secret_env": "CUBE_SQL_SECRET",
                "connect_timeout_seconds": 10,
            },
            "duck": {"kind": "duckdb", "path": "/data/dev.duckdb"},
        },
        "limits": {
            "timeout_seconds": 30,
            "max_body_bytes": 1000000,
            "max_result_bytes": 67108864,
        },
    }


def test_full_valid_config(
    valid_jwks_file,
    valid_placement_files,
    valid_secrets_file,
    valid_password_file,
):
    pf, sf = valid_placement_files
    d = _build_full_valid_dict(
        valid_jwks_file, pf, sf, valid_secrets_file, valid_password_file
    )
    cfg = ServerConfig.from_dict(d)

    assert cfg.listen.host == "0.0.0.0"
    assert cfg.listen.port == 8765
    assert cfg.tls is not None
    assert cfg.tls.require_client_cert is True
    assert cfg.require_grant_typ is True
    assert cfg.default_catalog == "lakekeeper"
    assert cfg.cube_mode == "jwt"
    assert cfg.placement.is_static is True
    assert "sr" in cfg.connections
    assert "lb" in cfg.connections
    assert "cube" in cfg.connections
    assert "duck" in cfg.connections

    keys = cfg.load_grant_keys()
    assert "atlas-2026-test" in keys
    assert keys["atlas-2026-test"].startswith("-----BEGIN PUBLIC KEY-----")


def test_each_rule_violation_raises_server_config_error(
    valid_jwks_file,
    valid_placement_files,
    valid_secrets_file,
    valid_password_file,
):
    pf, sf = valid_placement_files
    base_d = _build_full_valid_dict(
        valid_jwks_file, pf, sf, valid_secrets_file, valid_password_file
    )

    # 1. Unknown top-level key
    d1 = dict(base_d, unknown_key="invalid")
    with pytest.raises(ServerConfigError, match="Unknown top-level"):
        ServerConfig.from_dict(d1)

    # 2. Unknown key in listen
    d2 = dict(base_d, listen={"host": "0.0.0.0", "port": 8765, "extra": True})
    with pytest.raises(ServerConfigError, match="Unknown key.*listen"):
        ServerConfig.from_dict(d2)

    # 3. Invalid port
    d3 = dict(base_d, listen={"host": "0.0.0.0", "port": 70000})
    with pytest.raises(ServerConfigError, match="integer between 1 and 65535"):
        ServerConfig.from_dict(d3)

    # 4. StarRocks missing interactive
    d4 = json.loads(json.dumps(base_d))
    del d4["connections"]["sr"]["accounts"]["interactive"]
    with pytest.raises(ServerConfigError, match="contain at least 'interactive'"):
        ServerConfig.from_dict(d4)

    # 5. StarRocks forbidden account name
    d5 = json.loads(json.dumps(base_d))
    d5["connections"]["sr"]["accounts"]["admin"] = {
        "host": "localhost",
        "port": 9030,
        "user": "admin",
        "password_env": "PW",
    }
    with pytest.raises(ServerConfigError, match="invalid account"):
        ServerConfig.from_dict(d5)

    # 6. StarRocks account with both password_env and password_file
    d6 = json.loads(json.dumps(base_d))
    d6["connections"]["sr"]["accounts"]["interactive"][
        "password_file"
    ] = valid_password_file
    with pytest.raises(ServerConfigError, match="cannot have both"):
        ServerConfig.from_dict(d6)

    # 7. Cube secret_env forbidden when cube_mode is refuse
    d7 = json.loads(json.dumps(base_d))
    d7["cube_mode"] = "refuse"
    with pytest.raises(
        ServerConfigError, match="forbids 'secret_env' when cube_mode is 'refuse'"
    ):
        ServerConfig.from_dict(d7)

    # 8. Cube secret_env required when cube_mode is jwt
    d8 = json.loads(json.dumps(base_d))
    del d8["connections"]["cube"]["secret_env"]
    with pytest.raises(
        ServerConfigError, match="requires 'secret_env' when cube_mode is 'jwt'"
    ):
        ServerConfig.from_dict(d8)

    # 9. Invalid cube_mode
    d9 = dict(base_d, cube_mode="something_else")
    with pytest.raises(ServerConfigError, match="Invalid cube_mode"):
        ServerConfig.from_dict(d9)

    # 10. Placement neither static nor discovery
    d10 = dict(base_d, placement={})
    with pytest.raises(
        ServerConfigError, match="exactly one of 'discovery' or 'static', got neither"
    ):
        ServerConfig.from_dict(d10)

    # 11. Placement both static and discovery
    d11 = dict(
        base_d,
        placement={
            "static": {"placement_file": pf, "schemas_file": sf},
            "discovery": {"default_catalog": "lakekeeper"},
        },
    )
    with pytest.raises(ServerConfigError, match="got both"):
        ServerConfig.from_dict(d11)

    # 12. grant_keys both jwks_file and pem_files
    d12 = dict(
        base_d,
        grant_keys={"jwks_file": valid_jwks_file, "pem_files": {"k1": "/tmp/k1.pem"}},
    )
    with pytest.raises(ServerConfigError, match="got both"):
        ServerConfig.from_dict(d12)

    # 13. grant_keys neither
    d13 = dict(base_d, grant_keys={})
    with pytest.raises(ServerConfigError, match="got neither"):
        ServerConfig.from_dict(d13)


def test_error_never_contains_secret(
    valid_jwks_file,
    valid_placement_files,
    valid_secrets_file,
    valid_password_file,
    monkeypatch,
):
    secret_val = "SECRET-XYZ-SHOULD-NEVER-LEAK-987"
    monkeypatch.setenv("SEEKNAL_DEMO_QUERY_PASSWORD", secret_val)

    pf, sf = valid_placement_files
    d = _build_full_valid_dict(
        valid_jwks_file, pf, sf, valid_secrets_file, valid_password_file
    )

    # Cause an error during config validation or env checking
    d["listen"]["port"] = -99
    with pytest.raises(ServerConfigError) as exc_info:
        ServerConfig.from_dict(d)

    assert secret_val not in str(exc_info.value)

    # Cause a missing env var error
    del d["secrets_env_file"]
    d["listen"]["port"] = 8765
    monkeypatch.setenv("ATLAS_BI_PASSWORD", "bi_pass")
    monkeypatch.setenv("CUBE_SQL_SECRET", "cube_pass")
    monkeypatch.delenv("LAKEBASE_DSN", raising=False)
    cfg = ServerConfig.from_dict(d)
    with pytest.raises(ServerConfigError) as exc_info:
        cfg.check_env()

    assert secret_val not in str(exc_info.value)
    assert "LAKEBASE_DSN" in str(exc_info.value)


def test_jwks_loading(ec_jwk):
    priv, pub, valid_jwk, pem = ec_jwk

    # 1. Valid P-256 key with kid
    res = parse_jwks({"keys": [valid_jwk]})
    assert "atlas-2026-test" in res
    assert res["atlas-2026-test"].strip() == pem.strip()

    # 2. Private member 'd' present
    jwk_with_d = dict(valid_jwk, d="some_private_exponent")
    with pytest.raises(ServerConfigError, match="contains private key member 'd'"):
        parse_jwks({"keys": [jwk_with_d]})

    # 3. Wrong curve
    jwk_wrong_crv = dict(valid_jwk, crv="P-384")
    with pytest.raises(ServerConfigError, match="unsupported curve 'P-384'"):
        parse_jwks({"keys": [jwk_wrong_crv]})

    # 4. Duplicate kid
    jwk2 = dict(valid_jwk)
    with pytest.raises(ServerConfigError, match="Duplicate kid 'atlas-2026-test'"):
        parse_jwks({"keys": [valid_jwk, jwk2]})

    # 5. Missing kid
    jwk_no_kid = dict(valid_jwk)
    del jwk_no_kid["kid"]
    with pytest.raises(ServerConfigError, match="missing required 'kid'"):
        parse_jwks({"keys": [jwk_no_kid]})

    # 6. Wrong kty
    jwk_wrong_kty = dict(valid_jwk, kty="RSA")
    with pytest.raises(ServerConfigError, match="unsupported kty 'RSA'"):
        parse_jwks({"keys": [jwk_wrong_kty]})

    # 7. Empty keys
    with pytest.raises(ServerConfigError, match="must not be empty"):
        parse_jwks({"keys": []})


def test_secrets_env_file(
    valid_jwks_file,
    valid_placement_files,
    valid_password_file,
    tmp_path,
):
    pf, sf = valid_placement_files
    sec_file = tmp_path / "workloads.env"
    sec_file.write_text(
        "SEEKNAL_DEMO_QUERY_PASSWORD=file_pw_123\n# A comment\nATLAS_BI_PASSWORD=file_pw_456\n"
    )

    # 1. Mode 0644 is refused
    os.chmod(sec_file, 0o644)
    d = _build_full_valid_dict(
        valid_jwks_file, pf, sf, str(sec_file), valid_password_file
    )
    with pytest.raises(ServerConfigError, match="must be mode 0600 or stricter"):
        ServerConfig.from_dict(d)

    # 2. Mode 0600 is accepted and consulted before process env
    os.chmod(sec_file, 0o600)
    cfg = ServerConfig.from_dict(d)
    assert cfg.get_env("SEEKNAL_DEMO_QUERY_PASSWORD") == "file_pw_123"
    assert cfg.get_env("ATLAS_BI_PASSWORD") == "file_pw_456"

    # Consulted before process env
    os.environ["SEEKNAL_DEMO_QUERY_PASSWORD"] = "env_pw_override"
    try:
        assert cfg.get_env("SEEKNAL_DEMO_QUERY_PASSWORD") == "file_pw_123"
    finally:
        os.environ.pop("SEEKNAL_DEMO_QUERY_PASSWORD", None)


def test_group_readable_password_file(
    valid_jwks_file,
    valid_placement_files,
    valid_secrets_file,
    tmp_path,
):
    pf, sf = valid_placement_files
    pw_file = tmp_path / "pw.txt"
    pw_file.write_text("password123\n")

    # 1. Group-readable (0644) is refused
    os.chmod(pw_file, 0o644)
    d = _build_full_valid_dict(
        valid_jwks_file, pf, sf, valid_secrets_file, str(pw_file)
    )
    with pytest.raises(ServerConfigError, match="group- or world-readable"):
        ServerConfig.from_dict(d)

    # 2. Mode 0600 is accepted
    os.chmod(pw_file, 0o600)
    cfg = ServerConfig.from_dict(d)
    assert "report" in cfg.connections["sr"].accounts


def test_require_grant_typ_defaults_to_true(
    valid_jwks_file,
    valid_placement_files,
    valid_secrets_file,
    valid_password_file,
):
    pf, sf = valid_placement_files
    d = _build_full_valid_dict(
        valid_jwks_file, pf, sf, valid_secrets_file, valid_password_file
    )
    del d["require_grant_typ"]

    cfg = ServerConfig.from_dict(d)
    assert cfg.require_grant_typ is True

    d["require_grant_typ"] = False
    cfg_false = ServerConfig.from_dict(d)
    assert cfg_false.require_grant_typ is False


def test_static_vs_discovery_exclusivity(
    valid_jwks_file,
    valid_placement_files,
    valid_secrets_file,
    valid_password_file,
):
    pf, sf = valid_placement_files
    d = _build_full_valid_dict(
        valid_jwks_file, pf, sf, valid_secrets_file, valid_password_file
    )

    # Neither
    d["placement"] = {}
    with pytest.raises(ServerConfigError, match="got neither"):
        ServerConfig.from_dict(d)

    # Both
    d["placement"] = {
        "static": {"placement_file": pf, "schemas_file": sf},
        "discovery": {"default_catalog": "lakekeeper"},
    }
    with pytest.raises(ServerConfigError, match="got both"):
        ServerConfig.from_dict(d)
