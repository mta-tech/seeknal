"""Tests for Seeknal Serving server configuration."""

from __future__ import annotations

import base64
import json
import os
import stat
import pytest
import yaml
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec

from seeknal.serving.server.config import (
    LakebaseConnectionConfig,
    PostgresConnectionConfig,
    PreflightResult,
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


def test_validate_runtime_wiring_static_and_extra_connections(
    tmp_path, valid_jwks_file
):
    pf = tmp_path / "placement.yaml"
    pf.write_text("""
engines:
  duck: {kind: duckdb, dialect: duckdb}
  cube: {kind: cube, dialect: postgres}
tables:
  Lakekeeper.Users:
    keys: [Id]
    row_count: 10
    serve:
      duck: users
  lakekeeper.empty_tbl:
    keys: []
    row_count: 0
    serve:
      duck: empty_tbl
cubes:
  SalesCube:
    engine: cube
    measures: [total_amount]
    dimensions: [region]
""")
    sf = tmp_path / "schemas.json"
    sf.write_text(
        json.dumps(
            {
                "lakekeeper.users": ["Id", "UserName"],
                "lakekeeper.empty_tbl": [],
                "salescube": ["region", "total_amount"],
            }
        )
    )

    cfg = ServerConfig.from_dict(
        {
            "grant_keys": {"jwks_file": valid_jwks_file},
            "cube_mode": "refuse",
            "placement": {
                "static": {
                    "placement_file": str(pf),
                    "schemas_file": str(sf),
                }
            },
            "connections": {
                "duck": {"kind": "duckdb", "path": ":memory:"},
                "cube": {
                    "kind": "cube",
                    "host": "cube",
                    "port": 15432,
                    "dbname": "db",
                    "sql_user": "seeknal",
                },
                # Extra unreferenced connection is allowed
                "extra_lb": {"kind": "lakebase", "dsn_env": "EXTRA_LB_DSN"},
            },
        }
    )

    res = cfg.validate_runtime_wiring(
        env={"EXTRA_LB_DSN": "postgresql://u:p@localhost/db"}
    )
    assert isinstance(res, PreflightResult)
    assert res.mode == "static"
    assert res.placement is not None
    assert res.schemas is not None
    # Original casing and order preserved
    assert res.schemas["lakekeeper.users"] == ["Id", "UserName"]
    assert res.schemas["lakekeeper.empty_tbl"] == []
    assert res.schemas["salescube"] == ["region", "total_amount"]


def test_validate_runtime_wiring_static_engine_errors(tmp_path, valid_jwks_file):
    # Declared engine in placement (even if unused by tables or cube_mode=refuse) must exist in connections
    pf = tmp_path / "placement.yaml"
    pf.write_text("""
engines:
  duck: {kind: duckdb, dialect: duckdb}
  cube: {kind: cube, dialect: postgres}
tables:
  users:
    keys: [id]
    row_count: 1
    serve:
      duck: users
cubes: {}
""")
    sf = tmp_path / "schemas.json"
    sf.write_text(json.dumps({"users": ["id"]}))

    # 1. Missing cube connection even when cube_mode=refuse and no cubes exist
    cfg_missing = ServerConfig.from_dict(
        {
            "grant_keys": {"jwks_file": valid_jwks_file},
            "cube_mode": "refuse",
            "placement": {
                "static": {"placement_file": str(pf), "schemas_file": str(sf)}
            },
            "connections": {
                "duck": {"kind": "duckdb", "path": ":memory:"},
            },
        }
    )
    with pytest.raises(ServerConfigError, match="connections.cube"):
        cfg_missing.validate_runtime_wiring()

    # 2. Kind mismatch between placement engine and connection
    cfg_mismatch = ServerConfig.from_dict(
        {
            "grant_keys": {"jwks_file": valid_jwks_file},
            "cube_mode": "refuse",
            "placement": {
                "static": {"placement_file": str(pf), "schemas_file": str(sf)}
            },
            "connections": {
                "duck": {"kind": "duckdb", "path": ":memory:"},
                "cube": {"kind": "duckdb", "path": ":memory:"},
            },
        }
    )
    with pytest.raises(ServerConfigError, match="kind mismatch"):
        cfg_mismatch.validate_runtime_wiring()

    # 3. Missing declared engines with alphanumeric/hyphenated names (duck2, duck-prod) are named in error
    for eng_id in ("duck2", "duck-prod"):
        pf.write_text(f"""
engines:
  {eng_id}: {{kind: duckdb, dialect: duckdb}}
tables:
  users:
    keys: [id]
    row_count: 1
    serve:
      {eng_id}: users
cubes: {{}}
""")
        cfg_eng = ServerConfig.from_dict(
            {
                "grant_keys": {"jwks_file": valid_jwks_file},
                "placement": {
                    "static": {"placement_file": str(pf), "schemas_file": str(sf)}
                },
                "connections": {
                    "duck": {"kind": "duckdb", "path": ":memory:"},
                },
            }
        )
        with pytest.raises(ServerConfigError) as exc_eng:
            cfg_eng.validate_runtime_wiring()
        assert f"connections.{eng_id}" in str(exc_eng.value)


def test_validate_runtime_wiring_static_schemas_errors(tmp_path, valid_jwks_file):
    pf = tmp_path / "placement.yaml"
    pf.write_text("""
engines:
  duck: {kind: duckdb, dialect: duckdb}
tables:
  users:
    keys: [id]
    row_count: 1
    serve:
      duck: users
cubes: {}
""")
    sf = tmp_path / "schemas.json"

    def _make_cfg() -> ServerConfig:
        return ServerConfig.from_dict(
            {
                "grant_keys": {"jwks_file": valid_jwks_file},
                "placement": {
                    "static": {"placement_file": str(pf), "schemas_file": str(sf)}
                },
                "connections": {
                    "duck": {"kind": "duckdb", "path": ":memory:"},
                },
            }
        )

    # 1. Missing schema key for 'users'
    sf.write_text("{}")
    with pytest.raises(ServerConfigError, match="missing schema entry.*'users'"):
        _make_cfg().validate_runtime_wiring()

    # 2. Stale/extra schema key
    sf.write_text(json.dumps({"users": ["id"], "stale_orders": ["order_id"]}))
    with pytest.raises(ServerConfigError, match="stale/extra schema key.*'stale_orders'"):
        _make_cfg().validate_runtime_wiring()

    # 3. Case-insensitive key collision in schemas.json
    sf.write_text('{"users": ["id"], "USERS": ["id"]}')
    with pytest.raises(ServerConfigError, match="case-insensitive key collision"):
        _make_cfg().validate_runtime_wiring()

    # 4. Exact duplicate JSON key in schemas.json
    sf.write_text('{"users": ["id"], "users": ["id", "name"]}')
    with pytest.raises(ServerConfigError, match="duplicate JSON key 'users'"):
        _make_cfg().validate_runtime_wiring()

    # 5. Root not a mapping
    sf.write_text('["users"]')
    with pytest.raises(ServerConfigError, match="root must be a JSON object"):
        _make_cfg().validate_runtime_wiring()

    # 6. Value not a list
    sf.write_text('{"users": {"id": "int"}}')
    with pytest.raises(ServerConfigError, match="must be a JSON list"):
        _make_cfg().validate_runtime_wiring()

    # 7. Invalid column entries (empty string / non-string)
    sf.write_text('{"users": ["id", "   "]}')
    with pytest.raises(ServerConfigError, match="invalid column at index 1"):
        _make_cfg().validate_runtime_wiring()

    sf.write_text('{"users": ["id", 123]}')
    with pytest.raises(ServerConfigError, match="invalid column at index 1"):
        _make_cfg().validate_runtime_wiring()

    # 8. Case-insensitive duplicate columns
    sf.write_text('{"users": ["UserId", "userid"]}')
    with pytest.raises(ServerConfigError, match="duplicate column"):
        _make_cfg().validate_runtime_wiring()


def test_validate_runtime_wiring_discovery_mode(tmp_path, valid_jwks_file):
    # 1. Valid discovery mode with empty engines and extra connection
    cfg_empty = ServerConfig.from_dict(
        {
            "grant_keys": {"jwks_file": valid_jwks_file},
            "placement": {
                "discovery": {
                    "default_catalog": "lakekeeper",
                }
            },
            "connections": {
                "duck": {"kind": "duckdb", "path": ":memory:"},
            },
        }
    )
    res_empty = cfg_empty.validate_runtime_wiring()
    assert res_empty.mode == "discovery"
    assert res_empty.placement is None
    assert res_empty.schemas is None

    # 2. Valid discovery mode with engines + lakebase + extra connection
    cfg_valid = ServerConfig.from_dict(
        {
            "grant_keys": {"jwks_file": valid_jwks_file},
            "placement": {
                "discovery": {
                    "default_catalog": "lakekeeper",
                    "engines": {
                        "lb": {"kind": "lakebase", "dialect": "postgres"},
                        "cube": {
                            "kind": "cube",
                            "dialect": "postgres",
                            "url": "http://cube:4000",
                            "api_secret_env": "CUBE_META_SEC",
                        },
                    },
                    "lakebase": {
                        "engine": "lb",
                    },
                }
            },
            "connections": {
                "lb": {"kind": "lakebase", "dsn_env": "LB_DSN"},
                "cube": {
                    "kind": "cube",
                    "host": "cube",
                    "port": 15432,
                    "dbname": "db",
                    "sql_user": "seeknal",
                },
                "extra_duck": {"kind": "duckdb", "path": ":memory:"},
            },
        }
    )
    res_valid = cfg_valid.validate_runtime_wiring(
        env={"LB_DSN": "postgresql://u:p@h/db", "CUBE_META_SEC": "sec"}
    )
    assert res_valid.mode == "discovery"

    # 3. Missing discovery engine in connections
    cfg_missing_eng = ServerConfig.from_dict(
        {
            "grant_keys": {"jwks_file": valid_jwks_file},
            "placement": {
                "discovery": {
                    "default_catalog": "lakekeeper",
                    "engines": {
                        "lb": {"kind": "lakebase", "dialect": "postgres"},
                    },
                }
            },
            "connections": {
                "duck": {"kind": "duckdb", "path": ":memory:"},
            },
        }
    )
    with pytest.raises(ServerConfigError, match="connections.lb"):
        cfg_missing_eng.validate_runtime_wiring()

    # 4. Discovery engine kind mismatch in connections
    cfg_kind_mismatch = ServerConfig.from_dict(
        {
            "grant_keys": {"jwks_file": valid_jwks_file},
            "placement": {
                "discovery": {
                    "default_catalog": "lakekeeper",
                    "engines": {
                        "lb": {"kind": "lakebase", "dialect": "postgres"},
                    },
                }
            },
            "connections": {
                "lb": {"kind": "duckdb", "path": ":memory:"},
            },
        }
    )
    with pytest.raises(ServerConfigError, match="has kind 'duckdb'"):
        cfg_kind_mismatch.validate_runtime_wiring()


def test_malformed_files_and_secret_sentinels_never_leak(tmp_path, valid_jwks_file):
    sentinel = "SUPER_SECRET_SENTINEL_TOKEN_777"

    # 1. Malformed config YAML
    bad_cfg = tmp_path / "bad_cfg.yaml"
    bad_cfg.write_text(f"connections: {{bad: [{sentinel}\n", encoding="utf-8")
    with pytest.raises(ServerConfigError) as exc_cfg:
        ServerConfig.from_yaml(bad_cfg)
    assert "line 2, column 1" in str(exc_cfg.value)
    assert sentinel not in str(exc_cfg.value)

    # 2. Malformed placement YAML
    pf = tmp_path / "bad_placement.yaml"
    pf.write_text(f"engines: {{duck: [{sentinel}\n", encoding="utf-8")
    sf = tmp_path / "schemas.json"
    sf.write_text("{}")

    cfg = ServerConfig.from_dict(
        {
            "grant_keys": {"jwks_file": valid_jwks_file},
            "placement": {
                "static": {"placement_file": str(pf), "schemas_file": str(sf)}
            },
            "connections": {"duck": {"kind": "duckdb", "path": ":memory:"}},
        }
    )
    with pytest.raises(ServerConfigError) as exc_pl:
        cfg.validate_runtime_wiring()
    assert "Invalid YAML in static placement file" in str(exc_pl.value)
    assert "line 2, column 1" in str(exc_pl.value)
    assert sentinel not in str(exc_pl.value)

    # 3. Malformed schemas JSON
    pf.write_text("engines:\n  duck: {kind: duckdb, dialect: duckdb}\ntables: {}\ncubes: {}\n")
    sf.write_text(f'{{"users": ["{sentinel}"\n')
    with pytest.raises(ServerConfigError) as exc_sc:
        cfg.validate_runtime_wiring()
    assert "Invalid JSON in static schemas file" in str(exc_sc.value)
    assert "line" in str(exc_sc.value)
    assert sentinel not in str(exc_sc.value)

    # 4. Secret-like / URI-like value in stale schema key or column is redacted
    sf.write_text(json.dumps({f"postgresql://user:{sentinel}@host/db": ["id"]}))
    with pytest.raises(ServerConfigError) as exc_uri:
        cfg.validate_runtime_wiring()
    assert sentinel not in str(exc_uri.value)
    assert "<redacted>" in str(exc_uri.value)


def test_placement_invalid_payloads_and_nested_structures_never_leak_dsn_or_canary(
    tmp_path, valid_jwks_file
):
    dsn = "postgresql://demo:syntheticCanary@host/db"
    canary = "syntheticCanary"
    opaque = "opaqueValueX91z"

    pf = tmp_path / "placement.yaml"
    sf = tmp_path / "schemas.json"
    sf.write_text(json.dumps({"users": ["id"]}))

    def _make_cfg() -> ServerConfig:
        return ServerConfig.from_dict(
            {
                "grant_keys": {"jwks_file": valid_jwks_file},
                "placement": {
                    "static": {"placement_file": str(pf), "schemas_file": str(sf)}
                },
                "connections": {
                    "duck": {"kind": "duckdb", "path": ":memory:"},
                    "cube": {
                        "kind": "cube",
                        "host": "cube",
                        "port": 15432,
                        "dbname": "db",
                        "sql_user": "seeknal",
                    },
                },
            }
        )

    # 1. Invalid engine kind containing a full DSN
    pf.write_text(
        yaml.safe_dump(
            {
                "engines": {"duck": {"kind": dsn, "dialect": "duckdb"}},
                "tables": {},
                "cubes": {},
            }
        )
    )
    with pytest.raises(ServerConfigError) as exc_kind:
        _make_cfg().validate_runtime_wiring()
    msg_kind = str(exc_kind.value)
    assert dsn not in msg_kind
    assert canary not in msg_kind
    assert str(pf) in msg_kind
    assert "engines" in msg_kind

    # 2. Invalid mask_variants base / method containing opaque sentinel
    pf.write_text(
        yaml.safe_dump(
            {
                "engines": {"cube": {"kind": "cube", "dialect": "postgres"}},
                "tables": {},
                "cubes": {
                    "sales": {
                        "engine": "cube",
                        "measures": ["m1"],
                        "dimensions": ["d1"],
                        "mask_variants": {"d1": [opaque, dsn]},
                    }
                },
            }
        )
    )
    with pytest.raises(ServerConfigError) as exc_mv:
        _make_cfg().validate_runtime_wiring()
    msg_mv = str(exc_mv.value)
    assert opaque not in msg_mv
    assert dsn not in msg_mv
    assert canary not in msg_mv
    assert str(pf) in msg_mv
    assert "cubes" in msg_mv

    # 3. Malformed nested serve (list instead of mapping) raises ServerConfigError, not AttributeError
    pf.write_text(
        yaml.safe_dump(
            {
                "engines": {"duck": {"kind": "duckdb", "dialect": "duckdb"}},
                "tables": {
                    "users": {
                        "keys": ["id"],
                        "row_count": 1,
                        "serve": [dsn, opaque],
                    }
                },
                "cubes": {},
            }
        )
    )
    with pytest.raises(ServerConfigError) as exc_serve:
        _make_cfg().validate_runtime_wiring()
    msg_serve = str(exc_serve.value)
    assert dsn not in msg_serve
    assert canary not in msg_serve
    assert opaque not in msg_serve
    assert "tables.users.serve" in msg_serve


def test_placement_model_compatibility_coercions_and_metadata(
    tmp_path, valid_jwks_file
):
    pf = tmp_path / "placement.yaml"
    pf.write_text(
        yaml.safe_dump(
            {
                "version": "1",
                "metadata": {"owner": "analytics"},
                "engines": {
                    # dialect is optional in Placement.from_dict
                    "duck": {"kind": "duckdb"},
                },
                "tables": {
                    "users": {
                        "keys": ["id"],
                        "row_count": "100",
                        "serve": {
                            "duck": {
                                "name": "users",
                                "max_lag_seconds": "5.5",
                            }
                        },
                    }
                },
                "cubes": {},
                "policy": {
                    "point_lookup_max_keys": "10",
                    "analytics_min_rows": "5000",
                },
            }
        ),
        encoding="utf-8",
    )
    sf = tmp_path / "schemas.json"
    sf.write_text(json.dumps({"users": ["id"]}), encoding="utf-8")

    cfg = ServerConfig.from_dict(
        {
            "grant_keys": {"jwks_file": valid_jwks_file},
            "placement": {
                "static": {"placement_file": str(pf), "schemas_file": str(sf)}
            },
            "connections": {"duck": {"kind": "duckdb", "path": ":memory:"}},
        }
    )
    res = cfg.validate_runtime_wiring()
    assert res.placement is not None
    assert res.placement.policy.point_lookup_max_keys == 10
    assert res.placement.policy.analytics_min_rows == 5000
    assert res.placement.tables["users"].row_count == 100
    assert res.placement.tables["users"].serves[0].max_lag_seconds == 5.5


def test_unicode_casefold_vs_lowercase_alignment(tmp_path, valid_jwks_file):
    pf = tmp_path / "placement.yaml"
    sf = tmp_path / "schemas.json"

    def _make_cfg() -> ServerConfig:
        return ServerConfig.from_dict(
            {
                "grant_keys": {"jwks_file": valid_jwks_file},
                "cube_mode": "refuse",
                "placement": {
                    "static": {"placement_file": str(pf), "schemas_file": str(sf)}
                },
                "connections": {
                    "duck": {"kind": "duckdb", "path": ":memory:"},
                    "cube": {
                        "kind": "cube",
                        "host": "cube",
                        "port": 15432,
                        "dbname": "db",
                        "sql_user": "seeknal",
                    },
                },
            }
        )

    # 1. Single table 'straße' in placement with only 'STRASSE' in schemas.json:
    # .casefold() matches ('strasse' == 'strasse'), but runtime .lower() differs ('straße' != 'strasse')
    pf.write_text(
        yaml.safe_dump(
            {
                "engines": {"duck": {"kind": "duckdb", "dialect": "duckdb"}},
                "tables": {
                    "straße": {
                        "keys": ["id"],
                        "row_count": 1,
                        "serve": {"duck": "strasse_tbl"},
                    }
                },
                "cubes": {},
            },
            allow_unicode=True,
        ),
        encoding="utf-8",
    )
    sf.write_text(json.dumps({"STRASSE": ["id"]}), encoding="utf-8")
    with pytest.raises(ServerConfigError, match="differs under lowercase"):
        _make_cfg().validate_runtime_wiring()

    # 2. Table 'straße' + cube 'STRASSE' in placement collides under casefold
    pf.write_text(
        yaml.safe_dump(
            {
                "engines": {
                    "duck": {"kind": "duckdb", "dialect": "duckdb"},
                    "cube": {"kind": "cube", "dialect": "postgres"},
                },
                "tables": {
                    "straße": {
                        "keys": ["id"],
                        "row_count": 1,
                        "serve": {"duck": "strasse_tbl"},
                    }
                },
                "cubes": {
                    "STRASSE": {
                        "engine": "cube",
                        "measures": ["m1"],
                        "dimensions": ["d1"],
                    }
                },
            },
            allow_unicode=True,
        ),
        encoding="utf-8",
    )
    sf.write_text(json.dumps({"straße": ["id"]}, ensure_ascii=False), encoding="utf-8")
    with pytest.raises(ServerConfigError, match="collides under casefold"):
        _make_cfg().validate_runtime_wiring()


def test_compilation_cache_max_entries_config_and_constructor_agreement(
    valid_jwks_file,
    valid_placement_files,
    valid_secrets_file,
    valid_password_file,
):
    """ServerConfig YAML/dict and constructor agree on compilation_cache_max_entries (default 256, 0..1024)."""
    pf, sf = valid_placement_files
    base_d = _build_full_valid_dict(
        valid_jwks_file, pf, sf, valid_secrets_file, valid_password_file
    )

    # Default is 256
    cfg_default = ServerConfig.from_dict(base_d)
    assert cfg_default.compilation_cache_max_entries == 256

    # Valid values: 0 (disabled), 1, 256, 1024
    for valid_val in (0, 1, 128, 256, 1024):
        cfg = ServerConfig.from_dict({**base_d, "compilation_cache_max_entries": valid_val})
        assert cfg.compilation_cache_max_entries == valid_val

    # Invalid values: bool, float, negative, >1024, string, None rejected in both from_dict and constructor
    for bad_val in (True, False, -1, -50, 1025, 2000, 1.5, 0.0, "256", None):
        with pytest.raises(ServerConfigError, match="compilation_cache_max_entries"):
            ServerConfig.from_dict({**base_d, "compilation_cache_max_entries": bad_val})
        with pytest.raises(ServerConfigError, match="compilation_cache_max_entries"):
            ServerConfig(
                listen=cfg_default.listen,
                grant_keys=cfg_default.grant_keys,
                placement=cfg_default.placement,
                connections=cfg_default.connections,
                compilation_cache_max_entries=bad_val,  # type: ignore[arg-type]
            )


def test_lakebase_pool_config_validation(
    valid_jwks_file,
    valid_placement_files,
    valid_secrets_file,
    valid_password_file,
):
    pf, sf = valid_placement_files
    d = _build_full_valid_dict(
        valid_jwks_file, pf, sf, valid_secrets_file, valid_password_file
    )
    # Valid pool config
    d["connections"]["lb"] = {
        "kind": "lakebase",
        "dsn_env": "LAKEBASE_DSN",
        "pool_max": 16,
        "pool_min": 2,
        "connect_timeout_seconds": 5.0,
        "max_idle_seconds": 120.0,
        "max_lifetime_seconds": 600.0,
        "acquire_timeout_seconds": 3.0,
    }
    cfg = ServerConfig.from_dict(d)
    lb_cfg = cfg.connections["lb"]
    assert lb_cfg.pool_max == 16
    assert lb_cfg.pool_min == 2
    assert lb_cfg.connect_timeout_seconds == 5.0
    assert lb_cfg.max_idle_seconds == 120.0
    assert lb_cfg.max_lifetime_seconds == 600.0
    assert lb_cfg.acquire_timeout_seconds == 3.0

    # pool_max out of range (>64)
    d["connections"]["lb"]["pool_max"] = 65
    with pytest.raises(ServerConfigError, match="'pool_max' must be an integer between 1 and 64"):
        ServerConfig.from_dict(d)

    # pool_min > pool_max
    d["connections"]["lb"]["pool_max"] = 4
    d["connections"]["lb"]["pool_min"] = 5
    with pytest.raises(ServerConfigError, match="'pool_min' must be an integer between 0 and pool_max"):
        ServerConfig.from_dict(d)

    # Unknown key refused
    d["connections"]["lb"]["pool_min"] = 1
    d["connections"]["lb"]["invalid_key"] = "bad"
    with pytest.raises(ServerConfigError, match="Unknown key"):
        ServerConfig.from_dict(d)
    del d["connections"]["lb"]["invalid_key"]

    # Float config keys reject booleans and strings
    d["connections"]["lb"]["connect_timeout_seconds"] = "10"
    with pytest.raises(ServerConfigError, match="must be a positive number"):
        ServerConfig.from_dict(d)

    d["connections"]["lb"]["connect_timeout_seconds"] = True
    with pytest.raises(ServerConfigError, match="must be a positive number"):
        ServerConfig.from_dict(d)

    d["connections"]["lb"]["connect_timeout_seconds"] = 10.0
    d["connections"]["lb"]["max_idle_seconds"] = False
    with pytest.raises(ServerConfigError, match="must be a positive number"):
        ServerConfig.from_dict(d)


def test_duckdb_temp_directory_config_validation(
    tmp_path,
    valid_jwks_file,
    valid_placement_files,
    valid_secrets_file,
    valid_password_file,
):
    pf, sf = valid_placement_files
    d = _build_full_valid_dict(
        valid_jwks_file, pf, sf, valid_secrets_file, valid_password_file
    )
    temp_dir = tmp_path / "duck_temp"
    temp_dir.mkdir()
    allowed_data_dir = tmp_path / "data"
    allowed_data_dir.mkdir()

    # Valid temp_directory and max_temp_directory_size
    d["connections"]["duck"] = {
        "kind": "duckdb",
        "path": str(allowed_data_dir / "app.duckdb"),
        "allowed_directories": [str(allowed_data_dir) + "/"],
        "temp_directory": str(temp_dir),
        "max_temp_directory_size": "500MB",
    }
    cfg = ServerConfig.from_dict(d)
    duck_cfg = cfg.connections["duck"]
    assert duck_cfg.temp_directory == str(temp_dir)
    assert duck_cfg.max_temp_directory_size == "500MB"

    # temp_directory inside allowed_directories is rejected
    inside_dir = allowed_data_dir / "spill"
    inside_dir.mkdir()
    d["connections"]["duck"]["temp_directory"] = str(inside_dir)
    with pytest.raises(ServerConfigError, match="must not be inside"):
        ServerConfig.from_dict(d)

    # temp_directory inside data directory is rejected even when allowed_directories is empty
    d["connections"]["duck"]["allowed_directories"] = []
    d["connections"]["duck"]["temp_directory"] = str(inside_dir)
    with pytest.raises(ServerConfigError, match="must not be inside data directory"):
        ServerConfig.from_dict(d)

    # Relative temp_directory rejected
    d["connections"]["duck"]["temp_directory"] = "relative/temp"
    with pytest.raises(ServerConfigError, match="must be an absolute path"):
        ServerConfig.from_dict(d)

    # Unknown key refused
    d["connections"]["duck"]["temp_directory"] = str(temp_dir)
    d["connections"]["duck"]["unknown_duck_key"] = "bad"
    with pytest.raises(ServerConfigError, match="Unknown key"):
        ServerConfig.from_dict(d)


def test_postgres_and_lakebase_connections_coexist(
    valid_jwks_file,
    valid_placement_files,
    valid_secrets_file,
    valid_password_file,
):
    pf, sf = valid_placement_files
    d = _build_full_valid_dict(
        valid_jwks_file, pf, sf, valid_secrets_file, valid_password_file
    )
    d["connections"]["pg_report"] = {
        "kind": "postgres",
        "dsn_env": "CBN_PG_DSN",
    }
    cfg = ServerConfig.from_dict(d)
    assert isinstance(cfg.connections["pg_report"], PostgresConnectionConfig)
    assert cfg.connections["pg_report"].mode == "read_only"
    assert isinstance(cfg.connections["lb"], LakebaseConnectionConfig)
    assert cfg.connections["lb"].is_legacy is True
    assert "CBN_PG_DSN" in cfg.get_referenced_env_vars()


def test_parse_jwks_refuses_non_canonical_coordinate():
    alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
    for _ in range(20):
        priv = ec.generate_private_key(ec.SECP256R1())
        pub = priv.public_key()
        pn = pub.public_numbers()
        x_bytes = pn.x.to_bytes(32, "big")
        y_bytes = pn.y.to_bytes(32, "big")
        x_b64 = base64.urlsafe_b64encode(x_bytes).rstrip(b"=").decode("ascii")
        y_b64 = base64.urlsafe_b64encode(y_bytes).rstrip(b"=").decode("ascii")
        valid_jwk = {
            "kty": "EC",
            "crv": "P-256",
            "x": x_b64,
            "y": y_b64,
            "kid": "atlas-2026-test",
        }

        # Valid JWKS parses
        res = parse_jwks({"keys": [valid_jwk]})
        assert "atlas-2026-test" in res

        # Find non-canonical last character for x coordinate that decodes to the exact same bytes
        non_canonical_x = None
        for c in alphabet:
            if c == x_b64[-1]:
                continue
            cand = x_b64[:-1] + c
            cand_padded = cand + ("=" * ((4 - len(cand) % 4) % 4))
            try:
                cand_bytes = base64.urlsafe_b64decode(cand_padded.encode("ascii"))
                if cand_bytes == x_bytes:
                    non_canonical_x = cand
                    break
            except Exception:
                pass

        assert non_canonical_x is not None, "Failed to find non-canonical x coordinate"
        non_canonical_padded = non_canonical_x + (
            "=" * ((4 - len(non_canonical_x) % 4) % 4)
        )
        assert base64.urlsafe_b64decode(non_canonical_padded.encode("ascii")) == x_bytes

        bad_jwk = dict(valid_jwk, x=non_canonical_x)
        with pytest.raises(ServerConfigError, match="invalid base64url coordinate"):
            parse_jwks({"keys": [bad_jwk]})


