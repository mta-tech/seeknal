"""Tests for Seeknal Serving server CLI."""

from __future__ import annotations

import base64
import json
import ssl
from unittest.mock import MagicMock

import pytest
from cryptography.hazmat.primitives.asymmetric import ec

from seeknal.serving.__main__ import main


@pytest.fixture
def jwks_file(tmp_path):
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
        "kid": "cli-test-key",
    }
    p = tmp_path / "keys.jwks.json"
    p.write_text(json.dumps({"keys": [jwk]}))
    return str(p)


@pytest.fixture
def placement_files(tmp_path):
    pf = tmp_path / "placement.yaml"
    pf.write_text("""
engines:
  duck: {kind: duckdb, dialect: duckdb}
tables: {}
cubes: {}
""")
    sf = tmp_path / "schemas.json"
    sf.write_text("{}")
    return str(pf), str(sf)


@pytest.fixture
def cert_files(tmp_path):
    cert = tmp_path / "server.crt"
    cert.write_text("CERT")
    key = tmp_path / "server.key"
    key.write_text("KEY")
    ca = tmp_path / "catalog-ca.crt"
    ca.write_text("CA")
    return str(cert), str(key), str(ca)


def test_cli_check_ok(tmp_path, jwks_file, placement_files, monkeypatch, capsys):
    pf, sf = placement_files
    cfg_file = tmp_path / "serving.yaml"
    cfg_file.write_text(f"""
listen: {{host: 127.0.0.1, port: 8765}}
grant_keys: {{jwks_file: {jwks_file}}}
placement:
  static:
    placement_file: {pf}
    schemas_file: {sf}
connections:
  duck: {{kind: duckdb, path: ":memory:"}}
  lb: {{kind: lakebase, dsn_env: TEST_LAKEBASE_DSN}}
""")
    monkeypatch.setenv("TEST_LAKEBASE_DSN", "postgresql://user:pass@host/db")

    ret = main(["--config", str(cfg_file), "--check"])
    assert ret == 0
    captured = capsys.readouterr()
    assert "config ok" in captured.out


def test_cli_check_rejects_placement_engine_missing_from_connections(
    tmp_path, jwks_file, placement_files, monkeypatch, capsys
):
    pf, sf = placement_files
    secret_dsn = "postgresql://user:SUPER_SECRET_DSN_PW_999@host/db"
    monkeypatch.setenv("TEST_LAKEBASE_DSN", secret_dsn)

    cfg_file = tmp_path / "serving.yaml"
    cfg_file.write_text(f"""
listen: {{host: 127.0.0.1, port: 8765}}
grant_keys: {{jwks_file: {jwks_file}}}
placement:
  static:
    placement_file: {pf}
    schemas_file: {sf}
connections:
  lb: {{kind: lakebase, dsn_env: TEST_LAKEBASE_DSN}}
""")

    ret = main(["--config", str(cfg_file), "--check"])
    assert ret == 2
    captured = capsys.readouterr()
    assert "duck" in captured.err
    assert "connections.duck" in captured.err
    assert secret_dsn not in captured.err
    assert "SUPER_SECRET_DSN_PW_999" not in captured.err
    assert "Traceback" not in captured.err


def test_cli_check_missing_env_var(
    tmp_path, jwks_file, placement_files, monkeypatch, capsys
):
    secret_val = "SECRET-VALUE-SHOULD-NEVER-APPEAR-000"
    monkeypatch.setenv("EXISTING_SECRET_ENV", secret_val)
    monkeypatch.delenv("TEST_MISSING_DSN", raising=False)

    pf, sf = placement_files
    cfg_file = tmp_path / "serving.yaml"
    cfg_file.write_text(f"""
listen: {{host: 127.0.0.1, port: 8765}}
grant_keys: {{jwks_file: {jwks_file}}}
placement:
  static:
    placement_file: {pf}
    schemas_file: {sf}
connections:
  lb: {{kind: lakebase, dsn_env: TEST_MISSING_DSN}}
""")

    ret = main(["--config", str(cfg_file), "--check"])
    assert ret == 2
    captured = capsys.readouterr()

    # Must contain the variable name
    assert "TEST_MISSING_DSN" in captured.err
    # Must NOT contain any secret value
    assert secret_val not in captured.err
    assert secret_val not in captured.out
    # Must NOT contain a traceback
    assert "Traceback" not in captured.err


def test_cli_run_without_check_passes_uvicorn_kwargs(
    tmp_path, jwks_file, placement_files, cert_files, monkeypatch
):
    monkeypatch.setenv("SEEKNAL_CURSOR_KEY", "cli-test-cursor-key-32-bytes-ok-xx")
    pf, sf = placement_files
    cert, key, ca = cert_files
    cfg_file = tmp_path / "serving.yaml"
    cfg_file.write_text(f"""
listen: {{host: 0.0.0.0, port: 9000}}
tls:
  cert_file: {cert}
  key_file: {key}
  client_ca_file: {ca}
  require_client_cert: true
grant_keys: {{jwks_file: {jwks_file}}}
placement:
  static:
    placement_file: {pf}
    schemas_file: {sf}
connections:
  duck: {{kind: duckdb, path: ":memory:"}}
""")

    fake_run = MagicMock()
    ret = main(["--config", str(cfg_file)], run=fake_run)
    assert ret == 0
    assert fake_run.called

    _, kwargs = fake_run.call_args
    assert kwargs["host"] == "0.0.0.0"
    assert kwargs["port"] == 9000
    assert kwargs["ssl_certfile"] == cert
    assert kwargs["ssl_keyfile"] == key
    assert kwargs["ssl_ca_certs"] == ca
    assert kwargs["ssl_cert_reqs"] == ssl.CERT_REQUIRED


def test_cli_check_does_not_call_build_server_or_adapters_or_network(
    tmp_path, jwks_file, placement_files, monkeypatch, capsys
):
    import seeknal.serving.__main__ as cli_mod
    import seeknal.serving.adapters as adapters_mod
    import seeknal.serving.discovery as discovery_mod

    pf, sf = placement_files
    cfg_file = tmp_path / "serving.yaml"
    cfg_file.write_text(f"""
listen: {{host: 127.0.0.1, port: 8765}}
grant_keys: {{jwks_file: {jwks_file}}}
placement:
  static:
    placement_file: {pf}
    schemas_file: {sf}
connections:
  duck: {{kind: duckdb, path: ":memory:"}}
""")

    def _forbidden(*args, **kwargs):
        raise AssertionError("Must not be called during --check")

    monkeypatch.setattr(cli_mod, "build_server", _forbidden)
    monkeypatch.setattr(adapters_mod, "DuckDBAdapter", _forbidden)
    monkeypatch.setattr(adapters_mod, "build_adapter", _forbidden)
    monkeypatch.setattr(discovery_mod, "discover", _forbidden)
    monkeypatch.setattr(discovery_mod, "PlacementRefresher", _forbidden)

    ret = main(["--config", str(cfg_file), "--check"])
    assert ret == 0
    assert "config ok" in capsys.readouterr().out


def test_cli_malformed_yaml_and_unexpected_exception_never_leak_secrets(
    tmp_path, jwks_file, placement_files, monkeypatch, capsys
):
    import seeknal.serving.__main__ as cli_mod

    sentinel = "ULTRA_SECRET_SENTINEL_YAML_888"
    bad_cfg = tmp_path / "bad_serving.yaml"
    bad_cfg.write_text(
        f"listen: [unterminated_list_with_{sentinel}\n",
        encoding="utf-8",
    )

    ret = main(["--config", str(bad_cfg), "--check"])
    assert ret == 2
    captured = capsys.readouterr()
    assert "Invalid YAML in config file" in captured.err
    assert "line 2, column 1" in captured.err
    assert sentinel not in captured.err
    assert sentinel not in captured.out
    assert "Traceback" not in captured.err

    # Unexpected runtime exception produces a generic error without leaking exception message or traceback
    pf, sf = placement_files
    good_cfg = tmp_path / "good_serving.yaml"
    good_cfg.write_text(f"""
listen: {{host: 127.0.0.1, port: 8765}}
grant_keys: {{jwks_file: {jwks_file}}}
placement:
  static:
    placement_file: {pf}
    schemas_file: {sf}
connections:
  duck: {{kind: duckdb, path: ":memory:"}}
""")

    def _boom(*args, **kwargs):
        raise RuntimeError(f"internal explosion with {sentinel}")

    monkeypatch.setattr(cli_mod.ServerConfig, "from_yaml", _boom)
    ret_boom = main(["--config", str(good_cfg), "--check"])
    assert ret_boom == 2
    captured_boom = capsys.readouterr()
    assert sentinel not in captured_boom.err
    assert sentinel not in captured_boom.out
    assert "Traceback" not in captured_boom.err
    assert "Error:" in captured_boom.err


def test_cli_check_invalid_placement_kind_never_leaks_dsn_or_canary(
    tmp_path, jwks_file, capsys
):
    dsn = "postgresql://demo:syntheticCanary@host/db"
    pf = tmp_path / "placement.yaml"
    pf.write_text(f"""
engines:
  duck:
    kind: "{dsn}"
    dialect: duckdb
tables: {{}}
cubes: {{}}
""")
    sf = tmp_path / "schemas.json"
    sf.write_text("{}")
    cfg_file = tmp_path / "serving.yaml"
    cfg_file.write_text(f"""
listen: {{host: 127.0.0.1, port: 8765}}
grant_keys: {{jwks_file: {jwks_file}}}
placement:
  static:
    placement_file: {pf}
    schemas_file: {sf}
connections:
  duck: {{kind: duckdb, path: ":memory:"}}
""")

    ret = main(["--config", str(cfg_file), "--check"])
    assert ret == 2
    captured = capsys.readouterr()
    assert dsn not in captured.err
    assert "syntheticCanary" not in captured.err
    assert dsn not in captured.out
    assert "syntheticCanary" not in captured.out
    assert str(pf) in captured.err
    assert "engines" in captured.err
