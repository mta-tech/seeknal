"""Tests for the load testing harness (item 5)."""

from __future__ import annotations

import asyncio
import hashlib
import json
import tempfile
import time
from pathlib import Path
from unittest.mock import MagicMock

import duckdb
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec

from seeknal.serving import (
    DuckDBAdapter,
    Placement,
    ServingPipeline,
    create_app,
    sign_grant_token,
)
from seeknal.serving.proving.load import (
    LoadReport,
    load_cases,
    load_claims_template,
    load_tokens,
    main as load_main,
    make_grant_minter,
    run_load,
)
from seeknal.serving.server.config import ServerConfigError


@pytest.fixture
def ec_keys():
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()
    return priv, pub


@pytest.fixture
def test_app(ec_keys):
    priv, pub = ec_keys
    con = duckdb.connect()
    con.execute("CREATE TABLE users (id INT, name VARCHAR, amount DOUBLE)")
    con.execute("INSERT INTO users VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0)")

    placement = Placement.from_dict({
        "engines": {
            "duck": {"kind": "duckdb", "dialect": "duckdb"},
        },
        "tables": {
            "users": {
                "keys": ["id"],
                "row_count": 50,
                "serve": {"duck": "users"},
            }
        },
        "cubes": {},
    })
    schemas = {"users": ["id", "name", "amount"]}
    pipeline = ServingPipeline(
        placement=placement,
        schemas=schemas,
        public_keys={"k1": pub},
    )
    adapter = DuckDBAdapter(lambda: con.cursor())
    app = create_app(pipeline, {"duck": adapter})
    return app, priv


def _make_token(priv, sql: str, jti: str) -> str:
    now = time.time()
    claims = {
        "sub": "analyst@example.com",
        "ws": "analytics",
        "jti": jti,
        "iss": "atlas-catalog",
        "aud": "seeknal-serving",
        "iat": now,
        "exp": now + 120,
        "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
        "max_rows": 100,
        "tables": {
            "users": {
                "columns": ["id", "name", "amount"],
                "masks": {},
                "row_filters": [],
                "actions": ["select"],
            }
        },
    }
    return sign_grant_token(claims, priv, kid="k1")


def test_run_load_against_in_process_starlette_duckdb(test_app):
    """Test run_load against in-process Starlette app with DuckDB adapter and check per-rule grouping."""
    app, priv = test_app

    sql_small_scan = "SELECT id, name FROM users WHERE id = 1"
    sql_analytics = "SELECT count(*), sum(amount) FROM users"

    claims_template = {
        "sub": "analyst@example.com",
        "ws": "analytics",
        "iss": "atlas-catalog",
        "aud": "seeknal-serving",
        "max_rows": 100,
        "tables": {
            "users": {
                "columns": ["id", "name", "amount"],
                "masks": {},
                "row_filters": [],
                "actions": ["select"],
            }
        },
    }
    minter = make_grant_minter(priv, kid="k1", claims_template=claims_template)

    cases = [
        {"name": "case_small", "sql": sql_small_scan},
        {"name": "case_analytics", "sql": sql_analytics},
    ]

    report = asyncio.run(
        run_load(
            app=app,
            cases=cases,
            mint_token=minter,
            concurrency=1,
            duration=0.3,
        )
    )

    assert isinstance(report, LoadReport)
    assert report.total_requests > 0
    assert report.total_errors == 0
    assert report.duration_seconds > 0

    # Verify per-rule grouping
    assert "small_scan" in report.by_rule
    assert "analytics" in report.by_rule

    stats_small = report.by_rule["small_scan"]
    assert stats_small.request_count > 0
    assert stats_small.p50_ms > 0
    assert stats_small.p50_ms <= stats_small.p95_ms <= stats_small.p99_ms
    assert len(stats_small.errors_by_status) == 0

    stats_analytics = report.by_rule["analytics"]
    assert stats_analytics.request_count > 0
    assert stats_analytics.p50_ms > 0
    assert stats_analytics.p50_ms <= stats_analytics.p95_ms <= stats_analytics.p99_ms

    summary = report.format_summary()
    assert "small_scan" in summary
    assert "analytics" in summary
    assert "p50=" in summary


def test_run_load_with_token_file_and_cases(test_app, tmp_path):
    """Test loading cases with minter from key and template files."""
    app, priv = test_app

    sql = "SELECT id FROM users"

    key_pem = priv.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    key_file = tmp_path / "key.pem"
    key_file.write_bytes(key_pem)
    key_file.chmod(0o600)

    claims_template = {
        "sub": "analyst@example.com",
        "ws": "analytics",
        "iss": "atlas-catalog",
        "aud": "seeknal-serving",
        "max_rows": 100,
        "tables": {
            "users": {
                "columns": ["id", "name", "amount"],
                "masks": {},
                "row_filters": [],
                "actions": ["select"],
            }
        },
    }
    template_file = tmp_path / "template.json"
    template_file.write_text(json.dumps(claims_template), encoding="utf-8")

    cases_file = tmp_path / "cases.json"
    cases_file.write_text(
        json.dumps([{"name": "test_case", "sql": sql}]), encoding="utf-8"
    )

    cases = load_cases(str(cases_file))
    template = load_claims_template(str(template_file))
    minter = make_grant_minter(key_file.read_bytes(), kid="k1", claims_template=template)

    assert len(cases) == 1

    report = asyncio.run(
        run_load(
            app=app,
            cases=cases,
            mint_token=minter,
            concurrency=1,
            duration=0.2,
        )
    )

    assert report.total_requests > 0
    assert report.total_errors == 0


def test_run_load_reports_error_counts_by_status(test_app):
    """Test that errors during load test are reported by status code."""
    app, priv = test_app

    # SQL not matching grant token -> 403 EnforcementError
    sql_grant = "SELECT id FROM users"
    sql_send = "SELECT id, name FROM users"
    token = _make_token(priv, sql_grant, "jti-err-1")

    cases = [
        {"name": "err_case", "sql": sql_send, "token": token},
    ]

    report = asyncio.run(
        run_load(
            app=app,
            cases=cases,
            concurrency=1,
            duration=0.2,
        )
    )

    assert report.total_requests > 0
    assert report.total_errors == report.total_requests
    rule_stats = next(iter(report.by_rule.values()))
    assert 403 in rule_stats.errors_by_status
    assert rule_stats.errors_by_status[403] == report.total_requests


def test_static_token_mode_against_hardened_app_reports_grant_replayed(test_app):
    """Test that static-token mode against a hardened app reports grant_replayed errors after the first request."""
    app, priv = test_app

    sql = "SELECT id FROM users"
    tok = _make_token(priv, sql, "jti-static-replay-1")

    cases = [
        {"name": "static_case", "sql": sql, "token": tok},
    ]

    report = asyncio.run(
        run_load(
            app=app,
            cases=cases,
            concurrency=1,
            duration=0.2,
        )
    )

    assert report.total_requests > 1
    # First request succeeded (200), subsequent requests failed with grant_replayed
    assert report.total_errors == report.total_requests - 1
    assert report.grant_replayed_errors == report.total_errors

    # The grant_replayed errors are counted under their own key, not under 401
    assert "unknown" in report.by_rule
    stats = report.by_rule["unknown"]
    assert "grant_replayed" in stats.errors_by_status
    assert stats.errors_by_status["grant_replayed"] == report.total_errors
    assert 401 not in stats.errors_by_status

    summary = report.format_summary()
    assert "grant_replayed" in summary


def test_load_cli_refuses_group_world_readable_signing_key(tmp_path):
    """Test that CLI refuses signing key file if it is group- or world-readable."""
    priv = ec.generate_private_key(ec.SECP256R1())
    key_pem = priv.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    key_file = tmp_path / "key.pem"
    key_file.write_bytes(key_pem)
    key_file.chmod(0o644)  # group and world readable

    template_file = tmp_path / "template.json"
    template_file.write_text(json.dumps({"sub": "test"}), encoding="utf-8")

    cases_file = tmp_path / "cases.json"
    cases_file.write_text(json.dumps([{"sql": "SELECT 1"}]), encoding="utf-8")

    with pytest.raises(ServerConfigError, match="is group- or world-readable"):
        load_main([
            "--url", "http://localhost:8000",
            "--signing-key", str(key_file),
            "--kid", "k1",
            "--claims-template", str(template_file),
            "--cases", str(cases_file),
        ])


def test_load_cli_accepts_mode_0600_signing_key(tmp_path, monkeypatch, capsys):
    """Test that CLI accepts mode 0600 signing key and runs load."""
    priv = ec.generate_private_key(ec.SECP256R1())
    key_pem = priv.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    key_file = tmp_path / "key.pem"
    key_file.write_bytes(key_pem)
    key_file.chmod(0o600)

    template_file = tmp_path / "template.json"
    template_file.write_text(json.dumps({"sub": "test"}), encoding="utf-8")

    cases_file = tmp_path / "cases.json"
    cases_file.write_text(json.dumps([{"sql": "SELECT 1"}]), encoding="utf-8")

    async def fake_run_load(**kwargs):
        assert kwargs["mint_token"] is not None
        assert callable(kwargs["mint_token"])
        return LoadReport(total_requests=1, total_errors=0, by_rule={}, duration_seconds=0.1)

    import seeknal.serving.proving.load as load_module
    monkeypatch.setattr(load_module, "run_load", fake_run_load)

    load_main([
        "--url", "http://localhost:8000",
        "--signing-key", str(key_file),
        "--kid", "k1",
        "--claims-template", str(template_file),
        "--cases", str(cases_file),
    ])
    out = capsys.readouterr().out
    assert "Load Test Report" in out
