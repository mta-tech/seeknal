"""Tests for the load testing harness (item 5)."""

from __future__ import annotations

import asyncio
import hashlib
import json
import tempfile
import time
from pathlib import Path

import duckdb
import pytest
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
    load_tokens,
    run_load,
)


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
    adapter = DuckDBAdapter(lambda: con)
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

    tok1 = _make_token(priv, sql_small_scan, "jti-load-1")
    tok2 = _make_token(priv, sql_analytics, "jti-load-2")

    cases = [
        {"name": "case_small", "sql": sql_small_scan, "token": tok1},
        {"name": "case_analytics", "sql": sql_analytics, "token": tok2},
    ]

    report = asyncio.run(
        run_load(
            app=app,
            cases=cases,
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
    """Test loading cases and tokens from external files and passing tokens mapping."""
    app, priv = test_app

    sql = "SELECT id FROM users"
    tok = _make_token(priv, sql, "jti-file-1")

    tokens_file = tmp_path / "tokens.json"
    tokens_file.write_text(json.dumps({"test_case": tok}), encoding="utf-8")

    cases_file = tmp_path / "cases.json"
    cases_file.write_text(
        json.dumps([{"name": "test_case", "sql": sql}]), encoding="utf-8"
    )

    tokens = load_tokens(str(tokens_file))
    cases = load_cases(str(cases_file))

    assert "test_case" in tokens
    assert len(cases) == 1

    report = asyncio.run(
        run_load(
            app=app,
            cases=cases,
            tokens=tokens,
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
