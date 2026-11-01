"""Tests for the differential execution harness (item 4)."""

from __future__ import annotations

import datetime
import os
from decimal import Decimal

import duckdb
import pytest

from seeknal.serving.proving.differential import (
    DiffCase,
    default_cases,
    default_normalise,
    run_differential,
    setup_synthetic_claims_table,
)


def test_default_normalise():
    """Verify canonical value normalization."""
    # Decimal to fixed scale string
    assert default_normalise(Decimal("123.456")) == "123.46"
    assert default_normalise(Decimal("100.5")) == "100.50"
    assert default_normalise(Decimal("0")) == "0.00"

    # Datetimes to naive UTC ISO
    dt_naive = datetime.datetime(2026, 5, 1, 14, 30, 0)
    assert default_normalise(dt_naive) == "2026-05-01T14:30:00"

    dt_aware = datetime.datetime(2026, 5, 1, 14, 30, 0, tzinfo=datetime.timezone(datetime.timedelta(hours=7)))
    # 14:30 in UTC+7 is 07:30 in UTC
    assert default_normalise(dt_aware) == "2026-05-01T07:30:00"

    # Dates
    d = datetime.date(2026, 11, 1)
    assert default_normalise(d) == "2026-11-01"

    # Floats rounded to 9 significant digits
    assert default_normalise(12345.678912345) == 12345.6789
    assert default_normalise(0.000123456789123) == 0.000123456789

    # Bytes to hex
    assert default_normalise(b"\xde\xad\xbe\xef") == "deadbeef"

    # None and primitives
    assert default_normalise(None) is None
    assert default_normalise("text") == "text"
    assert default_normalise(42) == 42
    assert default_normalise(True) is True


def test_differential_harness_duckdb_matching():
    """Run harness with two DuckDB connections to prove identical engines match."""
    con1 = duckdb.connect()
    con2 = duckdb.connect()
    setup_synthetic_claims_table(con1)
    setup_synthetic_claims_table(con2)

    engines = {
        "duck1": lambda: con1,
        "duck2": lambda: con2,
    }

    results = run_differential(default_cases(), engines)

    assert len(results) == len(default_cases())
    for res in results:
        assert res.matched is True, f"Case '{res.name}' failed to match: {res.first_difference}"
        assert res.first_difference is None
        assert res.row_counts["duck1"] == res.row_counts["duck2"]
        assert res.row_counts["duck1"] > 0


def test_differential_harness_duckdb_reporting_mismatch():
    """Harness detects and reports first difference when second engine returns different rows."""
    con1 = duckdb.connect()
    con2 = duckdb.connect()
    setup_synthetic_claims_table(con1)
    setup_synthetic_claims_table(con2)

    # Deliberately modify data in engine 2
    con2.execute("UPDATE claims SET amount = 999.00 WHERE claim_id = 1")

    engines = {
        "duck1": lambda: con1,
        "duck2": lambda: con2,
    }

    cases = [
        DiffCase(
            name="amount_check",
            sql="SELECT claim_id, amount FROM claims WHERE claim_id = 1",
            ordered=True,
        )
    ]

    results = run_differential(cases, engines)
    assert len(results) == 1
    res = results[0]
    assert res.matched is False
    assert res.first_difference is not None
    assert "differ at row 0" in res.first_difference
    assert "100.50" in res.first_difference
    assert "999.00" in res.first_difference


def test_differential_harness_row_count_mismatch():
    """Harness detects and reports row count mismatch."""
    con1 = duckdb.connect()
    con2 = duckdb.connect()
    setup_synthetic_claims_table(con1)
    setup_synthetic_claims_table(con2)

    # Delete rows from engine 2
    con2.execute("DELETE FROM claims WHERE claim_id > 2")

    engines = {
        "duck1": lambda: con1,
        "duck2": lambda: con2,
    }

    cases = [
        DiffCase(
            name="count_all",
            sql="SELECT claim_id FROM claims",
            ordered=False,
        )
    ]

    results = run_differential(cases, engines)
    assert len(results) == 1
    res = results[0]
    assert res.matched is False
    assert "Row count mismatch" in str(res.first_difference)
    assert res.row_counts["duck1"] == 7
    assert res.row_counts["duck2"] == 3


@pytest.mark.skipif(
    not os.environ.get("SEEKNAL_DIFF_STARROCKS_URL") or not os.environ.get("SEEKNAL_DIFF_POSTGRES_URL"),
    reason="SEEKNAL_DIFF_STARROCKS_URL and SEEKNAL_DIFF_POSTGRES_URL are not set",
)
def test_integration_starrocks_postgres_differential():
    """Integration test against live StarRocks and Postgres (skipped unless env vars set)."""
    import urllib.parse
    import psycopg2
    import pymysql

    sr_url = os.environ["SEEKNAL_DIFF_STARROCKS_URL"]
    pg_url = os.environ["SEEKNAL_DIFF_POSTGRES_URL"]

    def connect_pg():
        return psycopg2.connect(pg_url)

    def connect_sr():
        parsed = urllib.parse.urlparse(sr_url)
        return pymysql.connect(
            host=parsed.hostname,
            port=parsed.port or 9030,
            user=parsed.username or "root",
            password=parsed.password or "",
            database=parsed.path.lstrip("/") or "test",
        )

    # Seed both engines
    setup_synthetic_claims_table(connect_pg())
    setup_synthetic_claims_table(connect_sr())

    engines = {
        "postgres": connect_pg,
        "starrocks": connect_sr,
    }

    results = run_differential(default_cases(), engines)
    for res in results:
        assert res.matched is True, f"Integration mismatch on '{res.name}': {res.first_difference}"
