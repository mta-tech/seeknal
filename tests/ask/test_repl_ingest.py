"""Tests for REPL auto-registration of target/ask_ingest/*.parquet."""

from __future__ import annotations

import warnings
from pathlib import Path

import duckdb
import pytest

from seeknal.cli.repl import REPL


def _write_parquet(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(":memory:")
    try:
        con.execute(
            f"COPY (SELECT 1 AS id, 'a' AS name UNION ALL SELECT 2, 'b') "
            f"TO '{path}' (FORMAT PARQUET)"
        )
    finally:
        con.close()


def _views(repl: REPL) -> set[str]:
    rows = repl.conn.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_catalog='memory' AND table_schema='main' AND table_type='VIEW'"
    ).fetchall()
    return {r[0] for r in rows}


def test_auto_register_ask_ingest(tmp_path):
    _write_parquet(tmp_path / "target" / "ask_ingest" / "acme_sales.parquet")

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        repl = REPL(project_path=tmp_path, skip_history=True)

    assert "ingest_acme_sales" in _views(repl)
    (n,) = repl.conn.execute(
        'SELECT COUNT(*) FROM "ingest_acme_sales"'
    ).fetchone()
    assert n == 2


def test_staging_parquets_not_registered(tmp_path):
    _write_parquet(tmp_path / "target" / "ask_ingest" / "real.parquet")
    _write_parquet(
        tmp_path / "target" / "ask_ingest" / "_staging" / "abc123" / "temp.parquet"
    )

    repl = REPL(project_path=tmp_path, skip_history=True)

    views = _views(repl)
    assert "ingest_real" in views
    # Staging parquets must never be auto-registered
    assert "ingest_temp" not in views


def test_empty_ask_ingest_graceful(tmp_path):
    # No parquet present; REPL startup must not raise.
    (tmp_path / "target" / "ask_ingest").mkdir(parents=True)
    REPL(project_path=tmp_path, skip_history=True)


def test_no_ask_ingest_dir_graceful(tmp_path):
    # Directory doesn't exist; REPL startup must not raise.
    REPL(project_path=tmp_path, skip_history=True)
