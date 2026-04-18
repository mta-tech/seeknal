"""Tests for write_ingested_table tool (self-defending writes + provenance)."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import duckdb
import pytest

from seeknal.ask.agents.tools._context import ToolContext, set_tool_context
from seeknal.ask.agents.tools.write_ingested_table import write_ingested_table


class _REPLStub:
    def __init__(self) -> None:
        self.conn = duckdb.connect(":memory:")


@pytest.fixture
def ctx(tmp_path: Path) -> ToolContext:
    context = ToolContext(
        repl=_REPLStub(),
        artifact_discovery=MagicMock(),
        project_path=tmp_path,
    )
    set_tool_context(context)
    return context


def _csv(path: Path, rows: list[tuple]) -> None:
    lines = ["invoice_id,date,amount"]
    for invoice_id, date, amount in rows:
        lines.append(f"{invoice_id},{date},{amount}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _count_rows(parquet: Path) -> int:
    con = duckdb.connect(":memory:")
    try:
        (n,) = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{parquet}')"
        ).fetchone()
        return int(n)
    finally:
        con.close()


def test_create_mode_writes_parquet_and_registers_view(tmp_path, ctx):
    src = tmp_path / "jan.csv"
    _csv(src, [(1, "2025-01-01", 100), (2, "2025-01-02", 200)])

    result = write_ingested_table(str(src), "acme_sales", "invoice_id", mode="create")
    parquet = tmp_path / "target/ask_ingest/acme_sales.parquet"
    prov = tmp_path / "target/ask_ingest/acme_sales_provenance.json"

    assert parquet.exists()
    assert prov.exists()
    assert "Rows inserted: 2" in result

    payload = json.loads(prov.read_text())
    for key in (
        "source_file_sha256", "ingested_at", "rows_before", "rows_after",
        "rows_inserted", "rows_deduped", "drift_decisions",
    ):
        assert key in payload
    assert payload["rows_before"] == 0
    assert payload["rows_after"] == 2

    # View registered on the REPL conn
    names = {
        r[0]
        for r in ctx.repl.conn.execute(
            "SELECT table_name FROM information_schema.tables"
        ).fetchall()
    }
    assert "ingest_acme_sales" in names


def test_create_mode_rejects_duplicate_table(tmp_path, ctx):
    src = tmp_path / "jan.csv"
    _csv(src, [(1, "2025-01-01", 100)])
    write_ingested_table(str(src), "acme_sales", "invoice_id", mode="create")

    out = write_ingested_table(str(src), "acme_sales", "invoice_id", mode="create")
    assert out.startswith("Error:")
    assert "already exists" in out


def test_silent_insert_blocked_in_append_mode(tmp_path, ctx):
    # Create
    src = tmp_path / "jan.csv"
    _csv(src, [(1, "2025-01-01", 100), (2, "2025-01-02", 200)])
    write_ingested_table(str(src), "acme_sales", "invoice_id", mode="create")
    parquet = tmp_path / "target/ask_ingest/acme_sales.parquet"
    mtime_before = parquet.stat().st_mtime_ns
    size_before = parquet.stat().st_size

    # Append attempt without confirmation: must return drift report, not write
    feb = tmp_path / "feb.csv"
    _csv(feb, [(2, "2025-01-02", 200), (3, "2025-02-01", 300)])
    out = write_ingested_table(
        str(feb), "acme_sales", "invoice_id", mode="append", user_confirmed=False,
    )

    assert "No data was written" in out
    assert "Duplicate business-key rows" in out
    # Parquet untouched
    assert parquet.stat().st_mtime_ns == mtime_before
    assert parquet.stat().st_size == size_before
    assert _count_rows(parquet) == 2


def test_append_dedup_skip_keeps_existing(tmp_path, ctx):
    src = tmp_path / "jan.csv"
    _csv(src, [(1, "2025-01-01", 100), (2, "2025-01-02", 200)])
    write_ingested_table(str(src), "acme_sales", "invoice_id", mode="create")

    feb = tmp_path / "feb.csv"
    # Overlap on invoice_id=2 (different amount — existing 200 should stay)
    _csv(feb, [(2, "2025-01-02", 999), (3, "2025-02-01", 300)])
    out = write_ingested_table(
        str(feb), "acme_sales", "invoice_id",
        mode="append", user_confirmed=True, dedup_strategy="skip",
    )
    assert "Appended to `ingest_acme_sales`" in out

    parquet = tmp_path / "target/ask_ingest/acme_sales.parquet"
    assert _count_rows(parquet) == 3  # 1 + 2 + 3

    con = duckdb.connect(":memory:")
    try:
        row = con.execute(
            f"SELECT amount FROM read_parquet('{parquet}') WHERE invoice_id = 2"
        ).fetchone()
    finally:
        con.close()
    # existing wins
    assert row[0] == 200


def test_append_dedup_replace_keeps_new(tmp_path, ctx):
    src = tmp_path / "jan.csv"
    _csv(src, [(1, "2025-01-01", 100), (2, "2025-01-02", 200)])
    write_ingested_table(str(src), "acme_sales", "invoice_id", mode="create")

    feb = tmp_path / "feb.csv"
    _csv(feb, [(2, "2025-01-02", 999), (3, "2025-02-01", 300)])
    write_ingested_table(
        str(feb), "acme_sales", "invoice_id",
        mode="append", user_confirmed=True, dedup_strategy="replace",
    )

    parquet = tmp_path / "target/ask_ingest/acme_sales.parquet"
    assert _count_rows(parquet) == 3

    con = duckdb.connect(":memory:")
    try:
        row = con.execute(
            f"SELECT amount FROM read_parquet('{parquet}') WHERE invoice_id = 2"
        ).fetchone()
    finally:
        con.close()
    # new wins
    assert row[0] == 999


def test_invalid_table_name_rejected(tmp_path, ctx):
    src = tmp_path / "j.csv"
    _csv(src, [(1, "2025-01-01", 10)])
    out = write_ingested_table(str(src), "DROP;--", "invoice_id", mode="create")
    assert out.startswith("Error:")
    assert "invalid table_name" in out


def test_invalid_business_key_rejected(tmp_path, ctx):
    src = tmp_path / "j.csv"
    _csv(src, [(1, "2025-01-01", 10)])
    out = write_ingested_table(str(src), "acme_sales", "id;--", mode="create")
    assert out.startswith("Error:")


def test_append_without_existing_table_errors(tmp_path, ctx):
    feb = tmp_path / "feb.csv"
    _csv(feb, [(3, "2025-02-01", 300)])
    out = write_ingested_table(
        str(feb), "acme_sales", "invoice_id", mode="append", user_confirmed=True,
    )
    assert out.startswith("Error:")
    assert "no existing table" in out


def test_provenance_written_on_create_and_append(tmp_path, ctx):
    src = tmp_path / "jan.csv"
    _csv(src, [(1, "2025-01-01", 100)])
    write_ingested_table(str(src), "acme_sales", "invoice_id", mode="create")

    feb = tmp_path / "feb.csv"
    _csv(feb, [(2, "2025-02-01", 200)])
    write_ingested_table(
        str(feb), "acme_sales", "invoice_id",
        mode="append", user_confirmed=True, dedup_strategy="skip",
    )

    prov = json.loads(
        (tmp_path / "target/ask_ingest/acme_sales_provenance.json").read_text()
    )
    assert prov["rows_before"] == 1
    assert prov["rows_after"] == 2
    assert prov["rows_inserted"] == 1
    assert prov["rows_deduped"] == 0
    assert prov["drift_decisions"]


def test_missing_business_key_column_on_create(tmp_path, ctx):
    src = tmp_path / "j.csv"
    _csv(src, [(1, "2025-01-01", 10)])
    out = write_ingested_table(str(src), "acme_sales", "nonexistent", mode="create")
    assert out.startswith("Error:")
    assert "not present in source" in out


def test_xlsx_source_auto_converted(tmp_path, ctx):
    """write_ingested_table should accept an xlsx path directly (no prior read_tabular)."""
    pytest.importorskip("openpyxl")
    import pandas as pd

    xlsx = tmp_path / "menu.xlsx"
    df = pd.DataFrame(
        {
            "invoice_id": [1, 2, 3],
            "date": ["2025-01-01", "2025-01-02", "2025-01-03"],
            "amount": [100, 150, 200],
        }
    )
    df.to_excel(xlsx, index=False, engine="openpyxl")

    result = write_ingested_table(
        str(xlsx), "menu", "invoice_id", mode="create"
    )
    assert "Created table `ingest_menu`" in result
    parquet = tmp_path / "target/ask_ingest/menu.parquet"
    assert parquet.exists()
    assert _count_rows(parquet) == 3


def test_resolves_staging_path_from_context(tmp_path, ctx):
    src = tmp_path / "j.csv"
    _csv(src, [(1, "2025-01-01", 10)])
    ctx.last_read_staging_path = str(src)

    out = write_ingested_table("", "acme_sales", "invoice_id", mode="create")
    assert "Created table" in out
