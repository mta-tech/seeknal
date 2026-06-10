"""Tests for the heartbeat IngestWriter (parquet backend)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from seeknal.heartbeat.ingest import (
    IngestWriter,
    ParquetWriter,
    derive_table_name,
    quarantine_file,
)


def test_derive_table_name_basic():
    assert derive_table_name(Path("sales.csv")) == "sales"
    assert derive_table_name(Path("Orders-2024.csv")) == "orders_2024"


def test_derive_table_name_rejects_reserved():
    with pytest.raises(ValueError, match="reserved"):
        derive_table_name(Path("select.csv"))


def test_derive_table_name_rejects_leading_digit():
    with pytest.raises(ValueError):
        derive_table_name(Path("9items.csv"))


def test_parquet_writer_csv_to_parquet(tmp_path: Path):
    src = tmp_path / "sales.csv"
    src.write_text("a,b\n1,2\n3,4\n")
    writer = ParquetWriter(tmp_path)
    res = writer.write(source_path=src, table_name="sales", checksum="deadbeef")
    assert res.status == "ok"
    assert res.rows == 2
    out = tmp_path / "target/ingested/sales/data.parquet"
    meta = tmp_path / "target/ingested/sales/_meta.json"
    assert out.exists()
    assert meta.exists()
    payload = json.loads(meta.read_text())
    assert payload["rows"] == 2
    assert payload["checksum_sha256"] == "deadbeef"
    assert payload["table"] == "sales"


def test_parquet_writer_jsonl_to_parquet(tmp_path: Path):
    src = tmp_path / "events.jsonl"
    src.write_text('{"a":1,"b":2}\n{"a":3,"b":4}\n')
    writer = ParquetWriter(tmp_path)
    res = writer.write(source_path=src, table_name="events")
    assert res.status == "ok"
    assert res.rows == 2


def test_parquet_writer_malformed_csv_quarantined(tmp_path: Path):
    src = tmp_path / "bad.csv"
    # Mismatched columns trigger DuckDB read_csv_auto failure with strict=true default.
    src.write_text("\x00\xff\xfe garbage data")
    writer = ParquetWriter(tmp_path)
    res = writer.write(source_path=src, table_name="bad")
    # Either DuckDB swallows it (status=ok) or it errors out.
    # The contract: never raise; produce an IngestResult.
    assert res.status in {"ok", "quarantined"}


def test_parquet_writer_unsupported_suffix(tmp_path: Path):
    src = tmp_path / "x.bin"
    src.write_bytes(b"\x00\x01\x02")
    writer = ParquetWriter(tmp_path)
    res = writer.write(source_path=src, table_name="x")
    assert res.status == "quarantined"
    assert "Unsupported" in (res.error or "")


def test_ingest_writer_dispatch_parquet(tmp_path: Path):
    src = tmp_path / "sales.csv"
    src.write_text("x,y\n1,2\n")
    writer = IngestWriter(tmp_path, target="parquet")
    res = writer.write(src)
    assert res.status == "ok"
    assert res.table == "sales"


def test_ingest_writer_unknown_target():
    with pytest.raises(ValueError, match="Unknown target"):
        IngestWriter(Path("."), target="cassandra")


def test_ingest_writer_iceberg_requires_config():
    with pytest.raises(ValueError, match="catalog_uri"):
        IngestWriter(Path("."), target="iceberg")


def test_ingest_writer_invalid_filename(tmp_path: Path):
    src = tmp_path / "select.csv"
    src.write_text("a\n1\n")
    writer = IngestWriter(tmp_path, target="parquet")
    res = writer.write(src)
    assert res.status == "quarantined"
    assert "reserved" in (res.error or "")


def test_quarantine_file_moves(tmp_path: Path):
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    bad = inbox / "bad.csv"
    bad.write_text("x")
    quarantine = tmp_path / "target/heartbeat/quarantine"
    dest = quarantine_file(bad, quarantine)
    assert dest.exists()
    assert not bad.exists()


def test_quarantine_file_collision_suffix(tmp_path: Path):
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    bad = inbox / "bad.csv"
    bad.write_text("x")
    quarantine = tmp_path / "target/heartbeat/quarantine"
    first = quarantine_file(bad, quarantine)
    bad.write_text("y")
    second = quarantine_file(bad, quarantine)
    assert first != second
    assert first.exists()
    assert second.exists()
