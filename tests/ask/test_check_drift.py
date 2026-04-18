"""Tests for check_ingestion_drift tool."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import duckdb
import pytest

from seeknal.ask.agents.tools._context import ToolContext, set_tool_context
from seeknal.ask.agents.tools.check_ingestion_drift import check_ingestion_drift
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


def _csv(path: Path, header: str, rows: list[str]) -> None:
    path.write_text(header + "\n" + "\n".join(rows) + "\n", encoding="utf-8")


def _seed_existing(tmp_path: Path) -> None:
    src = tmp_path / "jan.csv"
    _csv(src, "invoice_id,date,amount",
         ["1,2025-01-01,100", "2,2025-01-02,200", "3,2025-01-03,300"])
    write_ingested_table(str(src), "acme_sales", "invoice_id", mode="create")


def test_no_existing_table_returns_friendly_message(tmp_path, ctx):
    src = tmp_path / "feb.csv"
    _csv(src, "invoice_id,date,amount", ["1,2025-02-01,10"])
    out = check_ingestion_drift(str(src), "acme_sales", "invoice_id")
    assert "No existing table" in out
    assert "first ingestion" in out


def test_duplicate_count(tmp_path, ctx):
    _seed_existing(tmp_path)
    feb = tmp_path / "feb.csv"
    _csv(feb, "invoice_id,date,amount",
         ["2,2025-01-02,200", "3,2025-01-03,300", "4,2025-02-01,400"])
    out = check_ingestion_drift(str(feb), "acme_sales", "invoice_id")
    assert "Duplicate business-key rows: **2" in out


def test_no_drift_message(tmp_path, ctx):
    _seed_existing(tmp_path)
    feb = tmp_path / "feb.csv"
    _csv(feb, "invoice_id,date,amount", ["4,2025-02-01,400", "5,2025-02-02,500"])
    out = check_ingestion_drift(str(feb), "acme_sales", "invoice_id")
    assert "Schema match" in out
    assert "Duplicate business-key rows: **0" in out


def test_schema_drift_missing_and_extra(tmp_path, ctx):
    _seed_existing(tmp_path)
    feb = tmp_path / "feb.csv"
    # Replaces `date` with `event_time`, adds `region`
    _csv(
        feb,
        "invoice_id,event_time,amount,region",
        ["4,2025-02-01T00:00:00,400,APAC"],
    )
    out = check_ingestion_drift(str(feb), "acme_sales", "invoice_id")
    assert "Missing from new file" in out
    assert "`date`" in out
    assert "New columns not in existing table" in out
    assert "`event_time`" in out or "`region`" in out


def test_invalid_table_name_rejected(ctx):
    out = check_ingestion_drift("/tmp/whatever.csv", "bad;name", "invoice_id")
    assert out.startswith("Error:")


def test_missing_source_rejected(tmp_path, ctx):
    _seed_existing(tmp_path)
    out = check_ingestion_drift(str(tmp_path / "nope.csv"), "acme_sales", "invoice_id")
    assert out.startswith("Error:")
