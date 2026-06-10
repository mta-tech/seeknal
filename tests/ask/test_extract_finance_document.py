"""Tests for finance document extraction with mocked model output."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from seeknal.ask.agents.tools._context import ToolContext, set_tool_context
from seeknal.ask.agents.tools.extract_finance_document import (
    _parse_json_output,
    extract_finance_document,
)


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


def _fake_pdf(path: Path) -> Path:
    path.write_bytes(b"%PDF-1.4\n1 0 obj\n<<>>\nendobj\n%%EOF\n")
    return path


def test_extract_supplier_invoice_pdf(tmp_path, ctx):
    pdf = _fake_pdf(tmp_path / "supplier-invoice.pdf")
    payload = {
        "document_kind": "supplier_invoice",
        "direction": "ap",
        "document_number": "INV-1001",
        "counterparty_name": "Kilang Textile Sdn Bhd",
        "issue_date": "2026-05-01",
        "due_date": "2026-05-31",
        "currency": "MYR",
        "subtotal_amount": 1000.0,
        "tax_amount": 80.0,
        "sst_amount": 80.0,
        "total_amount": 1080.0,
        "line_items": [],
        "readiness_flags": [],
        "confidence": 0.91,
        "raw_text": "Invoice INV-1001",
    }

    with patch(
        "seeknal.ask.agents.tools.extract_finance_document._call_model",
        return_value=payload,
    ):
        out = extract_finance_document(str(pdf), hint="supplier invoice")

    assert "Extracted finance document draft" in out
    assert "INV-1001" in out
    assert "No required review fields" in out
    assert ctx.last_read_staging_path == str(pdf)


def test_parse_markdown_json_fence():
    raw = "```json\n" + json.dumps({"document_kind": "receipt", "total_amount": 12}) + "\n```"
    parsed = _parse_json_output(raw)
    assert parsed["document_kind"] == "receipt"
    assert parsed["currency"] == "MYR"
    assert parsed["line_items"] == []


def test_missing_review_fields_are_reported(tmp_path, ctx):
    pdf = _fake_pdf(tmp_path / "invoice.pdf")
    payload = {
        "document_kind": "supplier_invoice",
        "counterparty_name": None,
        "total_amount": None,
    }

    with patch(
        "seeknal.ask.agents.tools.extract_finance_document._call_model",
        return_value=payload,
    ):
        out = extract_finance_document(str(pdf))

    assert "Fields needing review" in out
    assert "counterparty_name" in out
    assert "total_amount" in out


def test_unsupported_suffix_rejected(tmp_path, ctx):
    txt = tmp_path / "note.txt"
    txt.write_text("not a finance document", encoding="utf-8")
    out = extract_finance_document(str(txt))
    assert out.startswith("Error:")
    assert "unsupported" in out.lower()


def test_missing_file_rejected(tmp_path, ctx):
    out = extract_finance_document(str(tmp_path / "missing.pdf"))
    assert out.startswith("Error:")
    assert "not found" in out.lower()
