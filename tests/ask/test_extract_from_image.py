"""Tests for extract_from_image (Gemini vision OCR) with a mocked model."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from seeknal.ask.agents.tools._context import ToolContext, set_tool_context
from seeknal.ask.agents.tools.extract_from_image import extract_from_image


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


def _fake_image(path: Path, size: int = 2048) -> Path:
    # Minimal valid PNG header + padding — we only care about file size and suffix.
    header = bytes.fromhex(
        "89504e470d0a1a0a0000000d49484452000000010000000108060000001f15c489"
    )
    path.write_bytes(header + b"\x00" * (size - len(header)))
    return path


def _patch_call_gemini(payload):
    """Context manager that patches _call_gemini to return a fixed payload."""
    return patch(
        "seeknal.ask.agents.tools.extract_from_image._call_gemini",
        return_value=payload,
    )


def test_extract_transfer_happy_path(tmp_path, ctx):
    img = _fake_image(tmp_path / "proof.jpg")
    payload = {
        "kind": "transfer",
        "amount": 1500000,
        "currency": "IDR",
        "sender_name": "FITRA K",
        "recipient_name": "BUDI S",
        "bank": "BCA",
        "transaction_date": "2026-04-17",
        "reference_number": "TRX-77",
        "note": "rent",
        "raw_text": "BCA mobile — transfer berhasil",
    }
    with _patch_call_gemini(payload):
        out = extract_from_image(str(img))

    assert "Extracted draft from image" in out
    assert "1500000" in out
    assert "BCA" in out
    # Source path stashed on ctx
    assert ctx.last_read_staging_path == str(img)


def test_extract_markdown_fence_stripped(tmp_path, ctx):
    # _call_gemini already parses JSON; this test exercises _parse_json_output
    # directly via the private helper to prove fence-stripping works.
    from seeknal.ask.agents.tools.extract_from_image import _parse_json_output

    fence = "```json\n" + json.dumps({"kind": "other", "amount": 123}) + "\n```"
    parsed = _parse_json_output(fence)
    assert parsed["amount"] == 123
    assert parsed["kind"] == "other"


def test_unsupported_suffix_rejected(tmp_path, ctx):
    bad = tmp_path / "bad.pdf"
    bad.write_bytes(b"%PDF")
    out = extract_from_image(str(bad))
    assert out.startswith("Error:")
    assert "unsupported" in out.lower()


def test_missing_file_rejected(tmp_path, ctx):
    out = extract_from_image(str(tmp_path / "nope.jpg"))
    assert out.startswith("Error:")
    assert "not found" in out.lower()


def test_oversized_image_rejected(tmp_path, ctx, monkeypatch):
    big = _fake_image(tmp_path / "big.jpg", size=1024)
    monkeypatch.setattr(
        "seeknal.ask.agents.tools.extract_from_image._MAX_IMAGE_BYTES", 10
    )
    out = extract_from_image(str(big))
    assert out.startswith("Error:")
    assert "exceeds" in out.lower()


def test_non_json_response_reports_error(tmp_path, ctx):
    img = _fake_image(tmp_path / "proof.jpg")
    with patch(
        "seeknal.ask.agents.tools.extract_from_image._call_gemini",
        side_effect=ValueError("Gemini returned non-JSON output: …"),
    ):
        out = extract_from_image(str(img))
    assert out.startswith("Error:")
    assert "non-json" in out.lower() or "could not parse" in out.lower()


def test_missing_required_fields_flagged(tmp_path, ctx):
    img = _fake_image(tmp_path / "proof.jpg")
    payload = {
        "kind": "transfer",
        "amount": None,
        "recipient_name": "BUDI",
        "transaction_date": "2026-04-17",
    }
    with _patch_call_gemini(payload):
        out = extract_from_image(str(img))
    assert "Required fields not yet resolved" in out
    assert "amount" in out.lower()
