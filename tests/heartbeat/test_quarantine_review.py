"""Tests for the quarantine subsystem (Step 14)."""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from seeknal.cli.heartbeat import app as heartbeat_app
from seeknal.heartbeat.quarantine.callback_map import CallbackMap, _from_base36
from seeknal.heartbeat.quarantine.notifier import (
    build_notice,
    render_callback,
)
from seeknal.heartbeat.quarantine.review import (
    QuarantineSidecar,
    apply_review_decision,
    list_pending_reviews,
    parse_edit_input,
)


def _make_sidecar(tmp_path: Path, run_id: str = "abc123") -> Path:
    needs = tmp_path / "target/heartbeat/quarantine/needs_review" / run_id
    needs.mkdir(parents=True)
    # Data file
    data = needs / "novel.csv"
    data.write_text("a,b\n1,2\n")
    sidecar_path = needs / "novel.csv.review.yml"
    sc = QuarantineSidecar(
        run_id=run_id,
        file=str(data),
        target_table="bronze.maybe",
        confidence=0.55,
        concerns=["weak match"],
        column_mapping={"a": "x", "b": "y"},
        status="pending_review",
    )
    sc.save(sidecar_path)
    return sidecar_path


def test_callback_map_token_allocation(tmp_path: Path):
    cmap = CallbackMap(tmp_path / "_callback_map.json")
    t1 = cmap.allocate("run-1")
    t2 = cmap.allocate("run-2")
    assert t1 != t2
    assert cmap.lookup(t1) == "run-1"
    assert cmap.lookup(t2) == "run-2"


def test_callback_map_persists_and_resumes(tmp_path: Path):
    path = tmp_path / "_callback_map.json"
    a = CallbackMap(path)
    t1 = a.allocate("r1")
    b = CallbackMap(path)
    assert b.lookup(t1) == "r1"
    # Counter should resume from existing max — no collision.
    t2 = b.allocate("r2")
    assert _from_base36(t2) > _from_base36(t1)


def test_callback_map_remove(tmp_path: Path):
    cmap = CallbackMap(tmp_path / "_callback_map.json")
    t = cmap.allocate("r1")
    cmap.remove(t)
    assert cmap.lookup(t) is None


def test_callback_payload_fits_in_64_bytes():
    payload = render_callback("confirm", "zzzz", 99)
    assert len(payload.encode()) < 64


def test_apply_review_confirm(tmp_path: Path):
    sidecar = _make_sidecar(tmp_path)
    result = apply_review_decision(sidecar, decision="confirm", resolved_by="bob")
    assert result["ok"]
    sc = QuarantineSidecar.load(sidecar)
    assert sc.status == "confirmed"
    assert sc.resolved_by == "bob"


def test_apply_review_reject_moves_file(tmp_path: Path):
    sidecar = _make_sidecar(tmp_path)
    data = sidecar.parent / "novel.csv"
    assert data.exists()
    rejected_dir = tmp_path / "target/heartbeat/quarantine/rejected/abc123"
    result = apply_review_decision(
        sidecar, decision="reject", rejected_dir=rejected_dir
    )
    assert result["ok"]
    sc = QuarantineSidecar.load(sidecar)
    assert sc.status == "rejected"
    assert not data.exists()
    assert (rejected_dir / "novel.csv").exists()


def test_apply_review_edit_mapping(tmp_path: Path):
    sidecar = _make_sidecar(tmp_path)
    edit = "target=bronze.confirmed\nmode=upsert\nkey=a\na=order_id\n"
    result = apply_review_decision(
        sidecar, decision="edit", edit_text=edit, resolved_by="cli"
    )
    assert result["ok"], result
    sc = QuarantineSidecar.load(sidecar)
    assert sc.status == "edited"
    assert sc.target_table == "bronze.confirmed"
    assert sc.write_mode == "upsert"
    assert sc.business_key == ["a"]
    assert sc.column_mapping.get("a") == "order_id"


def test_parse_edit_rejects_malformed_line():
    column, meta, errors = parse_edit_input("not valid line\ntarget=ok\n")
    assert errors  # at least one error
    assert meta.get("target_table") == "ok"


def test_parse_edit_unknown_mode():
    column, meta, errors = parse_edit_input("mode=weird")
    assert any("unknown mode" in e for e in errors)


def test_parse_edit_cancel():
    column, meta, errors = parse_edit_input("cancel\n")
    assert meta.get("cancelled") is True


def test_apply_review_idempotent(tmp_path: Path):
    sidecar = _make_sidecar(tmp_path)
    apply_review_decision(sidecar, decision="confirm")
    result2 = apply_review_decision(sidecar, decision="confirm")
    assert not result2["ok"]
    assert "already" in result2["error"]


def test_list_pending_reviews(tmp_path: Path):
    sidecar = _make_sidecar(tmp_path, run_id="abc123")
    pending = list_pending_reviews(tmp_path)
    assert sidecar in pending
    apply_review_decision(sidecar, decision="confirm")
    pending2 = list_pending_reviews(tmp_path)
    assert sidecar not in pending2


def test_build_notice(tmp_path: Path):
    sidecar = _make_sidecar(tmp_path)
    notice = build_notice(
        sidecar,
        preview_text="row1,row2",
        confirm_token="0001",
        reject_token="0001",
        edit_token="0001",
    )
    assert "needs review" in notice.summary()
    assert "55%" in notice.summary()  # confidence
    assert notice.callback_tokens["confirm"] == "0001"


def test_cli_review_lists_pending(tmp_path: Path, monkeypatch):
    sidecar = _make_sidecar(tmp_path, run_id="run1")
    monkeypatch.chdir(tmp_path)
    runner = CliRunner()
    result = runner.invoke(heartbeat_app, ["review", "run1"])
    assert result.exit_code == 0
    assert "Pending review" in result.output
    assert "novel.csv.review.yml" in result.output


def test_cli_review_confirm(tmp_path: Path, monkeypatch):
    sidecar = _make_sidecar(tmp_path, run_id="run1")
    monkeypatch.chdir(tmp_path)
    runner = CliRunner()
    result = runner.invoke(heartbeat_app, ["review", "run1", "--confirm"])
    assert result.exit_code == 0
    sc = QuarantineSidecar.load(sidecar)
    assert sc.status == "confirmed"


def test_cli_review_edit_invalid_input(tmp_path: Path, monkeypatch):
    _make_sidecar(tmp_path, run_id="run1")
    monkeypatch.chdir(tmp_path)
    runner = CliRunner()
    result = runner.invoke(
        heartbeat_app, ["review", "run1", "--edit", "garbage line here"]
    )
    assert result.exit_code != 0
