"""Tests for InboxState change detection."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from seeknal.heartbeat.inbox_state import (
    InboxFileRecord,
    InboxState,
    _compute_sha256,
)


def _make_inbox(tmp_path: Path, name: str = "inbox") -> Path:
    folder = tmp_path / name
    folder.mkdir()
    return folder


def test_insecure_state_path_rejected():
    with pytest.raises(ValueError, match="Insecure InboxState path"):
        InboxState(Path("/tmp/inbox_state.json"))


def test_scan_returns_new_file(tmp_path: Path):
    inbox = _make_inbox(tmp_path)
    (inbox / "sales.csv").write_text("a,b\n1,2\n")
    state = InboxState(tmp_path / "target/heartbeat/inbox_state.json")
    changed = state.scan([inbox])
    assert len(changed) == 1
    assert changed[0].name == "sales.csv"


def test_scan_skips_unchanged(tmp_path: Path):
    inbox = _make_inbox(tmp_path)
    f = inbox / "sales.csv"
    f.write_text("a,b\n1,2\n")
    state = InboxState(tmp_path / "target/heartbeat/inbox_state.json")
    changed = state.scan([inbox])
    state.mark_ingested(changed[0], target_table="ingested.sales")
    state.save()

    state2 = InboxState.load(state.state_path)
    assert state2.scan([inbox]) == []


def test_scan_detects_modified_file(tmp_path: Path):
    inbox = _make_inbox(tmp_path)
    f = inbox / "sales.csv"
    f.write_text("a,b\n1,2\n")
    state = InboxState(tmp_path / "target/heartbeat/inbox_state.json")
    changed = state.scan([inbox])
    state.mark_ingested(changed[0], target_table="ingested.sales")
    state.save()

    f.write_text("a,b\n1,2\n3,4\n")
    state2 = InboxState.load(state.state_path)
    new = state2.scan([inbox])
    assert len(new) == 1


def test_scan_ignores_hidden_files(tmp_path: Path):
    inbox = _make_inbox(tmp_path)
    (inbox / ".DS_Store").write_text("x")
    state = InboxState(tmp_path / "target/heartbeat/inbox_state.json")
    assert state.scan([inbox]) == []


def test_scan_missing_folder_no_error(tmp_path: Path):
    state = InboxState(tmp_path / "target/heartbeat/inbox_state.json")
    assert state.scan([tmp_path / "nope"]) == []


def test_atomic_save_round_trip(tmp_path: Path):
    inbox = _make_inbox(tmp_path)
    (inbox / "a.csv").write_text("a\n1\n")
    state = InboxState(tmp_path / "target/heartbeat/inbox_state.json")
    paths = state.scan([inbox])
    state.mark_ingested(paths[0], target_table="ingested.a")
    state.save()

    raw = json.loads(state.state_path.read_text())
    assert len(raw) == 1
    first = next(iter(raw.values()))
    assert first["target_table"] == "ingested.a"
    assert first["last_status"] == "ok"


def test_record_invalid_status_rejected():
    with pytest.raises(ValueError, match="Invalid last_status"):
        InboxFileRecord(
            path="/x",
            checksum_sha256="0",
            size_bytes=0,
            mtime=0.0,
            ingested_at="2026-05-15T00:00:00Z",
            target_table="t",
            last_status="weird",
        )


def test_mark_quarantined_persists(tmp_path: Path):
    inbox = _make_inbox(tmp_path)
    (inbox / "bad.csv").write_text("malformed")
    state = InboxState(tmp_path / "target/heartbeat/inbox_state.json")
    paths = state.scan([inbox])
    state.mark_quarantined(paths[0], reason="bad")
    state.save()
    state2 = InboxState.load(state.state_path)
    rec = state2.get(paths[0])
    assert rec is not None
    assert rec.last_status == "quarantined"


def test_load_corrupt_file_starts_empty(tmp_path: Path):
    state_path = tmp_path / "target/heartbeat/inbox_state.json"
    state_path.parent.mkdir(parents=True)
    state_path.write_text("{not json")
    state = InboxState.load(state_path)
    assert state.all_records() == []


def test_compute_sha256_deterministic(tmp_path: Path):
    f = tmp_path / "data.bin"
    f.write_bytes(b"hello world")
    h1, s1 = _compute_sha256(f)
    h2, s2 = _compute_sha256(f)
    assert h1 == h2
    assert s1 == s2 == 11
