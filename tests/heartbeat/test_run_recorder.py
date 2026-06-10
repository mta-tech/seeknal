"""Tests for RunRecorder + HeartbeatRun schema."""

from __future__ import annotations

import json
import threading
from pathlib import Path

from seeknal.heartbeat.recorder import (
    DagResult,
    ExposureResult,
    HeartbeatRun,
    IngestResult,
    RunRecorder,
    make_run,
)


def _build(status="ok", ingested=None, dag=None, exposure=None, errors=None):
    return HeartbeatRun(
        run_id="abc",
        started_at="2026-05-15T10:00:00Z",
        finished_at="2026-05-15T10:00:01Z",
        duration_ms=1000,
        status=status,
        ingested=ingested or [],
        dag=dag or DagResult(),
        exposure=exposure or [],
        errors=errors or [],
    )


def test_compute_status_ok_no_data():
    assert (
        HeartbeatRun.compute_status([], DagResult(), [], []) == "ok"
    )


def test_compute_status_ok_one_ingest():
    ingest = [IngestResult(file="a", table="t", rows=10, status="ok")]
    assert HeartbeatRun.compute_status(ingest, DagResult(), [], []) == "ok"


def test_compute_status_partial_on_error():
    ingest = [IngestResult(file="a", table="t", status="ok")]
    errs = ["DAG failed"]
    assert HeartbeatRun.compute_status(ingest, DagResult(), [], errs) == "partial"


def test_compute_status_partial_on_quarantine():
    ingest = [IngestResult(file="a", table="t", status="quarantined")]
    assert HeartbeatRun.compute_status(ingest, DagResult(), [], []) == "partial"


def test_compute_status_failed_when_all_fail():
    ingest = [IngestResult(file="a", table="t", status="failed")]
    dag = DagResult(nodes_failed=2)
    expo = [ExposureResult(node="x", target="t", status="failed")]
    assert HeartbeatRun.compute_status(ingest, dag, expo, ["e"]) == "failed"


def test_recorder_appends_line(tmp_path: Path):
    rec = RunRecorder(tmp_path / "target/heartbeat")
    run = _build()
    rec.append(run)
    text = rec.jsonl_path.read_text()
    assert text.count("\n") == 1
    parsed = json.loads(text.strip())
    assert parsed["run_id"] == "abc"
    assert parsed["status"] == "ok"
    # Schema completeness — AC4
    for key in (
        "run_id",
        "started_at",
        "finished_at",
        "duration_ms",
        "status",
        "ingested",
        "dag",
        "exposure",
        "errors",
    ):
        assert key in parsed


def test_recorder_writes_sidecar(tmp_path: Path):
    rec = RunRecorder(tmp_path / "target/heartbeat")
    run = _build()
    rec.append(run)
    sidecar = rec.sidecar_dir / "abc.json"
    assert sidecar.exists()
    parsed = json.loads(sidecar.read_text())
    assert parsed["run_id"] == "abc"


def test_recorder_atomic_append_concurrent(tmp_path: Path):
    rec = RunRecorder(tmp_path / "target/heartbeat")

    def worker(idx: int):
        rec.append(_build())

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    lines = rec.jsonl_path.read_text().strip().split("\n")
    assert len(lines) == 20
    for ln in lines:
        parsed = json.loads(ln)  # must each parse cleanly — no torn lines
        assert parsed["run_id"] == "abc"


def test_tail_returns_last_n(tmp_path: Path):
    rec = RunRecorder(tmp_path / "target/heartbeat")
    for i in range(5):
        run = _build()
        run.run_id = f"run-{i}"
        rec.append(run)
    last = rec.tail(limit=3)
    assert len(last) == 3
    assert last[-1]["run_id"] == "run-4"


def test_tail_empty(tmp_path: Path):
    rec = RunRecorder(tmp_path / "target/heartbeat")
    assert rec.tail() == []


def test_make_run_resolves_status():
    run = make_run(
        run_id="x",
        started_at="2026-05-15T10:00:00Z",
        finished_at="2026-05-15T10:00:01Z",
        duration_ms=500,
        ingested=[IngestResult(file="a", table="t", status="ok")],
        dag=DagResult(),
        exposure=[],
        errors=[],
    )
    assert run.status == "ok"


def test_make_run_skipped_status():
    run = make_run(
        run_id="x",
        started_at="t",
        finished_at="t",
        duration_ms=0,
        ingested=[],
        dag=DagResult(),
        exposure=[],
        errors=[],
        reason="tick-in-flight",
        status="skipped",
    )
    assert run.status == "skipped"
    assert run.reason == "tick-in-flight"
