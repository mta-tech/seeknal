"""Tests for HeartbeatRunner."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from unittest.mock import patch

import pytest

from seeknal.heartbeat.config import HeartbeatConfig
from seeknal.heartbeat.recorder import DagResult, ExposureResult
from seeknal.heartbeat.runner import HeartbeatRunner


def _patch_no_project_artifacts():
    """Patch DAG/exposure subsystems to make the heartbeat work in a bare temp project.

    The temp project has no YAML pipelines, so DAGBuilder().build() would fail.
    Patches force _run_dag and _run_exposures into no-ops.
    """
    return patch.multiple(
        HeartbeatRunner,
        _run_dag=lambda self, errors: DagResult(),
        _run_exposures=lambda self, errors: [],
    )


@pytest.mark.asyncio
async def test_tick_empty_inbox_records_ok(config: HeartbeatConfig, temp_project: Path):
    runner = HeartbeatRunner(config, temp_project)
    with _patch_no_project_artifacts():
        run = await runner.tick_once(reason="manual")
    assert run.status in {"ok", "partial"}
    lines = runner.recorder.jsonl_path.read_text().strip().split("\n")
    assert len(lines) == 1


@pytest.mark.asyncio
async def test_tick_ingests_new_file(config: HeartbeatConfig, temp_project: Path):
    (temp_project / "inbox" / "sales.csv").write_text("a,b\n1,2\n3,4\n")
    runner = HeartbeatRunner(config, temp_project)
    with _patch_no_project_artifacts():
        run = await runner.tick_once(reason="manual")
    assert len(run.ingested) == 1
    assert run.ingested[0].table == "sales"
    assert run.ingested[0].status == "ok"
    assert (temp_project / "target/ingested/sales/data.parquet").exists()


@pytest.mark.asyncio
async def test_tick_idempotent(config: HeartbeatConfig, temp_project: Path):
    (temp_project / "inbox" / "sales.csv").write_text("a,b\n1,2\n")
    runner = HeartbeatRunner(config, temp_project)
    with _patch_no_project_artifacts():
        await runner.tick_once(reason="interval")
        run2 = await runner.tick_once(reason="interval")
    assert run2.ingested == []  # AC5: idempotent re-tick
    assert run2.reason == "interval"


@pytest.mark.asyncio
async def test_tick_in_flight_records_skipped(config: HeartbeatConfig, temp_project: Path):
    runner = HeartbeatRunner(config, temp_project)

    async def slow_tick(self, run_id, started_at, t0, reason):
        await asyncio.sleep(0.2)
        return await HeartbeatRunner._tick_locked.__wrapped__(self, run_id, started_at, t0, reason) if False else await asyncio.sleep(0)  # noqa

    with _patch_no_project_artifacts():
        # Hold the lock by starting one tick, then issue a second concurrently.
        async def runner_with_delay():
            await runner._asyncio_lock.acquire()
            await asyncio.sleep(0.1)
            runner._asyncio_lock.release()

        slow_task = asyncio.create_task(runner_with_delay())
        await asyncio.sleep(0.01)
        skipped = await runner.tick_once(reason="manual")
        await slow_task

    assert skipped.status == "skipped"
    assert skipped.reason == "tick-in-flight"


@pytest.mark.asyncio
async def test_tick_dag_failure_records_but_continues(
    config: HeartbeatConfig, temp_project: Path
):
    (temp_project / "inbox" / "x.csv").write_text("a\n1\n")
    runner = HeartbeatRunner(config, temp_project)

    def boom(self, errors):
        errors.append("simulated DAG crash")
        return DagResult(nodes_failed=2)

    with patch.object(HeartbeatRunner, "_run_dag", boom), patch.object(
        HeartbeatRunner, "_run_exposures", lambda self, errors: []
    ):
        run = await runner.tick_once(reason="manual")
    assert run.dag.nodes_failed == 2
    assert any("simulated DAG crash" in e for e in run.errors)
    assert run.status in {"partial", "failed"}


@pytest.mark.asyncio
async def test_tick_exposure_failure_recorded(
    config: HeartbeatConfig, temp_project: Path
):
    (temp_project / "inbox" / "y.csv").write_text("a\n1\n")
    runner = HeartbeatRunner(config, temp_project)

    def fail_expo(self, errors):
        errors.append("expo crash")
        return [ExposureResult(node="e1", target="t", status="failed", error="boom")]

    with patch.object(HeartbeatRunner, "_run_dag", lambda self, errors: DagResult()), patch.object(
        HeartbeatRunner, "_run_exposures", fail_expo
    ):
        run = await runner.tick_once(reason="manual")
    assert run.exposure[0].status == "failed"
    assert run.status in {"partial", "failed"}


@pytest.mark.asyncio
async def test_run_forever_exits_when_disabled(disabled_project: Path):
    cfg = HeartbeatConfig.load(disabled_project)
    runner = HeartbeatRunner(cfg, disabled_project)
    # Should exit immediately.
    await asyncio.wait_for(runner.run_forever(), timeout=1.0)


@pytest.mark.asyncio
async def test_run_forever_exits_when_interval_zero(temp_project: Path):
    cfg = HeartbeatConfig.load(temp_project)
    assert cfg.interval_seconds == 0
    runner = HeartbeatRunner(cfg, temp_project)
    await asyncio.wait_for(runner.run_forever(), timeout=1.0)


@pytest.mark.asyncio
async def test_quarantined_file_moved(config: HeartbeatConfig, temp_project: Path):
    bad = temp_project / "inbox" / "select.csv"  # reserved keyword → quarantine
    bad.write_text("a\n1\n")
    runner = HeartbeatRunner(config, temp_project)
    with _patch_no_project_artifacts():
        run = await runner.tick_once(reason="manual")
    assert any(r.status == "quarantined" for r in run.ingested)
    assert not bad.exists()
    quarantine_dir = temp_project / config.quarantine_dir
    assert any(quarantine_dir.iterdir())
