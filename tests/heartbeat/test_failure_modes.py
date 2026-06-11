"""Failure-mode tests for DAG/Exposure (AC8, AC9)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from seeknal.heartbeat.config import HeartbeatConfig
from seeknal.heartbeat.recorder import DagResult, ExposureResult
from seeknal.heartbeat.runner import HeartbeatRunner


@pytest.mark.asyncio
async def test_dag_failure_does_not_exit_daemon(
    config: HeartbeatConfig, temp_project: Path
):
    (temp_project / "inbox" / "x.csv").write_text("a\n1\n")
    runner = HeartbeatRunner(config, temp_project)

    def crash_dag(self, errors):
        errors.append("DAG simulated crash")
        return DagResult(nodes_failed=1)

    with patch.object(HeartbeatRunner, "_run_dag", crash_dag), patch.object(
        HeartbeatRunner, "_run_exposures", lambda self, errors: []
    ):
        run = await runner.tick_once(reason="manual")
    assert run.dag.nodes_failed == 1
    assert any("DAG simulated crash" in e for e in run.errors)
    assert run.status in {"partial", "failed"}


@pytest.mark.asyncio
async def test_exposure_failure_recorded(
    config: HeartbeatConfig, temp_project: Path
):
    (temp_project / "inbox" / "y.csv").write_text("a\n1\n")
    runner = HeartbeatRunner(config, temp_project)

    def fail_expo(self, errors):
        return [ExposureResult(node="e1", target="t", status="failed", error="x")]

    with patch.object(HeartbeatRunner, "_run_dag", lambda self, errors: DagResult()), patch.object(
        HeartbeatRunner, "_run_exposures", fail_expo
    ):
        run = await runner.tick_once(reason="manual")
    assert run.exposure[0].status == "failed"
    assert run.status in {"partial", "failed"}


@pytest.mark.asyncio
async def test_inbox_state_save_failure_recorded(
    config: HeartbeatConfig, temp_project: Path, monkeypatch
):
    (temp_project / "inbox" / "z.csv").write_text("a\n1\n")
    runner = HeartbeatRunner(config, temp_project)

    def boom(self):
        raise RuntimeError("disk full")

    monkeypatch.setattr(
        type(runner._inbox_state), "save", boom
    )

    with patch.object(
        HeartbeatRunner, "_run_dag", lambda self, errors: DagResult()
    ), patch.object(
        HeartbeatRunner, "_run_exposures", lambda self, errors: []
    ):
        run = await runner.tick_once(reason="manual")
    assert any("inbox_state.save failed" in e for e in run.errors)
