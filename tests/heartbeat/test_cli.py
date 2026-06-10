"""Tests for the `seeknal heartbeat` CLI sub-app."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from seeknal.cli.heartbeat import app
from seeknal.heartbeat.recorder import DagResult, HeartbeatRun, RunRecorder
from seeknal.heartbeat.runner import HeartbeatRunner


runner = CliRunner()


def _no_dag():
    return patch.multiple(
        HeartbeatRunner,
        _run_dag=lambda self, errors: DagResult(),
        _run_exposures=lambda self, errors: [],
    )


def _write_heartbeat_md(project: Path, body: str) -> None:
    (project / "HEARTBEAT.md").write_text(body)


def test_tick_command_runs(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _write_heartbeat_md(
        tmp_path,
        """```yaml
interval: 0s
enabled: true
```
""",
    )
    (tmp_path / "inbox").mkdir()
    with _no_dag():
        result = runner.invoke(app, ["tick"])
    assert result.exit_code == 0, result.output
    assert "status=" in result.output


def test_tick_ingests_csv(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _write_heartbeat_md(
        tmp_path,
        """```yaml
interval: 0s
```
""",
    )
    (tmp_path / "inbox").mkdir()
    (tmp_path / "inbox" / "sales.csv").write_text("a,b\n1,2\n")
    with _no_dag():
        result = runner.invoke(app, ["tick"])
    assert result.exit_code == 0
    assert "ingested=1" in result.output
    assert (tmp_path / "target/ingested/sales/data.parquet").exists()


def test_start_with_interval_zero_exits_clean(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _write_heartbeat_md(
        tmp_path,
        """```yaml
interval: 0s
```
""",
    )
    result = runner.invoke(app, ["start"])
    assert result.exit_code == 0
    assert "disabled" in result.output.lower() or "exit" in result.output.lower() or "tick" in result.output.lower()


def test_start_with_enabled_false_exits_clean(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _write_heartbeat_md(
        tmp_path,
        """```yaml
interval: 30s
enabled: false
```
""",
    )
    result = runner.invoke(app, ["start"])
    assert result.exit_code == 0


def test_status_command_empty(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["status"])
    assert result.exit_code == 0
    assert "No heartbeat runs" in result.output


def test_status_command_with_runs(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    recorder = RunRecorder(tmp_path / "target/heartbeat")
    for i in range(3):
        run = HeartbeatRun(
            run_id=f"run-{i}",
            started_at="2026-05-15T10:00:00Z",
            finished_at="2026-05-15T10:00:01Z",
            duration_ms=10,
            status="ok",
        )
        recorder.append(run)
    result = runner.invoke(app, ["status", "--limit", "2"])
    assert result.exit_code == 0
    assert "run-2" in result.output
    assert "run-1" in result.output
    assert "run-0" not in result.output


def test_review_command_no_run(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["review", "nope"])
    assert result.exit_code != 0
    assert "No quarantine entries" in result.output


def test_review_command_empty_dir(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    qdir = tmp_path / "target/heartbeat/quarantine/needs_review/abc"
    qdir.mkdir(parents=True)
    result = runner.invoke(app, ["review", "abc"])
    assert result.exit_code == 0
    assert "No pending review" in result.output


def test_main_app_registers_heartbeat(tmp_path: Path, monkeypatch):
    """Smoke: top-level seeknal app should know about `heartbeat`."""
    from seeknal.cli.main import app as main_app
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(main_app, ["heartbeat", "--help"])
    assert result.exit_code == 0
    assert "tick" in result.output
    assert "start" in result.output
    assert "status" in result.output
