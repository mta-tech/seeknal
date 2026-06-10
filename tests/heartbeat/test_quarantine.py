"""Quarantine path tests — files that can't be ingested are moved aside."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from seeknal.heartbeat.config import HeartbeatConfig
from seeknal.heartbeat.recorder import DagResult
from seeknal.heartbeat.runner import HeartbeatRunner


def _no_dag():
    return patch.multiple(
        HeartbeatRunner,
        _run_dag=lambda self, errors: DagResult(),
        _run_exposures=lambda self, errors: [],
    )


@pytest.mark.asyncio
async def test_reserved_keyword_file_quarantined(
    config: HeartbeatConfig, temp_project: Path
):
    bad = temp_project / "inbox" / "select.csv"
    bad.write_text("a\n1\n")
    runner = HeartbeatRunner(config, temp_project)
    with _no_dag():
        run = await runner.tick_once(reason="manual")
    assert any(r.status == "quarantined" for r in run.ingested)
    assert not bad.exists()
    moved = list((temp_project / config.quarantine_dir).iterdir())
    assert moved, "Quarantined file should exist in quarantine_dir"


@pytest.mark.asyncio
async def test_unsupported_suffix_quarantined(
    config: HeartbeatConfig, temp_project: Path
):
    bad = temp_project / "inbox" / "weird.bin"
    bad.write_bytes(b"\x00\x01\x02")
    runner = HeartbeatRunner(config, temp_project)
    with _no_dag():
        run = await runner.tick_once(reason="manual")
    assert any(r.status == "quarantined" for r in run.ingested)


@pytest.mark.asyncio
async def test_daemon_continues_after_quarantine(
    config: HeartbeatConfig, temp_project: Path
):
    """Verifies daemon never exits on data errors (AC7)."""
    bad = temp_project / "inbox" / "weird.bin"
    bad.write_bytes(b"\x00")
    good = temp_project / "inbox" / "sales.csv"
    good.write_text("a,b\n1,2\n")
    runner = HeartbeatRunner(config, temp_project)
    with _no_dag():
        run = await runner.tick_once(reason="manual")
    statuses = sorted(r.status for r in run.ingested)
    assert "ok" in statuses
    assert "quarantined" in statuses
