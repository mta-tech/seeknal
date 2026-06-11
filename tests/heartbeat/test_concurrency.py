"""Concurrency tests — overlap detection across asyncio + cross-process flock."""

from __future__ import annotations

import asyncio
import fcntl
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
async def test_asyncio_lock_serializes_concurrent_ticks(
    config: HeartbeatConfig, temp_project: Path
):
    runner = HeartbeatRunner(config, temp_project)

    async def hold_lock(seconds: float):
        await runner._asyncio_lock.acquire()
        try:
            await asyncio.sleep(seconds)
        finally:
            runner._asyncio_lock.release()

    with _no_dag():
        holder = asyncio.create_task(hold_lock(0.15))
        await asyncio.sleep(0.02)
        run = await runner.tick_once(reason="manual")
        await holder

    assert run.status == "skipped"
    assert run.reason == "tick-in-flight"


@pytest.mark.asyncio
async def test_fcntl_lock_blocks_external_tick(
    config: HeartbeatConfig, temp_project: Path
):
    runner = HeartbeatRunner(config, temp_project)
    target = temp_project / "target" / "heartbeat"
    target.mkdir(parents=True, exist_ok=True)
    lock_path = target / ".lock"
    holder = open(lock_path, "w")
    try:
        fcntl.flock(holder.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        with _no_dag():
            run = await runner.tick_once(reason="manual")
        assert run.status == "skipped"
        assert run.reason == "tick-in-flight"
    finally:
        try:
            fcntl.flock(holder.fileno(), fcntl.LOCK_UN)
        except Exception:
            pass
        holder.close()
