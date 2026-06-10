"""End-to-end heartbeat tests (AC11) — full happy-path round-trip."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from seeknal.heartbeat.config import HeartbeatConfig
from seeknal.heartbeat.recorder import DagResult, ExposureResult
from seeknal.heartbeat.runner import HeartbeatRunner


@pytest.mark.asyncio
async def test_full_happy_path(tmp_path: Path):
    """Drop file → tick → parquet + DAG + exposure + runs.jsonl line."""
    (tmp_path / "HEARTBEAT.md").write_text(
        """```yaml
interval: 0s
enabled: true
inbox_folders:
  - inbox
```
"""
    )
    (tmp_path / "inbox").mkdir()
    (tmp_path / "inbox" / "orders.csv").write_text(
        "order_id,amount\nA001,99.50\nA002,42.00\n"
    )

    cfg = HeartbeatConfig.load(tmp_path)
    runner = HeartbeatRunner(cfg, tmp_path)

    with patch.object(
        HeartbeatRunner,
        "_run_dag",
        lambda self, errors: DagResult(
            nodes_attempted=1, nodes_skipped=0, nodes_failed=0
        ),
    ), patch.object(
        HeartbeatRunner,
        "_run_exposures",
        lambda self, errors: [
            ExposureResult(node="exp1", target="file://out.csv", status="ok")
        ],
    ):
        run = await runner.tick_once(reason="manual")

    assert run.status == "ok"
    assert len(run.ingested) == 1
    assert run.ingested[0].status == "ok"
    assert run.ingested[0].table == "orders"
    assert run.dag.nodes_attempted == 1
    assert run.exposure and run.exposure[0].status == "ok"

    parquet = tmp_path / "target/ingested/orders/data.parquet"
    assert parquet.exists()

    jsonl = tmp_path / "target/heartbeat/runs.jsonl"
    assert jsonl.exists()
    line = jsonl.read_text().strip().split("\n")[-1]
    payload = json.loads(line)
    assert payload["run_id"] == run.run_id
    # AC4 schema completeness
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
        assert key in payload


@pytest.mark.asyncio
async def test_repeat_tick_skips_dag_and_exposure(tmp_path: Path):
    """AC5: no new files → idempotent tick (no DAG/exposure on interval reason)."""
    (tmp_path / "HEARTBEAT.md").write_text(
        "interval: 0s\ninbox_folders:\n  - inbox\n"
    )
    (tmp_path / "inbox").mkdir()
    (tmp_path / "inbox" / "first.csv").write_text("a\n1\n")

    cfg = HeartbeatConfig.load(tmp_path)
    runner = HeartbeatRunner(cfg, tmp_path)

    dag_call_count = {"n": 0}
    exposure_call_count = {"n": 0}

    def stub_dag(self, errors):
        dag_call_count["n"] += 1
        return DagResult(nodes_attempted=1)

    def stub_exposure(self, errors):
        exposure_call_count["n"] += 1
        return [ExposureResult(node="e", target="t", status="ok")]

    with patch.object(HeartbeatRunner, "_run_dag", stub_dag), patch.object(
        HeartbeatRunner, "_run_exposures", stub_exposure
    ):
        await runner.tick_once(reason="interval")
        await runner.tick_once(reason="interval")

    assert dag_call_count["n"] == 1
    assert exposure_call_count["n"] == 1
