"""RunRecorder — append run records to ``target/heartbeat/runs.jsonl``.

Each tick writes a single summary JSON line. A pretty-printed sidecar is
written to ``target/heartbeat/runs/<run_id>.json`` for human inspection.
"""

from __future__ import annotations

import fcntl
import json
import logging
import os
import tempfile
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, Optional

logger = logging.getLogger("seeknal.heartbeat.recorder")


@dataclass
class IngestResult:
    file: str
    table: str
    rows: int = 0
    bytes: int = 0
    target: Literal["parquet", "iceberg"] = "parquet"
    status: Literal[
        "ok", "quarantined", "quarantined_for_review", "failed"
    ] = "ok"
    error: Optional[str] = None
    confidence: Optional[float] = None
    reason: Optional[str] = None


@dataclass
class DagResult:
    nodes_attempted: int = 0
    nodes_skipped: int = 0
    nodes_failed: int = 0
    fingerprints: dict[str, str] = field(default_factory=dict)
    per_node: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class ExposureResult:
    node: str
    target: str
    status: Literal["ok", "failed", "skipped"]
    error: Optional[str] = None


@dataclass
class ClassifierEntry:
    """Per-file classifier decision logged into the run record (AC21)."""

    file: str
    source_used: Literal["catalog", "llm", "default", "fallback"] = "default"
    target_table: str = ""
    confidence: float = 0.0
    llm_call_id: Optional[str] = None
    decision: Literal["routed", "quarantined"] = "routed"


@dataclass
class HeartbeatRun:
    run_id: str
    started_at: str
    finished_at: str
    duration_ms: int
    status: Literal["ok", "skipped", "partial", "failed"]
    reason: Optional[str] = None
    ingested: list[IngestResult] = field(default_factory=list)
    dag: DagResult = field(default_factory=DagResult)
    exposure: list[ExposureResult] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    classifier: list[ClassifierEntry] = field(default_factory=list)
    llm_prompt_truncated: Optional[str] = None
    llm_response_truncated: Optional[str] = None

    @classmethod
    def compute_status(
        cls,
        ingested: list[IngestResult],
        dag: DagResult,
        exposure: list[ExposureResult],
        errors: list[str],
    ) -> Literal["ok", "partial", "failed"]:
        ok_ingest = [r for r in ingested if r.status == "ok"]
        bad_ingest = [r for r in ingested if r.status not in {"ok"}]
        ingest_failed = ingested and not ok_ingest
        dag_failed = dag.nodes_failed > 0
        bad_exposure = any(r.status == "failed" for r in exposure)
        all_failed = ingest_failed and dag_failed and (bad_exposure or not exposure)
        if all_failed:
            return "failed"
        any_failure = bool(errors) or bool(bad_ingest) or dag_failed or bad_exposure
        if any_failure:
            return "partial"
        return "ok"


def _utcnow_iso() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _to_jsonable(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "__dataclass_fields__"):
        return asdict(value)
    return value


class RunRecorder:
    """Append-only writer for ``runs.jsonl`` + sidecar JSON files."""

    def __init__(self, runs_dir: Path):
        runs_dir = Path(runs_dir)
        self._runs_dir = runs_dir
        self._runs_jsonl = runs_dir / "runs.jsonl"
        self._sidecar_dir = runs_dir / "runs"

    @property
    def runs_dir(self) -> Path:
        return self._runs_dir

    @property
    def jsonl_path(self) -> Path:
        return self._runs_jsonl

    @property
    def sidecar_dir(self) -> Path:
        return self._sidecar_dir

    def append(self, run: HeartbeatRun) -> None:
        self._runs_dir.mkdir(parents=True, exist_ok=True)
        self._sidecar_dir.mkdir(parents=True, exist_ok=True)
        line = json.dumps(asdict(run), default=_to_jsonable, sort_keys=True)
        # Use fcntl.flock to serialize concurrent appends.
        with open(self._runs_jsonl, "a") as fh:
            try:
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
                fh.write(line + "\n")
                fh.flush()
                os.fsync(fh.fileno())
            finally:
                fcntl.flock(fh.fileno(), fcntl.LOCK_UN)

        sidecar_path = self._sidecar_dir / f"{run.run_id}.json"
        fd, tmp_path = tempfile.mkstemp(
            prefix=".heartbeat-run.", suffix=".json.tmp", dir=str(self._sidecar_dir)
        )
        try:
            with os.fdopen(fd, "w") as fh:
                json.dump(
                    asdict(run),
                    fh,
                    default=_to_jsonable,
                    indent=2,
                    sort_keys=True,
                )
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp_path, sidecar_path)
        except Exception:
            try:
                os.unlink(tmp_path)
            except FileNotFoundError:
                pass
            raise

    def tail(self, limit: int = 10) -> list[dict[str, Any]]:
        if not self._runs_jsonl.exists():
            return []
        with open(self._runs_jsonl, "r") as fh:
            lines = fh.readlines()
        out: list[dict[str, Any]] = []
        for line in lines[-limit:]:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError as exc:
                logger.warning("Skipping malformed runs.jsonl line: %s", exc)
        return out


def make_run(
    run_id: str,
    started_at: str,
    finished_at: str,
    duration_ms: int,
    ingested: list[IngestResult],
    dag: DagResult,
    exposure: list[ExposureResult],
    errors: list[str],
    *,
    reason: Optional[str] = None,
    status: Optional[Literal["ok", "skipped", "partial", "failed"]] = None,
    classifier: Optional[list[ClassifierEntry]] = None,
) -> HeartbeatRun:
    resolved_status = status or HeartbeatRun.compute_status(
        ingested, dag, exposure, errors
    )
    return HeartbeatRun(
        run_id=run_id,
        started_at=started_at,
        finished_at=finished_at,
        duration_ms=duration_ms,
        status=resolved_status,
        reason=reason,
        ingested=ingested,
        dag=dag,
        exposure=exposure,
        errors=errors,
        classifier=classifier or [],
    )


__all__ = [
    "ClassifierEntry",
    "DagResult",
    "ExposureResult",
    "HeartbeatRun",
    "IngestResult",
    "RunRecorder",
    "make_run",
    "_utcnow_iso",
]
