"""HeartbeatRunner — the polling loop.

Phases per tick:
    A   Scan    — discover new/changed files in inbox folders.
    A.5 Classify (opt-in v0.5) — route each file to a target table.
    B   Ingest  — write each file via :class:`IngestWriter`; quarantine bad ones.
    C   DAG     — invoke ``DAGRunner.run()`` if anything new ingested.
    D   Exposure — call ``ExposureExecutor.run()`` for each exposure node.
    E   Record  — append a ``HeartbeatRun`` to ``runs.jsonl`` + sidecar.

The loop never exits on data errors; each phase is wrapped in best-effort
try/except. Overlapping ticks (same process or another process) are detected
via ``asyncio.Lock`` + ``fcntl.flock`` and recorded as ``status=skipped``.
"""

from __future__ import annotations

import asyncio
import fcntl
import logging
import os
import signal
import time
import traceback
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Optional

from seeknal.heartbeat.config import HeartbeatConfig
from seeknal.heartbeat.inbox_state import InboxState
from seeknal.heartbeat.ingest import IngestWriter, quarantine_file
from seeknal.heartbeat.recorder import (
    DagResult,
    ExposureResult,
    HeartbeatRun,
    IngestResult,
    RunRecorder,
    make_run,
    _utcnow_iso,
)

logger = logging.getLogger("seeknal.heartbeat.runner")


def _try_load_dotenv() -> None:
    """Best-effort .env loader for non-CLI heartbeat startup. No-op if missing."""
    try:
        from dotenv import load_dotenv  # type: ignore

        load_dotenv(usecwd=True)
    except Exception:  # noqa: BLE001
        pass


class HeartbeatRunner:
    """Drives heartbeat ticks. Owns lifecycle but delegates work."""

    def __init__(
        self,
        config: HeartbeatConfig,
        project_root: Path,
        *,
        profile_loader: Optional[Any] = None,
    ):
        _try_load_dotenv()
        self._config = config
        self._project_root = Path(project_root).expanduser().resolve()
        self._profile_loader = profile_loader

        target_dir = self._project_root / "target" / "heartbeat"
        target_dir.mkdir(parents=True, exist_ok=True)

        self._inbox_state = InboxState.load(target_dir / "inbox_state.json")
        self._recorder = RunRecorder(target_dir)
        self._lock_path = target_dir / ".lock"
        self._asyncio_lock = asyncio.Lock()
        self._shutdown_event = asyncio.Event()

        iceberg_uri = None
        iceberg_warehouse = None
        if config.ingested_target == "iceberg" and profile_loader is not None:
            try:
                defaults = profile_loader.load_source_defaults("iceberg") or {}
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Failed to load iceberg source_defaults: %s", exc
                )
                defaults = {}
            iceberg_uri = defaults.get("catalog_uri")
            iceberg_warehouse = defaults.get("warehouse")

        try:
            self._ingest_writer = IngestWriter(
                self._project_root,
                target=config.ingested_target or "parquet",
                iceberg_catalog_uri=iceberg_uri,
                iceberg_warehouse=iceberg_warehouse,
            )
        except ValueError as exc:
            logger.warning(
                "Iceberg target unavailable (%s); falling back to parquet.", exc
            )
            self._ingest_writer = IngestWriter(self._project_root, target="parquet")

    @property
    def config(self) -> HeartbeatConfig:
        return self._config

    @property
    def project_root(self) -> Path:
        return self._project_root

    @property
    def recorder(self) -> RunRecorder:
        return self._recorder

    @property
    def inbox_state(self) -> InboxState:
        return self._inbox_state

    @property
    def ingest_writer(self) -> IngestWriter:
        return self._ingest_writer

    def request_shutdown(self) -> None:
        self._shutdown_event.set()

    @contextmanager
    def _fcntl_lock(self):
        """Acquire a non-blocking flock; raise BlockingIOError if held."""
        self._lock_path.parent.mkdir(parents=True, exist_ok=True)
        fh = open(self._lock_path, "w")
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            yield
        finally:
            try:
                fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
            except Exception:
                pass
            fh.close()

    def _resolve_inbox_folders(self) -> list[Path]:
        out: list[Path] = []
        for folder in self._config.inbox_folders:
            folder_path = (
                folder if folder.is_absolute() else self._project_root / folder
            )
            out.append(folder_path)
        return out

    async def tick_once(self, reason: str = "manual") -> HeartbeatRun:
        """Execute a single tick. Always returns a HeartbeatRun (never raises)."""
        run_id = uuid.uuid4().hex
        started_at = _utcnow_iso()
        t0 = time.perf_counter()

        # In-process single-flight (AC6).
        if self._asyncio_lock.locked():
            return self._record_skipped(
                run_id, started_at, reason="tick-in-flight"
            )

        async with self._asyncio_lock:
            try:
                with self._fcntl_lock():
                    return await self._tick_locked(run_id, started_at, t0, reason)
            except BlockingIOError:
                return self._record_skipped(
                    run_id, started_at, reason="tick-in-flight"
                )

    async def _tick_locked(
        self,
        run_id: str,
        started_at: str,
        t0: float,
        reason: str,
    ) -> HeartbeatRun:
        errors: list[str] = []
        ingested: list[IngestResult] = []

        # Phase A — Scan
        folders = self._resolve_inbox_folders()
        try:
            new_paths = self._inbox_state.scan(folders)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"scan failed: {exc}")
            logger.warning("Scan failed: %s", exc)
            new_paths = []

        # Phase B — Ingest
        for path in new_paths:
            try:
                result = self._ingest_writer.write(path)
                ingested.append(result)
                if result.status == "ok":
                    self._inbox_state.mark_ingested(
                        path, target_table=result.table, status="ok"
                    )
                elif result.status == "quarantined":
                    try:
                        moved = quarantine_file(path, self._project_root / self._config.quarantine_dir)
                        result.file = str(moved)
                    except Exception as exc:  # noqa: BLE001
                        errors.append(f"quarantine move failed: {exc}")
                    self._inbox_state.mark_quarantined(
                        path,
                        reason=result.error or "quarantined",
                        target_table=result.table,
                    )
                else:
                    self._inbox_state.mark_failed(path, target_table=result.table)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"ingest exception for {path}: {exc}")
                logger.exception("Unexpected ingest exception for %s", path)
                ingested.append(
                    IngestResult(
                        file=str(path),
                        table="",
                        target=self._ingest_writer.target,  # type: ignore[arg-type]
                        status="failed",
                        error=str(exc),
                    )
                )
        try:
            self._inbox_state.save()
        except Exception as exc:  # noqa: BLE001
            errors.append(f"inbox_state.save failed: {exc}")

        # AC5 idempotency gate
        has_new_ok = any(r.status == "ok" for r in ingested)
        run_phases = has_new_ok or reason != "interval"

        dag_result = DagResult()
        exposure_results: list[ExposureResult] = []

        if run_phases:
            dag_result = self._run_dag(errors)
            exposure_results = self._run_exposures(errors)

        finished_at = _utcnow_iso()
        duration_ms = int((time.perf_counter() - t0) * 1000)

        run = make_run(
            run_id=run_id,
            started_at=started_at,
            finished_at=finished_at,
            duration_ms=duration_ms,
            ingested=ingested,
            dag=dag_result,
            exposure=exposure_results,
            errors=errors,
            reason=reason if not has_new_ok and reason == "interval" else None,
        )
        try:
            self._recorder.append(run)
        except Exception as exc:  # noqa: BLE001
            logger.warning("RunRecorder append failed: %s", exc)
        # Proactive Telegram notification — env-var gated, best-effort.
        # Never imports python-telegram-bot or seeknal.ask.gateway code
        # (Principle 5 / lint contract); uses stdlib urllib to POST to
        # api.telegram.org directly.
        try:
            from seeknal.heartbeat.tick_notifier import notify_tick

            notify_tick(run, project_name=self._config.project_name)
        except Exception as exc:  # noqa: BLE001
            logger.warning("tick notification failed: %s", exc)
        return run

    def _run_dag(self, errors: list[str]) -> DagResult:
        try:
            from seeknal.workflow.dag import DAGBuilder
            from seeknal.workflow.runner import DAGRunner
            from seeknal.workflow.executors.base import ExecutionContext
            from seeknal.workflow.manifest_builder import build_manifest_from_dag
        except Exception as exc:  # noqa: BLE001
            errors.append(f"DAG imports failed: {exc}")
            return DagResult()

        try:
            dag_builder = DAGBuilder(
                project_path=self._project_root,
                profile_path=self._config.profile_path,
            )
            dag_builder.build()
            manifest = build_manifest_from_dag(
                dag_builder, self._config.project_name
            )
        except Exception as exc:  # noqa: BLE001
            errors.append(f"DAG build failed: {exc}\n{traceback.format_exc()}")
            return DagResult()

        try:
            exec_context = ExecutionContext(
                project_name=self._config.project_name,
                workspace_path=self._project_root,
                target_path=self._project_root / "target",
                profile_path=self._config.profile_path,
                env_name=os.getenv("SEEKNAL_ENV"),
            )
            runner = DAGRunner(
                manifest=manifest,
                target_path=self._project_root / "target",
                exec_context=exec_context,
            )
            summary = runner.run()
            return DagResult(
                nodes_attempted=summary.total_nodes,
                nodes_skipped=summary.skipped_nodes,
                nodes_failed=summary.failed_nodes,
                fingerprints=dict(summary.fingerprints),
                per_node=[
                    {
                        "node_id": nr.node_id,
                        "status": nr.status.value,
                        "duration": nr.duration,
                        "row_count": nr.row_count,
                    }
                    for nr in summary.results
                ],
            )
        except Exception as exc:  # noqa: BLE001
            errors.append(f"DAG run failed: {exc}")
            logger.exception("DAGRunner.run failed")
            return DagResult(nodes_failed=1)

    def _run_exposures(self, errors: list[str]) -> list[ExposureResult]:
        results: list[ExposureResult] = []
        try:
            from seeknal.workflow.dag import DAGBuilder
            from seeknal.workflow.executors.base import (
                ExecutionContext,
                ExecutionStatus,
            )
            from seeknal.workflow.executors.exposure_executor import (
                ExposureExecutor,
            )
            from seeknal.workflow.manifest_builder import build_manifest_from_dag
            from seeknal.dag.manifest import NodeType
        except Exception as exc:  # noqa: BLE001
            errors.append(f"Exposure imports failed: {exc}")
            return results

        try:
            dag_builder = DAGBuilder(
                project_path=self._project_root,
                profile_path=self._config.profile_path,
            )
            dag_builder.build()
            manifest = build_manifest_from_dag(
                dag_builder, self._config.project_name
            )
        except Exception as exc:  # noqa: BLE001
            errors.append(f"Exposure manifest build failed: {exc}")
            return results

        exec_context = ExecutionContext(
            project_name=self._config.project_name,
            workspace_path=self._project_root,
            target_path=self._project_root / "target",
            profile_path=self._config.profile_path,
            env_name=os.getenv("SEEKNAL_ENV"),
        )

        exposures = [
            n for n in manifest.nodes.values() if n.node_type == NodeType.EXPOSURE
        ]
        for node in exposures:
            try:
                executor = ExposureExecutor(node, exec_context)
                exec_result = executor.run()
                status = (
                    "ok"
                    if exec_result.status == ExecutionStatus.SUCCESS
                    else "failed"
                )
                target = str(node.config.get("target", "?")) if node.config else "?"
                results.append(
                    ExposureResult(
                        node=node.id,
                        target=target,
                        status=status,  # type: ignore[arg-type]
                        error=exec_result.error_message,
                    )
                )
                if status == "failed":
                    errors.append(
                        f"Exposure {node.id} failed: {exec_result.error_message}"
                    )
            except Exception as exc:  # noqa: BLE001
                errors.append(f"Exposure node {node.id} crashed: {exc}")
                results.append(
                    ExposureResult(
                        node=node.id,
                        target="?",
                        status="failed",
                        error=str(exc),
                    )
                )
        return results

    def _record_skipped(
        self, run_id: str, started_at: str, *, reason: str
    ) -> HeartbeatRun:
        finished_at = _utcnow_iso()
        run = make_run(
            run_id=run_id,
            started_at=started_at,
            finished_at=finished_at,
            duration_ms=0,
            ingested=[],
            dag=DagResult(),
            exposure=[],
            errors=[],
            reason=reason,
            status="skipped",
        )
        try:
            self._recorder.append(run)
        except Exception as exc:  # noqa: BLE001
            logger.warning("RunRecorder append failed: %s", exc)
        return run

    async def run_forever(self) -> None:
        """Loop until shutdown_event. Handles SIGTERM/SIGINT gracefully."""
        if not self._config.enabled or self._config.interval_seconds <= 0:
            logger.info(
                "Heartbeat disabled (enabled=%s, interval=%ss); exiting.",
                self._config.enabled,
                self._config.interval_seconds,
            )
            return

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, self._shutdown_event.set)
            except (NotImplementedError, ValueError):  # Windows / non-main thread
                pass

        while not self._shutdown_event.is_set():
            try:
                await self.tick_once(reason="interval")
            except Exception as exc:  # noqa: BLE001
                logger.exception("Unexpected tick crash: %s", exc)
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=self._config.interval_seconds,
                )
                break
            except asyncio.TimeoutError:
                continue
