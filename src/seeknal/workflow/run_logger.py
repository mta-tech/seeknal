"""
Per-run log file writer for pipeline execution.

Collects node execution details and writes them to a structured
plain-text log file at target/logs/run_{run_id}.log.

Default mode: log file only created when at least one node fails.
Verbose mode: log file always created with all node details.
"""

import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional


@dataclass
class NodeLogEntry:
    """A single node's execution log entry."""
    node_id: str
    status: str
    duration_seconds: float = 0.0
    error_message: Optional[str] = None
    output_log: Optional[str] = None
    row_count: int = 0
    attempt: Optional[int] = None
    total_attempts: Optional[int] = None


class RunLogger:
    """Collects node execution results and writes a structured log file.

    Thread-safe: uses a lock for concurrent log_node() calls in parallel mode.

    Args:
        target_path: Base target directory (e.g., target/ or target/envs/{env}/).
        run_id: Unique run identifier (timestamp-based).
        project_name: Name of the project being executed.
        verbose: If True, log all nodes; if False, log only failures.
        flags: CLI flags used for this run (for header display).
    """

    def __init__(
        self,
        target_path: Path,
        run_id: str,
        project_name: str,
        verbose: bool = False,
        flags: Optional[List[str]] = None,
    ):
        self.target_path = target_path
        self.run_id = run_id
        self.project_name = project_name
        self.verbose = verbose
        self.flags = flags or []
        self._entries: List[NodeLogEntry] = []
        self._lock = threading.Lock()
        self._start_time = time.time()

    def log_node(
        self,
        node_id: str,
        status: str,
        duration_seconds: float = 0.0,
        error_message: Optional[str] = None,
        output_log: Optional[str] = None,
        row_count: int = 0,
        attempt: Optional[int] = None,
        total_attempts: Optional[int] = None,
    ) -> None:
        """Record a node's execution result. Thread-safe."""
        entry = NodeLogEntry(
            node_id=node_id,
            status=status,
            duration_seconds=duration_seconds,
            error_message=error_message,
            output_log=output_log,
            row_count=row_count,
            attempt=attempt,
            total_attempts=total_attempts,
        )
        with self._lock:
            self._entries.append(entry)

    @property
    def has_failures(self) -> bool:
        """Check if any logged node has failed."""
        return any(e.status.upper() == "FAILED" for e in self._entries)

    @property
    def log_path(self) -> Path:
        """Path where the log file will be written."""
        return self.target_path / "logs" / f"run_{self.run_id}.log"

    def write(self) -> Optional[Path]:
        """Write the log file if conditions are met.

        Returns the path to the written log file, or None if no file was written.

        In default mode, only writes when there are failures.
        In verbose mode, always writes.
        """
        if not self.verbose and not self.has_failures:
            return None

        log_path = self.log_path
        log_path.parent.mkdir(parents=True, exist_ok=True)

        total_duration = time.time() - self._start_time

        lines: List[str] = []

        # Header
        lines.append("=== Seeknal Run Log ===")
        lines.append(f"Run ID:    {self.run_id}")
        lines.append(f"Project:   {self.project_name}")
        lines.append(f"Started:   {datetime.fromtimestamp(self._start_time).isoformat()}")
        if self.flags:
            lines.append(f"Flags:     {' '.join(self.flags)}")
        lines.append("")

        # Determine which entries to include
        entries = self._entries if self.verbose else [
            e for e in self._entries if e.status.upper() == "FAILED"
        ]

        # Node sections
        for entry in entries:
            status_label = entry.status.upper()
            attempt_suffix = ""
            if entry.attempt is not None and entry.total_attempts is not None:
                attempt_suffix = f" (attempt {entry.attempt}/{entry.total_attempts})"

            lines.append(f"--- {entry.node_id} [{status_label}]{attempt_suffix} ---")
            lines.append(f"Duration:  {entry.duration_seconds:.2f}s")

            if entry.row_count > 0:
                lines.append(f"Rows:      {entry.row_count:,}")

            if entry.error_message:
                lines.append(f"Error:     {entry.error_message}")

            if entry.output_log:
                lines.append("")
                lines.append("Subprocess output:")
                for line in entry.output_log.splitlines():
                    lines.append(f"  {line}")

            lines.append("")

        # Summary
        total = len(self._entries)
        failed = sum(1 for e in self._entries if e.status.upper() == "FAILED")
        cached = sum(1 for e in self._entries if e.status.upper() == "CACHED")
        succeeded = sum(1 for e in self._entries if e.status.upper() == "SUCCESS")

        lines.append("=== Summary ===")
        lines.append(f"Total: {total} | Executed: {succeeded} | Cached: {cached} | Failed: {failed}")
        lines.append(f"Duration: {total_duration:.2f}s")

        log_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return log_path
