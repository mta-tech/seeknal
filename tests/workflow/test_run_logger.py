"""Tests for RunLogger — per-run log file writer."""

import threading
import time
from pathlib import Path

import pytest

from seeknal.workflow.run_logger import RunLogger, NodeLogEntry


@pytest.fixture
def tmp_target(tmp_path):
    """Create a temporary target directory."""
    return tmp_path / "target"


class TestRunLogger:
    """Tests for RunLogger class."""

    def test_log_node_records_entry(self, tmp_target):
        logger = RunLogger(tmp_target, "20260228_120000", "test-project")
        logger.log_node("transform.clean", "SUCCESS", duration_seconds=1.5, row_count=100)

        assert len(logger._entries) == 1
        assert logger._entries[0].node_id == "transform.clean"
        assert logger._entries[0].status == "SUCCESS"
        assert logger._entries[0].row_count == 100

    def test_has_failures_false_when_no_failures(self, tmp_target):
        logger = RunLogger(tmp_target, "20260228_120000", "test-project")
        logger.log_node("source.data", "CACHED")
        logger.log_node("transform.clean", "SUCCESS", duration_seconds=1.0)

        assert logger.has_failures is False

    def test_has_failures_true_when_failure_exists(self, tmp_target):
        logger = RunLogger(tmp_target, "20260228_120000", "test-project")
        logger.log_node("transform.clean", "SUCCESS", duration_seconds=1.0)
        logger.log_node("transform.model", "FAILED", error_message="No module named 'sklearn'")

        assert logger.has_failures is True

    def test_log_path(self, tmp_target):
        logger = RunLogger(tmp_target, "20260228_120000", "test-project")
        assert logger.log_path == tmp_target / "logs" / "run_20260228_120000.log"

    def test_write_skips_when_no_failures_default_mode(self, tmp_target):
        logger = RunLogger(tmp_target, "20260228_120000", "test-project", verbose=False)
        logger.log_node("transform.clean", "SUCCESS", duration_seconds=1.0)

        result = logger.write()
        assert result is None
        assert not logger.log_path.exists()

    def test_write_creates_file_on_failure_default_mode(self, tmp_target):
        logger = RunLogger(tmp_target, "20260228_120000", "test-project", verbose=False)
        logger.log_node("transform.clean", "SUCCESS", duration_seconds=1.0)
        logger.log_node("transform.model", "FAILED", error_message="ImportError: sklearn")

        result = logger.write()
        assert result == logger.log_path
        assert result.exists()

        content = result.read_text()
        assert "=== Seeknal Run Log ===" in content
        assert "Run ID:    20260228_120000" in content
        assert "Project:   test-project" in content
        # Default mode: only FAILED nodes in the log
        assert "transform.model [FAILED]" in content
        assert "ImportError: sklearn" in content
        # SUCCESS nodes NOT included in default mode
        assert "transform.clean [SUCCESS]" not in content

    def test_write_creates_file_always_in_verbose_mode(self, tmp_target):
        logger = RunLogger(tmp_target, "20260228_120000", "test-project", verbose=True)
        logger.log_node("source.data", "CACHED")
        logger.log_node("transform.clean", "SUCCESS", duration_seconds=1.0, row_count=50)

        result = logger.write()
        assert result is not None
        assert result.exists()

        content = result.read_text()
        assert "source.data [CACHED]" in content
        assert "transform.clean [SUCCESS]" in content
        assert "Rows:      50" in content

    def test_write_includes_all_nodes_in_verbose_mode(self, tmp_target):
        logger = RunLogger(tmp_target, "20260228_120000", "test-project", verbose=True)
        logger.log_node("source.data", "CACHED")
        logger.log_node("transform.clean", "SUCCESS", duration_seconds=1.5)
        logger.log_node("transform.model", "FAILED", error_message="timeout")

        result = logger.write()
        content = result.read_text()

        assert "source.data [CACHED]" in content
        assert "transform.clean [SUCCESS]" in content
        assert "transform.model [FAILED]" in content

    def test_write_includes_subprocess_output(self, tmp_target):
        logger = RunLogger(tmp_target, "20260228_120000", "test-project", verbose=False)
        logger.log_node(
            "transform.model", "FAILED",
            error_message="Process exited with code 1",
            output_log="stderr:\nModuleNotFoundError: No module named 'sklearn'\n",
        )

        result = logger.write()
        content = result.read_text()
        assert "Subprocess output:" in content
        assert "ModuleNotFoundError" in content

    def test_write_includes_flags(self, tmp_target):
        logger = RunLogger(
            tmp_target, "20260228_120000", "test-project",
            verbose=True, flags=["--verbose", "--full"]
        )
        logger.log_node("source.data", "CACHED")

        result = logger.write()
        content = result.read_text()
        assert "Flags:     --verbose --full" in content

    def test_write_includes_summary(self, tmp_target):
        logger = RunLogger(tmp_target, "20260228_120000", "test-project", verbose=True)
        logger.log_node("source.data", "CACHED")
        logger.log_node("transform.clean", "SUCCESS", duration_seconds=1.0)
        logger.log_node("transform.model", "FAILED", error_message="error")

        result = logger.write()
        content = result.read_text()
        assert "=== Summary ===" in content
        assert "Total: 3" in content
        assert "Executed: 1" in content
        assert "Cached: 1" in content
        assert "Failed: 1" in content

    def test_write_retry_attempts(self, tmp_target):
        logger = RunLogger(tmp_target, "20260228_120000", "test-project", verbose=True)
        logger.log_node(
            "transform.model", "FAILED",
            error_message="Connection refused",
            attempt=1, total_attempts=3,
        )
        logger.log_node(
            "transform.model", "FAILED",
            error_message="Connection refused",
            attempt=2, total_attempts=3,
        )
        logger.log_node(
            "transform.model", "SUCCESS",
            duration_seconds=2.0,
            attempt=3, total_attempts=3,
        )

        result = logger.write()
        content = result.read_text()
        assert "(attempt 1/3)" in content
        assert "(attempt 2/3)" in content
        assert "(attempt 3/3)" in content

    def test_thread_safety(self, tmp_target):
        """Log nodes from multiple threads concurrently."""
        logger = RunLogger(tmp_target, "20260228_120000", "test-project", verbose=True)
        errors = []

        def log_from_thread(thread_id):
            try:
                for i in range(10):
                    logger.log_node(
                        f"node.thread{thread_id}_{i}",
                        "SUCCESS",
                        duration_seconds=0.01,
                    )
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=log_from_thread, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert len(logger._entries) == 50  # 5 threads × 10 nodes

        result = logger.write()
        assert result is not None
        assert result.exists()

    def test_creates_log_directory(self, tmp_target):
        """Log directory is created automatically."""
        logger = RunLogger(tmp_target, "20260228_120000", "test-project", verbose=True)
        logger.log_node("source.data", "CACHED")

        assert not (tmp_target / "logs").exists()
        logger.write()
        assert (tmp_target / "logs").exists()


class TestPythonExecutorErrorMessage:
    """Tests for PythonExecutor error_message fix."""

    def test_executor_result_has_output_log_field(self):
        from seeknal.workflow.executors.base import ExecutorResult, ExecutionStatus

        result = ExecutorResult(
            node_id="transform.test",
            status=ExecutionStatus.FAILED,
            error_message="Process exited with code 1: ImportError",
            output_log="stderr:\nImportError: no module named 'foo'",
        )
        assert result.output_log is not None
        assert "ImportError" in result.output_log

    def test_output_log_excluded_from_to_dict(self):
        from seeknal.workflow.executors.base import ExecutorResult, ExecutionStatus

        result = ExecutorResult(
            node_id="transform.test",
            status=ExecutionStatus.FAILED,
            error_message="error",
            output_log="full output here",
        )
        d = result.to_dict()
        assert "output_log" not in d
        assert d["error_message"] == "error"

    def test_output_log_default_none(self):
        from seeknal.workflow.executors.base import ExecutorResult, ExecutionStatus

        result = ExecutorResult(
            node_id="transform.test",
            status=ExecutionStatus.SUCCESS,
        )
        assert result.output_log is None
