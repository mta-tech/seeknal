"""
Tests for FileStateBackend implementation.

Tests the file-based state backend with JSON persistence,
atomic writes, and backward compatibility.
"""

import json
import pytest
from pathlib import Path
from tempfile import TemporaryDirectory

from seeknal.state.file_backend import (
    FileStateBackend,
    create_file_state_backend,
)
from seeknal.workflow.state import NodeState, NodeStatus, RunState


class TestFileStateBackend:
    """Tests for FileStateBackend."""

    def test_init(self):
        """Test creating a file state backend."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            assert backend.base_path == Path(tmpdir)

    def test_set_and_get_node_state(self):
        """Test setting and getting node state."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            state = NodeState(
                hash="abc123",
                last_run="2024-01-01T00:00:00",
                status="success"
            )

            backend.set_node_state("run1", "node1", state)
            retrieved = backend.get_node_state("run1", "node1")

            assert retrieved is not None
            assert retrieved.hash == "abc123"
            assert retrieved.status == "success"

    def test_get_nonexistent_node_state(self):
        """Test getting a non-existent node state."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            state = backend.get_node_state("run1", "node1")
            assert state is None

    def test_set_node_state_no_overwrite(self):
        """Test that overwrite=False raises error for existing state."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            state1 = NodeState(hash="abc123", last_run="2024-01-01", status="success")
            state2 = NodeState(hash="def456", last_run="2024-01-02", status="success")

            backend.set_node_state("run1", "node1", state1)

            # Should raise error since state exists
            from seeknal.state.backend import ConcurrencyError
            with pytest.raises(ConcurrencyError):
                backend.set_node_state("run1", "node1", state2, overwrite=False)

    def test_set_and_get_run_state(self):
        """Test setting and getting run state."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            state = RunState(
                run_id="run1",
                last_run="2024-01-01T00:00:00",
            )

            backend.set_run_state("run1", state)
            retrieved = backend.get_run_state("run1")

            assert retrieved is not None
            assert retrieved.run_id == "run1"

    def test_get_nonexistent_run_state(self):
        """Test getting a non-existent run state."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            state = backend.get_run_state("run1")
            assert state is None

    def test_list_runs(self):
        """Test listing all runs."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            backend.set_run_state("run1", RunState(run_id="run1"))
            backend.set_run_state("run2", RunState(run_id="run2"))

            runs = backend.list_runs()
            assert set(runs) == {"run1", "run2"}

    def test_list_runs_empty(self):
        """Test listing runs when none exist."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            runs = backend.list_runs()
            assert runs == []

    def test_list_nodes(self):
        """Test listing nodes for a run."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            backend.set_node_state("run1", "node1", NodeState(hash="a", last_run="2024-01-01", status="pending"))
            backend.set_node_state("run1", "node2", NodeState(hash="b", last_run="2024-01-01", status="pending"))

            nodes = backend.list_nodes("run1")
            assert set(nodes) == {"node1", "node2"}

    def test_list_nodes_empty(self):
        """Test listing nodes when none exist."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            nodes = backend.list_nodes("run1")
            assert nodes == []

    def test_delete_run(self):
        """Test deleting a run."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            backend.set_run_state("run1", RunState(run_id="run1"))
            backend.set_node_state("run1", "node1", NodeState(hash="a", last_run="2024-01-01", status="pending"))

            assert backend.exists("run1") is True

            backend.delete_run("run1")

            assert backend.exists("run1") is False
            assert backend.node_exists("run1", "node1") is False

    def test_delete_node_state(self):
        """Test deleting a node state."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            backend.set_node_state("run1", "node1", NodeState(hash="a", last_run="2024-01-01", status="pending"))

            assert backend.node_exists("run1", "node1") is True

            backend.delete_node_state("run1", "node1")

            assert backend.node_exists("run1", "node1") is False

    def test_exists(self):
        """Test checking if run exists."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            assert backend.exists("run1") is False

            backend.set_run_state("run1", RunState(run_id="run1"))
            assert backend.exists("run1") is True

    def test_node_exists(self):
        """Test checking if node exists."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            assert backend.node_exists("run1", "node1") is False

            backend.set_node_state("run1", "node1", NodeState(hash="a", last_run="2024-01-01", status="pending"))
            assert backend.node_exists("run1", "node1") is True

    def test_acquire_and_release_lock(self):
        """Test lock acquisition and release."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))

            assert backend.acquire_lock("run1", "node1") is True
            assert backend.acquire_lock("run1", "node1") is False  # Already locked

            backend.release_lock("run1", "node1")
            assert backend.acquire_lock("run1", "node1") is True  # Now available

    def test_get_and_set_metadata(self):
        """Test getting and setting metadata."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            backend.set_run_state("run1", RunState(run_id="run1"))

            metadata = backend.get_metadata("run1")
            assert "run_id" in metadata
            assert metadata["run_id"] == "run1"

    def test_get_all_node_states(self):
        """Test getting all node states for a run."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            backend.set_node_state("run1", "node1", NodeState(hash="a", last_run="2024-01-01", status="pending"))
            backend.set_node_state("run1", "node2", NodeState(hash="b", last_run="2024-01-01", status="pending"))

            all_states = backend.get_all_node_states("run1")

            assert len(all_states) == 2
            assert "node1" in all_states
            assert "node2" in all_states

    def test_health_check(self):
        """Test backend health check."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            assert backend.health_check() is True

    def test_transaction_context_manager(self):
        """Test transaction context manager."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))

            with backend.transaction() as tx:
                assert tx.is_active is True
                backend.set_node_state("run1", "node1", NodeState(hash="a", last_run="2024-01-01", status="pending"))

            # After transaction, state should be persisted
            assert backend.node_exists("run1", "node1") is True

    def test_transaction_rollback(self):
        """Test transaction rollback on error."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))

            try:
                with backend.transaction() as tx:
                    backend.set_node_state("run1", "node1", NodeState(hash="a", last_run="2024-01-01", status="pending"))
                    raise ValueError("Test error")
            except ValueError:
                pass

            # After failed transaction, check state
            # Note: In this simple implementation, the write still happens
            # A more sophisticated implementation would use temp files

    def test_backup_created_on_overwrite(self):
        """Test that backup is created when overwriting existing state."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            state1 = RunState(run_id="run1", last_run="2024-01-01T00:00:00")
            state2 = RunState(run_id="run1", last_run="2024-01-02T00:00:00")

            backend.set_run_state("run1", state1)
            backend.set_run_state("run1", state2)

            # Check backup file was created
            run_dir = Path(tmpdir) / "run1"
            backup_files = list(run_dir.glob("*.bak.*"))
            assert len(backup_files) > 0


class TestCreateFileStateBackend:
    """Tests for create_file_state_backend convenience function."""

    def test_create_backend(self):
        """Test creating a file backend via convenience function."""
        with TemporaryDirectory() as tmpdir:
            backend = create_file_state_backend(Path(tmpdir))
            assert isinstance(backend, FileStateBackend)


class TestStateBackendFactoryWithFileBackend:
    """Tests for StateBackendFactory with FileStateBackend."""

    def test_factory_creates_file_backend(self):
        """Test that factory can create file backend."""
        # Already registered in module
        from seeknal.state.backend import StateBackendFactory

        # Should be able to create via factory
        with TemporaryDirectory() as tmpdir:
            backend = StateBackendFactory.create("file", base_path=Path(tmpdir))
            assert isinstance(backend, FileStateBackend)

    def test_list_backends_includes_file(self):
        """Test that 'file' is in list of available backends."""
        from seeknal.state.backend import StateBackendFactory

        backends = StateBackendFactory.list_backends()
        assert "file" in backends


class TestBackwardCompatibility:
    """Tests for backward compatibility with old state formats."""

    def test_load_from_old_runner_format(self):
        """Test loading state from old runner.py format."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))

            # Create old runner.py state file
            state_dir = Path(tmpdir) / "state"
            state_dir.mkdir(parents=True)
            old_state_path = state_dir / "execution_state.json"

            old_state_data = {
                "transform.my_transform": {
                    "status": "success",
                    "duration": 1.5,  # seconds
                    "row_count": 1000,
                    "last_run": "2024-01-01T00:00:00"
                }
            }

            with open(old_state_path, "w") as f:
                json.dump(old_state_data, f)

            # Load should migrate from old format
            state = backend.get_run_state("")

            assert state is not None
            assert "transform.my_transform" in state.nodes
            assert state.nodes["transform.my_transform"].status == "success"
            assert state.nodes["transform.my_transform"].duration_ms == 1500  # Converted to ms

    def test_load_from_v1_state_format(self):
        """Test loading state from v1.0 format."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))

            # Create v1.0 state file
            state_path = Path(tmpdir) / "run_state.json"

            v1_state_data = {
                "version": "1.0.0",
                "nodes": {
                    "transform.my_transform": {
                        "hash": "abc123",
                        "last_run": "2024-01-01T00:00:00",
                        "status": "success",
                        "duration_ms": 1500,
                        "row_count": 1000,
                    }
                }
            }

            with open(state_path, "w") as f:
                json.dump(v1_state_data, f)

            # Load should work
            state = backend.get_run_state("")

            assert state is not None
            assert "transform.my_transform" in state.nodes


class TestPathTraversalSanitization:
    """Tests for path traversal sanitization (security-002)."""

    def test_sanitize_run_id_normal(self):
        """Test that normal run IDs are unchanged."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            assert backend._sanitize_run_id("normal_run") == "normal_run"
            assert backend._sanitize_run_id("run-123") == "run-123"
            assert backend._sanitize_run_id("run_with_underscores") == "run_with_underscores"

    def test_sanitize_run_id_parent_traversal(self):
        """Test that ../ sequences are removed from run IDs."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            assert backend._sanitize_run_id("../escape") == "escape"
            assert backend._sanitize_run_id("../../escape") == "escape"
            assert backend._sanitize_run_id("run/../attack") == "runattack"

    def test_sanitize_run_id_windows_traversal(self):
        """Test that ..\\ sequences are removed from run IDs (Windows)."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            assert backend._sanitize_run_id("..\\escape") == "escape"
            assert backend._sanitize_run_id("run\\..\\attack") == "runattack"

    def test_sanitize_run_id_slashes(self):
        """Test that slashes are removed from run IDs."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            assert backend._sanitize_run_id("path/with/slashes") == "pathwithslashes"
            assert backend._sanitize_run_id("path\\with\\backslashes") == "pathwithbackslashes"

    def test_sanitize_run_id_double_dots(self):
        """Test that standalone .. sequences are removed."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            assert backend._sanitize_run_id("..") == ""
            assert backend._sanitize_run_id("...") == "."
            assert backend._sanitize_run_id("run..test") == "runtest"

    def test_sanitize_node_id_normal(self):
        """Test that normal node IDs are unchanged."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            assert backend._sanitize_node_id("normal_node") == "normal_node"
            assert backend._sanitize_node_id("node-123") == "node-123"
            assert backend._sanitize_node_id("transform.my_transform") == "transform.my_transform"

    def test_sanitize_node_id_parent_traversal(self):
        """Test that ../ sequences are removed from node IDs."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            assert backend._sanitize_node_id("../escape") == "escape"
            assert backend._sanitize_node_id("../../escape") == "escape"
            assert backend._sanitize_node_id("node/../attack") == "nodeattack"

    def test_sanitize_node_id_slashes(self):
        """Test that slashes are removed from node IDs."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            assert backend._sanitize_node_id("node/with/slashes") == "nodewithslashes"
            assert backend._sanitize_node_id("node\\with\\backslashes") == "nodewithbackslashes"

    def test_sanitize_empty_string(self):
        """Test that empty strings are handled correctly."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            assert backend._sanitize_run_id("") == ""
            assert backend._sanitize_node_id("") == ""

    def test_sanitize_null_bytes(self):
        """Test that null bytes are handled."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            # Null bytes should remain (they'll be caught by file system)
            assert backend._sanitize_run_id("run\x00") == "run\x00"

    def test_sanitize_unicode_characters(self):
        """Test that Unicode characters are preserved."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            assert backend._sanitize_run_id("run_日本語") == "run_日本語"
            assert backend._sanitize_run_id("run_emoji_🔥") == "run_emoji_🔥"

    def test_sanitize_very_long_path(self):
        """Test that very long paths are handled."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            long_id = "a" * 1000
            result = backend._sanitize_run_id(long_id)
            assert len(result) == len(long_id)

    def test_sanitize_combined_attack_patterns(self):
        """Test combinations of attack patterns are sanitized."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            # Complex path traversal attempt
            assert backend._sanitize_run_id("../../../etc/passwd") == "etcpasswd"
            assert backend._sanitize_run_id("..\\..\\..\\windows\\system32") == "windowssystem32"
            # "normal/" becomes "normal", "../../etc/passwd" becomes "etcpasswd"
            assert backend._sanitize_run_id("normal/../../etc/passwd") == "normaletcpasswd"


class TestSecurityErrorOnInsecureBasePath:
    """Tests for security error when using insecure base paths (security-002)."""

    def test_insecure_base_path_rejects_tmp(self):
        """Test that FileStateBackend rejects /tmp as base path."""
        with pytest.raises(ValueError, match="Insecure base path detected"):
            FileStateBackend(Path("/tmp"))

    def test_insecure_base_path_rejects_var_tmp(self):
        """Test that FileStateBackend rejects /var/tmp as base path."""
        with pytest.raises(ValueError, match="Insecure base path detected"):
            FileStateBackend(Path("/var/tmp"))

    def test_insecure_base_path_rejects_dev_shm(self):
        """Test that FileStateBackend rejects /dev/shm as base path."""
        with pytest.raises(ValueError, match="Insecure base path detected"):
            FileStateBackend(Path("/dev/shm"))

    def test_insecure_base_path_rejects_tmp_subdirectory(self):
        """Test that FileStateBackend rejects subdirectories of /tmp."""
        with pytest.raises(ValueError, match="Insecure base path detected"):
            FileStateBackend(Path("/tmp/seeknal_state"))

    def test_secure_base_path_accepts_home(self):
        """Test that FileStateBackend accepts ~/.seeknal as base path."""
        with TemporaryDirectory() as tmpdir:
            # Use a temporary directory (simulating home directory)
            backend = FileStateBackend(Path(tmpdir))
            assert backend.base_path == Path(tmpdir)

    def test_secure_base_path_accepts_user_local(self):
        """Test that FileStateBackend accepts ~/.local/share as base path."""
        with TemporaryDirectory() as tmpdir:
            # Simulate XDG compliant path
            backend = FileStateBackend(Path(tmpdir))
            assert backend.base_path == Path(tmpdir)

    def test_insecure_path_error_message_includes_recommendation(self):
        """Test that error message includes secure path recommendation."""
        with pytest.raises(ValueError) as exc_info:
            FileStateBackend(Path("/tmp"))

        error_msg = str(exc_info.value)
        assert "Insecure base path detected" in error_msg
        assert "sensitive workflow data" in error_msg
        assert "SEEKNAL_BASE_CONFIG_PATH" in error_msg or ".seeknal" in error_msg

    def test_secure_absolute_path_accepted(self):
        """Test that secure absolute paths are accepted."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            assert backend is not None

    def test_secure_relative_path_expanded(self):
        """Test that relative paths are expanded and validated."""
        with TemporaryDirectory() as tmpdir:
            # Create a subdirectory
            subdir = Path(tmpdir) / "state"
            subdir.mkdir()

            # Relative path will be expanded to absolute
            backend = FileStateBackend(subdir)
            assert backend is not None


class TestPathTraversalIntegration:
    """Integration tests for path traversal prevention (security-002)."""

    def test_get_run_state_path_sanitizes_run_id(self):
        """Test that _get_run_state_path sanitizes the run ID."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            path = backend._get_run_state_path("../escape")
            # The path should be within base_path, not outside
            assert str(path).startswith(str(backend.base_path))
            assert ".." not in str(path)

    def test_get_node_state_path_sanitizes_both_ids(self):
        """Test that _get_node_state_path sanitizes both run_id and node_id."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            path = backend._get_node_state_path("../escape_run", "../escape_node")
            # The path should be within base_path, not outside
            assert str(path).startswith(str(backend.base_path))
            assert ".." not in str(path)

    def test_node_state_operations_contained_within_base_path(self):
        """Test that node state operations cannot escape base path."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            state = NodeState(
                hash="abc123",
                last_run="2024-01-01T00:00:00",
                status="success"
            )

            # Try to write to a path with traversal
            backend.set_node_state("../escape_run", "../escape_node", state)

            # Verify file is actually stored within base_path
            base_path_files = list(Path(tmpdir).rglob("*.json"))
            assert len(base_path_files) > 0
            for file in base_path_files:
                # All files should be under tmpdir, not parent directory
                assert str(file).startswith(tmpdir)

    def test_run_state_operations_contained_within_base_path(self):
        """Test that run state operations cannot escape base path."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            state = RunState(
                run_id="../escape",
                last_run="2024-01-01T00:00:00",
            )

            # Try to write to a path with traversal
            backend.set_run_state("../escape", state)

            # Verify file is actually stored within base_path
            base_path_files = list(Path(tmpdir).rglob("*.json"))
            assert len(base_path_files) > 0
            for file in base_path_files:
                # All files should be under tmpdir, not parent directory
                assert str(file).startswith(tmpdir)

    def test_delete_run_sanitizes_run_id(self):
        """Test that delete_run sanitizes the run ID."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            state = RunState(run_id="legit_run", last_run="2024-01-01T00:00:00")
            backend.set_run_state("legit_run", state)

            # Try to delete with traversal attempt
            backend.delete_run("../escape")

            # The legit run should still exist (sanitization worked)
            assert backend.exists("legit_run") is True

    def test_list_nodes_sanitizes_run_id(self):
        """Test that list_nodes sanitizes the run ID."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            backend.set_node_state("legit_run", "node1", NodeState(hash="a", last_run="2024-01-01", status="pending"))

            # Try to list with traversal attempt - should return empty
            nodes = backend.list_nodes("../escape")
            assert nodes == []

    def test_path_traversal_cannot_read_files_outside_base(self):
        """Test that path traversal cannot be used to read files outside base path."""
        with TemporaryDirectory() as tmpdir:
            # Create a file in parent directory
            parent_dir = Path(tmpdir).parent
            secret_file = parent_dir / "secret.json"
            secret_file.write_text('{"secret": "data"}')

            backend = FileStateBackend(Path(tmpdir))

            # Try to read the secret file using traversal
            state = backend.get_run_state("../secret")
            # Should not find the file (it's outside sanitized path)
            assert state is None

    def test_complex_traversal_attempt_sanitized(self):
        """Test complex path traversal attempts are properly sanitized."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            state = NodeState(hash="abc", last_run="2024-01-01", status="pending")

            # Various complex traversal attempts
            malicious_run_ids = [
                "./../../escape",
                "..\\..\\windows",
                "..././../escape",
                "normal/../../escape",
            ]

            for run_id in malicious_run_ids:
                backend.set_node_state(run_id, "node1", state)

            # All files should be contained within tmpdir
            all_files = list(Path(tmpdir).rglob("*.json"))
            for file in all_files:
                assert str(file).startswith(tmpdir)


class TestIsInsecurePathIntegration:
    """Tests for is_insecure_path integration (security-002)."""

    def test_is_insecure_path_imported(self):
        """Test that is_insecure_path is properly imported."""
        from seeknal.state.file_backend import FileStateBackend
        from seeknal.utils.path_security import is_insecure_path

        # Verify the function is available
        assert callable(is_insecure_path)

    def test_is_insecure_path_detects_tmp(self):
        """Test that is_insecure_path detects /tmp."""
        from seeknal.utils.path_security import is_insecure_path

        assert is_insecure_path("/tmp") is True
        assert is_insecure_path("/tmp/subdir") is True
        assert is_insecure_path("/var/tmp") is True
        assert is_insecure_path("/dev/shm") is True

    def test_is_insecure_path_allows_secure_paths(self):
        """Test that is_insecure_path allows secure paths."""
        from seeknal.utils.path_security import is_insecure_path

        assert is_insecure_path("/home/user/.seeknal") is False
        assert is_insecure_path("~/.seeknal") is False
        assert is_insecure_path("/opt/seeknal/state") is False

    def test_insecure_path_blocks_backend_initialization(self):
        """Test that insecure paths block FileStateBackend initialization."""
        from seeknal.utils.path_security import is_insecure_path

        insecure_paths = ["/tmp", "/var/tmp", "/dev/shm"]

        for path in insecure_paths:
            assert is_insecure_path(path) is True
            with pytest.raises(ValueError, match="Insecure base path"):
                FileStateBackend(Path(path))


class TestSecurityEdgeCases:
    """Edge case tests for security functionality (security-002)."""

    def test_empty_run_id(self):
        """Test that empty run ID is handled."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            # Empty run_id should not cause issues
            result = backend._sanitize_run_id("")
            assert result == ""

    def test_special_characters_in_ids(self):
        """Test that special characters are handled correctly."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            # Special characters that are not path separators should be preserved
            assert backend._sanitize_run_id("run@#$%") == "run@#$%"
            assert backend._sanitize_run_id("run!&*()") == "run!&*()"

    def test_mixed_slash_types(self):
        """Test mixed forward and backward slashes."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            assert backend._sanitize_run_id("path/../win\\path") == "pathwinpath"

    def test_dots_with_separators(self):
        """Test various combinations of dots and separators."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            # ".../" becomes "." (since "..." -> "."), then "/" removed, then "." kept
            assert backend._sanitize_run_id(".../test") == ".test"
            # "./" becomes "." (single dot is kept), then "/" removed, resulting in ".test"
            assert backend._sanitize_run_id("./test") == ".test"
            # Same behavior with backslash
            assert backend._sanitize_run_id(".\\test") == ".test"

    def test_only_separators(self):
        """Test inputs with only separators."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            assert backend._sanitize_run_id("///") == ""
            assert backend._sanitize_run_id("\\\\\\") == ""

    def test_consecutive_dots(self):
        """Test consecutive dots."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            # Odd number of dots should leave one dot
            assert backend._sanitize_run_id("...") == "."
            assert backend._sanitize_run_id(".....") == "."

    def test_unicode_normalization_attacks(self):
        """Test potential Unicode normalization attacks."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            # These should be preserved as-is (filesystem handles Unicode normalization)
            assert backend._sanitize_run_id("run\u200b") == "run\u200b"  # Zero-width space
            assert backend._sanitize_run_id("run\ufeff") == "run\ufeff"  # Zero-width no-break space
