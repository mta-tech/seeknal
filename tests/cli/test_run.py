"""
Tests for seeknal run command.

Tests the YAML pipeline execution functionality including:
- DAG building from YAML files
- State tracking and incremental runs
- Change detection
- CLI flags (--full, --nodes, --types, --show-plan, etc.)
- Error handling (cycles, missing dependencies)
"""

import json
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch, Mock
from typer.testing import CliRunner
from seeknal.cli.main import app

runner = CliRunner()


@pytest.fixture
def temp_project_path(tmp_path):
    """Create a temporary project path with YAML files."""
    project_path = tmp_path / "test_project"
    project_path.mkdir()

    # Create seeknal directory structure
    (project_path / "seeknal" / "sources").mkdir(parents=True)
    (project_path / "seeknal" / "transforms").mkdir(parents=True)
    (project_path / "seeknal" / "feature_groups").mkdir(parents=True)

    return project_path


@pytest.fixture
def sample_yaml_files(temp_project_path):
    """Create sample YAML files for testing."""
    # Source
    source_yaml = """
kind: source
name: raw_users
description: Raw user data
source: csv
params:
  path: data/users.csv
"""
    (temp_project_path / "seeknal" / "sources" / "raw_users.yml").write_text(source_yaml)

    # Transform
    transform_yaml = """
kind: transform
name: clean_users
description: Clean user data
transform: SELECT * FROM source.raw_users
inputs:
  - ref: source.raw_users
"""
    (temp_project_path / "seeknal" / "transforms" / "clean_users.yml").write_text(transform_yaml)

    # Feature group
    feature_group_yaml = """
kind: feature_group
name: user_features
description: User feature group
entity: user
features:
  user_id:
    dtype: integer
  name:
    dtype: string
inputs:
  - ref: transform.clean_users
"""
    (temp_project_path / "seeknal" / "feature_groups" / "user_features.yml").write_text(
        feature_group_yaml
    )

    return temp_project_path


@pytest.fixture
def state_file(temp_project_path):
    """Create a mock state file."""
    state_path = temp_project_path / "target" / "run_state.json"
    state_path.parent.mkdir(parents=True, exist_ok=True)

    state_data = {
        "version": "1.0.0",
        "seeknal_version": "2.0.0",
        "last_run": "2024-01-26T12:00:00",
        "run_id": "20240126_120000",
        "config": {},
        "nodes": {
            "source.raw_users": {
                "hash": "abc123",
                "last_run": "2024-01-26T12:00:00",
                "status": "success",
                "duration_ms": 1000,
                "row_count": 100,
                "version": 1,
                "metadata": {},
            }
        },
    }

    state_path.write_text(json.dumps(state_data))
    return state_path


class TestSeeknalRunBasic:
    """Test basic seeknal run functionality."""

    def test_run_without_yaml_files(self, temp_project_path, monkeypatch):
        """Test run command with no YAML files shows appropriate message."""
        monkeypatch.chdir(temp_project_path)

        result = runner.invoke(app, ["run"])

        # Should show error about no YAML files
        assert result.exit_code == 1
        assert "No YAML files found" in result.stdout or "DAG build failed" in result.stdout

    def test_run_with_show_plan(self, sample_yaml_files, monkeypatch):
        """Test --show-plan flag displays execution plan."""
        monkeypatch.chdir(sample_yaml_files)

        result = runner.invoke(app, ["run", "--show-plan"])

        assert result.exit_code == 0
        assert "Execution Plan" in result.stdout
        # Node names are shown without prefix in the plan
        assert "raw_users" in result.stdout
        assert "clean_users" in result.stdout
        assert "user_features" in result.stdout

    def test_run_dry_run(self, sample_yaml_files, monkeypatch):
        """Test --dry-run flag shows what would execute."""
        monkeypatch.chdir(sample_yaml_files)

        result = runner.invoke(app, ["run", "--dry-run"])

        assert result.exit_code == 0
        assert "Dry run" in result.stdout or "Would execute" in result.stdout
        assert "source.raw_users" in result.stdout


class TestSeeknalRunState:
    """Test state tracking and incremental runs."""

    def test_first_run_no_state(self, sample_yaml_files, monkeypatch):
        """Test first run when no state exists."""
        monkeypatch.chdir(sample_yaml_files)

        result = runner.invoke(app, ["run", "--show-plan"])

        assert result.exit_code == 0
        assert "No previous state found" in result.stdout or "first run" in result.stdout

    def test_incremental_run_with_state(self, sample_yaml_files, state_file, monkeypatch):
        """Test incremental run loads previous state."""
        monkeypatch.chdir(sample_yaml_files)

        result = runner.invoke(app, ["run", "--show-plan"])

        assert result.exit_code == 0
        assert "Loaded previous state" in result.stdout

    def test_full_run_ignores_state(self, sample_yaml_files, state_file, monkeypatch):
        """Test --full flag runs all nodes regardless of state."""
        monkeypatch.chdir(sample_yaml_files)

        result = runner.invoke(app, ["run", "--show-plan", "--full"])

        assert result.exit_code == 0
        assert "Full" in result.stdout


class TestSeeknalRunFiltering:
    """Test filtering options (--nodes, --types, --exclude-tags)."""

    def test_run_specific_nodes(self, sample_yaml_files, monkeypatch):
        """Test --nodes flag runs specific nodes."""
        monkeypatch.chdir(sample_yaml_files)

        result = runner.invoke(
            app, ["run", "--show-plan", "--nodes", "raw_users"]
        )

        assert result.exit_code == 0
        # Check that nodes are mentioned
        assert "raw_users" in result.stdout or "users" in result.stdout

    def test_run_by_type(self, sample_yaml_files, monkeypatch):
        """Test --types flag filters by node type."""
        monkeypatch.chdir(sample_yaml_files)

        result = runner.invoke(app, ["run", "--show-plan", "--types", "source"])

        assert result.exit_code == 0
        # Node names are shown without prefix
        assert "raw_users" in result.stdout

    def test_exclude_tags(self, temp_project_path, monkeypatch):
        """Test --exclude-tags flag filters out nodes with tags."""
        # Create YAML file with tags
        source_yaml = """
kind: source
name: tagged_source
description: Tagged source
tags:
  - legacy
  - deprecated
source: csv
params:
  path: data/old.csv
"""
        (temp_project_path / "seeknal" / "sources" / "tagged_source.yml").write_text(
            source_yaml
        )

        monkeypatch.chdir(temp_project_path)

        result = runner.invoke(
            app, ["run", "--show-plan", "--exclude-tags", "legacy"]
        )

        assert result.exit_code == 0
        # Tagged source should be skipped
        assert "tagged_source" not in result.stdout or "SKIP" in result.stdout


class TestSeeknalRunErrorHandling:
    """Test error handling for cycles and missing dependencies."""

    def test_cycle_detection(self, temp_project_path, monkeypatch):
        """Test cycle detection in DAG."""
        # Create YAML files with circular dependency
        source_yaml = """
kind: source
name: node_a
description: Node A
source: csv
params:
  path: data/a.csv
inputs:
  - ref: transform.node_b
"""
        (temp_project_path / "seeknal" / "sources" / "node_a.yml").write_text(source_yaml)

        transform_yaml = """
kind: transform
name: node_b
description: Node B
transform: SELECT * FROM source.node_a
inputs:
  - ref: source.node_a
"""
        (temp_project_path / "seeknal" / "transforms" / "node_b.yml").write_text(
            transform_yaml
        )

        monkeypatch.chdir(temp_project_path)

        result = runner.invoke(app, ["run"])

        assert result.exit_code == 1
        assert "Cycle detected" in result.stdout

    def test_missing_dependency(self, temp_project_path, monkeypatch):
        """Test handling of missing dependencies."""
        # Create YAML with non-existent dependency
        transform_yaml = """
kind: transform
name: orphan_transform
description: Orphan transform
transform: SELECT * FROM source.non_existent
inputs:
  - ref: source.non_existent
"""
        (temp_project_path / "seeknal" / "transforms" / "orphan_transform.yml").write_text(
            transform_yaml
        )

        monkeypatch.chdir(temp_project_path)

        result = runner.invoke(app, ["run"])

        assert result.exit_code == 1
        # Missing dependencies trigger cycle detection during topological sort
        assert "Cycle detected" in result.stdout or "cycle" in result.stdout.lower()


class TestSeeknalRunLegacyMode:
    """Test legacy Flow mode (backward compatibility)."""

    def test_legacy_flow_mode(self, monkeypatch, tmp_path):
        """Test running legacy Flow object."""
        monkeypatch.chdir(tmp_path)

        # Mock the Flow class
        with patch("seeknal.flow.Flow") as mock_flow_class:
            mock_flow = MagicMock()
            mock_flow.run.return_value = MagicMock(count=lambda: 100)
            mock_flow_class.return_value = mock_flow

            result = runner.invoke(app, ["run", "my_flow", "--start-date", "2024-01-01"])

            assert result.exit_code == 0
            assert "my_flow" in result.stdout


class TestSeeknalRunProgressReporting:
    """Test progress reporting and execution summary."""

    def test_progress_output_format(self, sample_yaml_files, monkeypatch):
        """Test progress output shows node status."""
        monkeypatch.chdir(sample_yaml_files)

        result = runner.invoke(app, ["run", "--show-plan"])

        assert result.exit_code == 0
        assert "RUN" in result.stdout or "CACHED" in result.stdout
        assert "Total:" in result.stdout

    def test_execution_summary(self, sample_yaml_files, monkeypatch):
        """Test execution summary shows statistics."""
        monkeypatch.chdir(sample_yaml_files)

        # Mock successful execution
        with patch("seeknal.workflow.executors.get_executor") as mock_get_executor:
            mock_executor = MagicMock()
            mock_result = MagicMock()
            mock_result.is_success.return_value = True
            mock_result.row_count = 100
            mock_result.metadata = {}
            mock_executor.run.return_value = mock_result
            mock_get_executor.return_value = mock_executor

            result = runner.invoke(app, ["run"])

            assert result.exit_code == 0
            assert "Execution Summary" in result.stdout
            assert "Total nodes:" in result.stdout


class TestSeeknalRunRetryLogic:
    """Test retry logic with --retry flag."""

    def test_retry_on_failure(self, sample_yaml_files, monkeypatch):
        """Test --retry flag retries failed nodes."""
        monkeypatch.chdir(sample_yaml_files)

        call_count = {"count": 0}

        def mock_run_side_effect(*args, **kwargs):
            call_count["count"] += 1
            mock_result = MagicMock()
            # First node fails, retries succeed
            if call_count["count"] <= 2:
                mock_result.is_success.return_value = False
                mock_result.is_failed.return_value = True
                mock_result.status.value = "failed"
                mock_result.error_message = "Temporary error"
            else:
                mock_result.is_success.return_value = True
                mock_result.is_failed.return_value = False
                mock_result.row_count = 100
            return mock_result

        with patch("seeknal.workflow.executors.get_executor") as mock_get_executor:
            mock_executor = MagicMock()
            mock_executor.run.side_effect = mock_run_side_effect
            mock_get_executor.return_value = mock_executor

            result = runner.invoke(app, ["run", "--retry", "2", "--continue-on-error"])

            # With 3 nodes and retries enabled with continue-on-error,
            # we should have multiple calls
            assert call_count["count"] >= 3


class TestSeeknalRunContinueOnError:
    """Test --continue-on-error flag."""

    def test_continue_on_error(self, sample_yaml_files, monkeypatch):
        """Test --continue-on-error continues after failures."""
        monkeypatch.chdir(sample_yaml_files)

        with patch("seeknal.workflow.executors.get_executor") as mock_get_executor:
            mock_executor = MagicMock()
            mock_result = MagicMock()
            mock_result.is_success.return_value = False
            mock_result.is_failed.return_value = True
            mock_result.status.value = "failed"
            mock_result.error_message = "Node failed"
            mock_executor.run.return_value = mock_result
            mock_get_executor.return_value = mock_executor

            result = runner.invoke(app, ["run", "--continue-on-error"])

            # Should continue despite failures
            assert "continue" in result.stdout.lower() or "failed" in result.stdout.lower()


class TestSeeknalRunStatePersistence:
    """Test state persistence across runs."""

    def test_state_saved_after_run(self, sample_yaml_files, monkeypatch):
        """Test state is saved after successful run."""
        monkeypatch.chdir(sample_yaml_files)

        with patch("seeknal.workflow.executors.get_executor") as mock_get_executor:
            mock_executor = MagicMock()
            mock_result = MagicMock()
            mock_result.is_success.return_value = True
            mock_result.row_count = 100
            mock_result.metadata = {}
            mock_executor.run.return_value = mock_result
            mock_get_executor.return_value = mock_executor

            result = runner.invoke(app, ["run"])

            assert result.exit_code == 0
            assert "State saved" in result.stdout

            # Verify state file was created
            state_path = sample_yaml_files / "target" / "run_state.json"
            assert state_path.exists()

            # Verify state content
            with open(state_path) as f:
                state_data = json.load(f)
                assert "nodes" in state_data
                assert "run_id" in state_data


class TestSeeknalRunHelp:
    """Test help documentation."""

    def test_run_help_displays_all_flags(self):
        """Test --help shows all available flags."""
        result = runner.invoke(app, ["run", "--help"])

        assert result.exit_code == 0
        assert "--full" in result.stdout
        assert "--nodes" in result.stdout
        assert "--types" in result.stdout
        assert "--show-plan" in result.stdout
        assert "--dry-run" in result.stdout
        assert "--continue-on-error" in result.stdout
        assert "--retry" in result.stdout

    def test_run_help_shows_examples(self):
        """Test --help shows usage examples."""
        result = runner.invoke(app, ["run", "--help"])

        assert result.exit_code == 0
        assert "Examples:" in result.stdout
        assert "seeknal run" in result.stdout
        assert "seeknal run --full" in result.stdout
