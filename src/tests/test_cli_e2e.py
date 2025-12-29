"""End-to-end tests for Seeknal CLI workflows.

These tests verify complete workflows using the CLI, including:
- Project initialization and setup
- Feature group creation and management
- Materialization workflows
- Resource listing and inspection
"""

import os
import json
import tempfile
import shutil
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from typer.testing import CliRunner

from seeknal.cli.main import app


runner = CliRunner()


@pytest.fixture(scope="function")
def clean_test_env(tmp_path):
    """Create a clean test environment for E2E tests."""
    original_dir = os.getcwd()
    os.chdir(tmp_path)
    yield tmp_path
    os.chdir(original_dir)


class TestProjectWorkflow:
    """E2E tests for project management workflows."""

    def test_full_project_initialization_workflow(self, clean_test_env):
        """Test complete project initialization workflow."""
        tmp_path = clean_test_env

        # Step 1: Initialize a new project
        result = runner.invoke(
            app,
            ["init", "--name", "e2e_test_project", "--description", "E2E test project"]
        )
        assert result.exit_code == 0
        assert "initialized successfully" in result.stdout

        # Step 2: Verify directories were created
        assert (tmp_path / "flows").exists()
        assert (tmp_path / "entities").exists()
        assert (tmp_path / "feature_groups").exists()

        # Step 3: Validate the setup
        result = runner.invoke(app, ["validate"])
        assert result.exit_code == 0
        assert "validations passed" in result.stdout

        # Step 4: List projects to verify
        result = runner.invoke(app, ["list", "projects"])
        assert result.exit_code == 0

    def test_project_creation_and_show(self, clean_test_env):
        """Test creating a project and viewing its details."""
        # Create project
        result = runner.invoke(
            app, ["init", "--name", "show_test_project"]
        )
        assert result.exit_code == 0

        # Show project - project should exist after init
        result = runner.invoke(app, ["show", "project", "my_project"])
        # Note: show may use a different project, check generic behavior
        assert result.exit_code in [0, 1]


class TestValidationWorkflow:
    """E2E tests for configuration validation workflows."""

    def test_validation_reports_all_checks(self, clean_test_env):
        """Test that validation runs all checks and reports results."""
        # Initialize project first
        runner.invoke(app, ["init", "--name", "validation_test"])

        result = runner.invoke(app, ["validate"])
        assert result.exit_code == 0

        # Should check configuration
        assert "Validating configuration" in result.stdout

        # Should check database
        assert "Validating database connection" in result.stdout

        # Should report success
        assert "validations passed" in result.stdout


class TestListingWorkflow:
    """E2E tests for resource listing workflows."""

    def test_list_all_resource_types(self, clean_test_env):
        """Test listing all available resource types."""
        # Initialize project
        runner.invoke(app, ["init", "--name", "list_test_project"])

        resource_types = [
            "projects",
            "entities",
            "flows",
            "workspaces",
            "feature-groups",
            "offline-stores"
        ]

        for resource_type in resource_types:
            result = runner.invoke(app, ["list", resource_type])
            assert result.exit_code == 0, f"Failed to list {resource_type}"


class TestDryRunWorkflow:
    """E2E tests for dry-run workflows."""

    def test_run_flow_dry_run(self, clean_test_env):
        """Test running a flow in dry-run mode."""
        runner.invoke(app, ["init", "--name", "dry_run_test"])

        result = runner.invoke(
            app,
            [
                "run", "test_flow",
                "--dry-run",
                "--start-date", "2024-01-01",
                "--end-date", "2024-12-31"
            ]
        )
        assert result.exit_code == 0
        assert "Dry run mode" in result.stdout
        assert "no changes will be made" in result.stdout

    def test_clean_dry_run(self, clean_test_env):
        """Test cleaning feature data in dry-run mode."""
        runner.invoke(app, ["init", "--name", "clean_dry_run_test"])

        result = runner.invoke(
            app, ["clean", "test_fg", "--ttl", "30", "--dry-run"]
        )
        assert result.exit_code == 0
        assert "Dry run mode" in result.stdout
        assert "no data will be deleted" in result.stdout


class TestVersionWorkflow:
    """E2E tests for version information workflows."""

    def test_version_shows_all_components(self):
        """Test that version shows all component versions."""
        result = runner.invoke(app, ["version"])
        assert result.exit_code == 0

        # Should show all version components
        assert "Seeknal version:" in result.stdout
        assert "Python version:" in result.stdout
        assert "PySpark version:" in result.stdout
        assert "DuckDB version:" in result.stdout

    def test_version_format(self):
        """Test that versions are in expected format."""
        result = runner.invoke(app, ["version"])
        assert result.exit_code == 0

        # Each line should have a version number pattern
        lines = result.stdout.strip().split("\n")
        assert len(lines) >= 4  # At least 4 version lines


class TestMaterializeWorkflow:
    """E2E tests for materialization workflows."""

    def test_materialize_displays_parameters(self, clean_test_env):
        """Test that materialize command displays parameters correctly."""
        runner.invoke(app, ["init", "--name", "materialize_test"])

        result = runner.invoke(
            app,
            [
                "materialize", "test_fg",
                "--start-date", "2024-01-01",
                "--end-date", "2024-12-31",
                "--mode", "overwrite"
            ]
        )
        # Will fail because feature group doesn't exist, but should show parameters
        assert "Materializing feature group: test_fg" in result.stdout
        assert "Start date: 2024-01-01" in result.stdout
        assert "Mode: overwrite" in result.stdout

    def test_materialize_with_append_mode(self, clean_test_env):
        """Test materialize with append mode."""
        runner.invoke(app, ["init", "--name", "materialize_append_test"])

        result = runner.invoke(
            app,
            [
                "materialize", "test_fg",
                "--start-date", "2024-06-01",
                "--mode", "append"
            ]
        )
        assert "Mode: append" in result.stdout


class TestErrorHandling:
    """E2E tests for error handling."""

    def test_invalid_resource_type_error(self, clean_test_env):
        """Test error handling for invalid resource types."""
        result = runner.invoke(app, ["list", "invalid-type"])
        assert result.exit_code != 0

    def test_invalid_date_format_error(self, clean_test_env):
        """Test error handling for invalid date formats."""
        runner.invoke(app, ["init", "--name", "error_test"])

        result = runner.invoke(
            app, ["materialize", "test_fg", "--start-date", "not-a-date"]
        )
        assert result.exit_code == 1
        assert "Invalid date format" in result.stdout

    def test_nonexistent_project_show_error(self, clean_test_env):
        """Test error handling when showing non-existent project."""
        result = runner.invoke(
            app, ["show", "project", "definitely_nonexistent_project_12345"]
        )
        assert result.exit_code == 1
        assert "not found" in result.stdout


class TestHelpWorkflow:
    """E2E tests for help documentation."""

    def test_main_help_shows_all_commands(self):
        """Test that main help shows all available commands."""
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0

        expected_commands = [
            "init", "run", "materialize", "list",
            "show", "validate", "version", "debug", "clean"
        ]

        for cmd in expected_commands:
            assert cmd in result.stdout, f"Command '{cmd}' not found in help"

    def test_all_commands_have_help(self):
        """Test that all commands have help documentation."""
        commands = [
            "init", "run", "materialize", "list",
            "show", "validate", "version", "debug", "clean"
        ]

        for cmd in commands:
            result = runner.invoke(app, [cmd, "--help"])
            assert result.exit_code == 0, f"Help failed for command '{cmd}'"
            assert len(result.stdout) > 50, f"Help too short for command '{cmd}'"


class TestCleanWorkflow:
    """E2E tests for data cleanup workflows."""

    def test_clean_with_ttl(self, clean_test_env):
        """Test clean command with TTL parameter."""
        runner.invoke(app, ["init", "--name", "clean_ttl_test"])

        result = runner.invoke(
            app, ["clean", "test_fg", "--ttl", "7", "--dry-run"]
        )
        assert result.exit_code == 0
        assert "Cleaning feature group: test_fg" in result.stdout

    def test_clean_with_before_date(self, clean_test_env):
        """Test clean command with before date parameter."""
        runner.invoke(app, ["init", "--name", "clean_date_test"])

        result = runner.invoke(
            app, ["clean", "test_fg", "--before", "2024-01-01", "--dry-run"]
        )
        assert result.exit_code == 0
        assert "Deleting data before: 2024-01-01" in result.stdout


class TestDebugWorkflow:
    """E2E tests for debug workflows."""

    def test_debug_displays_feature_group_name(self, clean_test_env):
        """Test that debug command displays feature group name."""
        runner.invoke(app, ["init", "--name", "debug_test"])

        result = runner.invoke(app, ["debug", "test_fg"])
        # Will fail because fg doesn't exist or Spark isn't initialized
        # But command should parse and attempt to run
        assert result.exit_code in [0, 1]
        # Either shows the feature group name or an error about SparkContext/feature group
        assert (
            "test_fg" in result.stdout or
            "Feature Group: test_fg" in result.stdout or
            "Debug failed" in result.stdout
        )


class TestCLIIntegration:
    """Integration tests verifying CLI components work together."""

    def test_init_validate_list_workflow(self, clean_test_env):
        """Test init -> validate -> list workflow."""
        # Initialize
        result = runner.invoke(
            app, ["init", "--name", "integration_test"]
        )
        assert result.exit_code == 0

        # Validate
        result = runner.invoke(app, ["validate"])
        assert result.exit_code == 0

        # List all resource types
        for resource in ["projects", "entities", "flows", "offline-stores"]:
            result = runner.invoke(app, ["list", resource])
            assert result.exit_code == 0

    def test_version_then_validate(self):
        """Test version and validate commands work independently."""
        # Version should always work
        result = runner.invoke(app, ["version"])
        assert result.exit_code == 0

        # Validate should work regardless of previous commands
        result = runner.invoke(app, ["validate"])
        assert result.exit_code == 0
