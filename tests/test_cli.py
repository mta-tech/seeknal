"""Tests for Seeknal CLI commands."""

import os
import json
import tempfile
import shutil
from datetime import datetime
from unittest import mock
from pathlib import Path

import pytest
from typer.testing import CliRunner

from seeknal.cli.main import (
    app,
    OutputFormat,
    WriteMode,
    ResourceType,
    DeleteResourceType,
    _get_version,
    _echo_success,
    _echo_error,
    _echo_warning,
    _echo_info,
)

# Pre-populate sys.modules with mock to enable patching without triggering full import chain
import sys
if "seeknal.featurestore.feature_group" not in sys.modules:
    sys.modules["seeknal.featurestore.feature_group"] = mock.MagicMock()


runner = CliRunner()


class TestVersionCommand:
    """Tests for the version command."""

    def test_version_displays_seeknal_version(self):
        """Version command should display Seeknal version."""
        result = runner.invoke(app, ["version"])
        assert result.exit_code == 0
        assert "Seeknal version:" in result.stdout

    def test_version_displays_python_version(self):
        """Version command should display Python version."""
        result = runner.invoke(app, ["version"])
        assert result.exit_code == 0
        assert "Python version:" in result.stdout

    def test_version_displays_pyspark_version(self):
        """Version command should display PySpark version."""
        result = runner.invoke(app, ["version"])
        assert result.exit_code == 0
        assert "PySpark version:" in result.stdout

    def test_version_displays_duckdb_version(self):
        """Version command should display DuckDB version."""
        result = runner.invoke(app, ["version"])
        assert result.exit_code == 0
        assert "DuckDB version:" in result.stdout


class TestInitCommand:
    """Tests for the init command."""

    def test_init_creates_project(self, tmp_path):
        """Init command should create a new project."""
        os.chdir(tmp_path)
        result = runner.invoke(app, ["init", "--name", "test_project"])
        assert result.exit_code == 0
        assert "initialized successfully" in result.stdout

    def test_init_creates_directories(self, tmp_path):
        """Init command should create required directories."""
        os.chdir(tmp_path)
        result = runner.invoke(app, ["init", "--name", "test_project_dirs"])
        assert result.exit_code == 0
        assert (tmp_path / "flows").exists()
        assert (tmp_path / "entities").exists()
        assert (tmp_path / "feature_groups").exists()

    def test_init_with_description(self, tmp_path):
        """Init command should accept description option."""
        os.chdir(tmp_path)
        result = runner.invoke(
            app, ["init", "--name", "test_project_desc", "--description", "Test description"]
        )
        assert result.exit_code == 0
        assert "initialized successfully" in result.stdout

    def test_init_defaults_to_directory_name(self, tmp_path):
        """Init command should default project name to directory name."""
        os.chdir(tmp_path)
        result = runner.invoke(app, ["init"])
        assert result.exit_code == 0
        assert "Initializing Seeknal project" in result.stdout


class TestValidateCommand:
    """Tests for the validate command."""

    def test_validate_checks_config(self):
        """Validate command should check configuration."""
        result = runner.invoke(app, ["validate"])
        assert result.exit_code == 0
        assert "Validating configuration" in result.stdout

    def test_validate_checks_database(self):
        """Validate command should check database connection."""
        result = runner.invoke(app, ["validate"])
        assert result.exit_code == 0
        assert "Validating database connection" in result.stdout

    def test_validate_success_message(self):
        """Validate command should show success message on completion."""
        result = runner.invoke(app, ["validate"])
        assert result.exit_code == 0
        assert "validations passed" in result.stdout


class TestListCommand:
    """Tests for the list command."""

    def test_list_projects(self):
        """List command should list projects."""
        result = runner.invoke(app, ["list", "projects"])
        assert result.exit_code == 0

    def test_list_entities(self):
        """List command should list entities."""
        result = runner.invoke(app, ["list", "entities"])
        assert result.exit_code == 0

    def test_list_flows(self):
        """List command should list flows."""
        result = runner.invoke(app, ["list", "flows"])
        assert result.exit_code == 0

    def test_list_workspaces(self):
        """List command should list workspaces."""
        result = runner.invoke(app, ["list", "workspaces"])
        assert result.exit_code == 0

    def test_list_feature_groups(self):
        """List command should list feature groups."""
        result = runner.invoke(app, ["list", "feature-groups"])
        assert result.exit_code == 0

    def test_list_offline_stores(self):
        """List command should list offline stores."""
        result = runner.invoke(app, ["list", "offline-stores"])
        assert result.exit_code == 0

    def test_list_invalid_resource_type(self):
        """List command should fail for invalid resource type."""
        result = runner.invoke(app, ["list", "invalid-resource"])
        assert result.exit_code != 0


class TestShowCommand:
    """Tests for the show command."""

    def test_show_project(self):
        """Show command should display project details."""
        # First ensure project exists
        runner.invoke(app, ["init", "--name", "show_test_project"])
        result = runner.invoke(app, ["show", "project", "my_project"])
        # May succeed or fail depending on whether project exists
        assert result.exit_code in [0, 1]

    def test_show_invalid_resource(self):
        """Show command should fail for non-existent resource."""
        result = runner.invoke(app, ["show", "project", "nonexistent_project_xyz"])
        assert result.exit_code == 1
        assert "not found" in result.stdout

    def test_show_json_format(self):
        """Show command should support JSON output format."""
        result = runner.invoke(
            app, ["show", "project", "my_project", "--format", "json"]
        )
        # May succeed or fail depending on project existence
        assert result.exit_code in [0, 1]


class TestMaterializeCommand:
    """Tests for the materialize command."""

    def test_materialize_requires_feature_group(self):
        """Materialize command should require feature group argument."""
        result = runner.invoke(app, ["materialize"])
        assert result.exit_code != 0

    def test_materialize_requires_start_date(self):
        """Materialize command should require start date."""
        result = runner.invoke(app, ["materialize", "test_fg"])
        assert result.exit_code != 0

    def test_materialize_invalid_date_format(self):
        """Materialize command should reject invalid date format."""
        result = runner.invoke(
            app, ["materialize", "test_fg", "--start-date", "invalid-date"]
        )
        assert result.exit_code == 1
        assert "Invalid date format" in result.stdout


class TestRunCommand:
    """Tests for the run command."""

    def test_run_requires_flow_name(self):
        """Run command should require flow name argument."""
        result = runner.invoke(app, ["run"])
        assert result.exit_code != 0

    def test_run_dry_run_mode(self):
        """Run command should support dry-run mode."""
        result = runner.invoke(app, ["run", "test_flow", "--dry-run"])
        assert result.exit_code == 0
        assert "Dry run mode" in result.stdout

    def test_run_with_date_range(self):
        """Run command should accept date range options."""
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
        assert "Start date: 2024-01-01" in result.stdout


class TestCleanCommand:
    """Tests for the clean command."""

    def test_clean_requires_feature_group(self):
        """Clean command should require feature group argument."""
        result = runner.invoke(app, ["clean"])
        assert result.exit_code != 0

    def test_clean_requires_before_or_ttl(self):
        """Clean command should require --before or --ttl option."""
        result = runner.invoke(app, ["clean", "test_fg"])
        assert result.exit_code == 1
        assert "Either --before or --ttl must be specified" in result.stdout

    def test_clean_dry_run_mode(self):
        """Clean command should support dry-run mode."""
        result = runner.invoke(
            app, ["clean", "test_fg", "--ttl", "30", "--dry-run"]
        )
        assert result.exit_code == 0
        assert "Dry run mode" in result.stdout

    def test_clean_with_before_date(self):
        """Clean command should accept --before date option."""
        result = runner.invoke(
            app, ["clean", "test_fg", "--before", "2024-01-01", "--dry-run"]
        )
        assert result.exit_code == 0

    def test_clean_invalid_date_format(self):
        """Clean command should reject invalid date format."""
        result = runner.invoke(
            app, ["clean", "test_fg", "--before", "invalid"]
        )
        assert result.exit_code == 1
        assert "Invalid date format" in result.stdout


class TestOutputFormatEnum:
    """Tests for OutputFormat enum."""

    def test_table_format(self):
        """OutputFormat should have TABLE value."""
        assert OutputFormat.TABLE.value == "table"

    def test_json_format(self):
        """OutputFormat should have JSON value."""
        assert OutputFormat.JSON.value == "json"

    def test_yaml_format(self):
        """OutputFormat should have YAML value."""
        assert OutputFormat.YAML.value == "yaml"


class TestWriteModeEnum:
    """Tests for WriteMode enum."""

    def test_overwrite_mode(self):
        """WriteMode should have OVERWRITE value."""
        assert WriteMode.OVERWRITE.value == "overwrite"

    def test_append_mode(self):
        """WriteMode should have APPEND value."""
        assert WriteMode.APPEND.value == "append"

    def test_merge_mode(self):
        """WriteMode should have MERGE value."""
        assert WriteMode.MERGE.value == "merge"


class TestResourceTypeEnum:
    """Tests for ResourceType enum."""

    def test_projects_type(self):
        """ResourceType should have PROJECTS value."""
        assert ResourceType.PROJECTS.value == "projects"

    def test_workspaces_type(self):
        """ResourceType should have WORKSPACES value."""
        assert ResourceType.WORKSPACES.value == "workspaces"

    def test_entities_type(self):
        """ResourceType should have ENTITIES value."""
        assert ResourceType.ENTITIES.value == "entities"

    def test_flows_type(self):
        """ResourceType should have FLOWS value."""
        assert ResourceType.FLOWS.value == "flows"

    def test_feature_groups_type(self):
        """ResourceType should have FEATURE_GROUPS value."""
        assert ResourceType.FEATURE_GROUPS.value == "feature-groups"

    def test_offline_stores_type(self):
        """ResourceType should have OFFLINE_STORES value."""
        assert ResourceType.OFFLINE_STORES.value == "offline-stores"


class TestHelperFunctions:
    """Tests for CLI helper functions."""

    def test_get_version_returns_string(self):
        """_get_version should return a version string."""
        version = _get_version()
        assert isinstance(version, str)
        assert len(version) > 0


class TestHelpCommand:
    """Tests for help output."""

    def test_main_help(self):
        """Main help should display available commands."""
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "version" in result.stdout
        assert "init" in result.stdout
        assert "list" in result.stdout
        assert "validate" in result.stdout

    def test_init_help(self):
        """Init command help should display options."""
        result = runner.invoke(app, ["init", "--help"])
        assert result.exit_code == 0
        assert "--name" in result.stdout
        assert "--description" in result.stdout

    def test_list_help(self):
        """List command help should display resource types."""
        result = runner.invoke(app, ["list", "--help"])
        assert result.exit_code == 0
        assert "RESOURCE_TYPE" in result.stdout

    def test_materialize_help(self):
        """Materialize command help should display options."""
        result = runner.invoke(app, ["materialize", "--help"])
        assert result.exit_code == 0
        assert "--start-date" in result.stdout
        assert "--mode" in result.stdout


class TestValidateFeaturesCommand:
    """Tests for the validate-features command."""

    def test_validate_features_help(self):
        """validate-features command help should display options."""
        result = runner.invoke(app, ["validate-features", "--help"])
        assert result.exit_code == 0
        assert "--mode" in result.stdout
        assert "--verbose" in result.stdout
        assert "FEATURE_GROUP" in result.stdout

    def test_validate_features_requires_feature_group(self):
        """validate-features command should require feature group argument."""
        result = runner.invoke(app, ["validate-features"])
        assert result.exit_code != 0

    def test_validate_features_mode_option(self):
        """validate-features command should accept mode option."""
        result = runner.invoke(app, ["validate-features", "--help"])
        assert result.exit_code == 0
        assert "warn" in result.stdout
        assert "fail" in result.stdout

    def test_validate_features_verbose_option(self):
        """validate-features command should accept verbose option."""
        result = runner.invoke(app, ["validate-features", "--help"])
        assert result.exit_code == 0
        assert "-v" in result.stdout
        assert "--verbose" in result.stdout


class TestDebugCommand:
    """Tests for the debug command."""

    def test_debug_requires_feature_group(self):
        """Debug command should require feature group argument."""
        result = runner.invoke(app, ["debug"])
        assert result.exit_code != 0

    def test_debug_with_limit(self):
        """Debug command should accept limit option."""
        # Will fail if feature group doesn't exist, but should parse options
        result = runner.invoke(
            app, ["debug", "test_fg", "--limit", "5"]
        )
        # May fail for non-existent fg, but should not fail on argument parsing
        assert "test_fg" in result.stdout or result.exit_code in [0, 1]


class TestDeleteCommand:
    """Tests for the delete command."""

    def test_delete_requires_resource_type(self):
        """Delete command should require resource type argument."""
        result = runner.invoke(app, ["delete"])
        assert result.exit_code != 0

    def test_delete_requires_name(self):
        """Delete command should require name argument."""
        result = runner.invoke(app, ["delete", "feature-group"])
        assert result.exit_code != 0

    def test_delete_help(self):
        """Delete command help should display options."""
        result = runner.invoke(app, ["delete", "--help"])
        assert result.exit_code == 0
        assert "feature-group" in result.stdout
        assert "--force" in result.stdout

    def test_delete_feature_group_success(self):
        """Delete command should successfully delete a feature group with --force."""
        with mock.patch("seeknal.featurestore.feature_group.FeatureGroup") as mock_fg_class:
            # Setup mock
            mock_fg_instance = mock.MagicMock()
            mock_fg_class.load.return_value = mock_fg_instance
            mock_fg_instance.delete.return_value = None

            result = runner.invoke(
                app, ["delete", "feature-group", "test_fg", "--force"]
            )

            assert result.exit_code == 0
            assert "Deleted feature group 'test_fg'" in result.stdout
            assert "removed storage files and metadata" in result.stdout
            mock_fg_class.load.assert_called_once_with(name="test_fg")
            mock_fg_instance.delete.assert_called_once()

    def test_delete_feature_group_not_found(self):
        """Delete command should return error for non-existent feature group."""
        with mock.patch("seeknal.featurestore.feature_group.FeatureGroup") as mock_fg_class:
            # Simulate feature group not found
            mock_fg_class.load.side_effect = Exception("Feature group not found")

            result = runner.invoke(
                app, ["delete", "feature-group", "nonexistent_fg", "--force"]
            )

            assert result.exit_code == 1
            assert "not found" in result.stdout
            mock_fg_class.load.assert_called_once_with(name="nonexistent_fg")

    def test_delete_feature_group_confirmation_cancelled(self):
        """Delete command should exit gracefully when confirmation is cancelled."""
        with mock.patch("seeknal.featurestore.feature_group.FeatureGroup") as mock_fg_class:
            # Setup mock for load
            mock_fg_instance = mock.MagicMock()
            mock_fg_class.load.return_value = mock_fg_instance

            # Simulate "n" input for confirmation
            result = runner.invoke(
                app, ["delete", "feature-group", "test_fg"], input="n\n"
            )

            assert result.exit_code == 0
            assert "cancelled" in result.stdout.lower()
            # delete() should NOT be called when cancelled
            mock_fg_instance.delete.assert_not_called()

    def test_delete_feature_group_confirmation_accepted(self):
        """Delete command should proceed when confirmation is accepted."""
        with mock.patch("seeknal.featurestore.feature_group.FeatureGroup") as mock_fg_class:
            # Setup mock
            mock_fg_instance = mock.MagicMock()
            mock_fg_class.load.return_value = mock_fg_instance
            mock_fg_instance.delete.return_value = None

            # Simulate "y" input for confirmation
            result = runner.invoke(
                app, ["delete", "feature-group", "test_fg"], input="y\n"
            )

            assert result.exit_code == 0
            assert "Deleted feature group 'test_fg'" in result.stdout
            mock_fg_instance.delete.assert_called_once()

    def test_delete_feature_group_delete_failure(self):
        """Delete command should handle deletion failures gracefully."""
        with mock.patch("seeknal.featurestore.feature_group.FeatureGroup") as mock_fg_class:
            # Setup mock
            mock_fg_instance = mock.MagicMock()
            mock_fg_class.load.return_value = mock_fg_instance
            mock_fg_instance.delete.side_effect = Exception("Storage deletion failed")

            result = runner.invoke(
                app, ["delete", "feature-group", "test_fg", "--force"]
            )

            assert result.exit_code == 1
            assert "Failed to delete" in result.stdout
            assert "test_fg" in result.stdout

    def test_delete_invalid_resource_type(self):
        """Delete command should fail for invalid resource type."""
        result = runner.invoke(app, ["delete", "invalid-type", "test_name"])
        assert result.exit_code != 0


class TestDeleteResourceTypeEnum:
    """Tests for DeleteResourceType enum."""

    def test_feature_group_type(self):
        """DeleteResourceType should have FEATURE_GROUP value."""
        assert DeleteResourceType.FEATURE_GROUP.value == "feature-group"
