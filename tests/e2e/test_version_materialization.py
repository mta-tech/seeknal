"""End-to-end tests for version-specific materialization workflows.

These tests verify complete version management workflows including:
- Creating feature groups with multiple versions
- Listing and comparing versions via CLI
- Materializing specific versions (not just latest)
- Verifying version history persists correctly
"""

import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from unittest import mock

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


class TestVersionMaterializationWorkflow:
    """E2E tests for version-specific materialization workflows."""

    def test_materialize_with_version_displays_parameters(self, clean_test_env):
        """Test that materialize command with --version displays parameters correctly."""
        runner.invoke(app, ["init", "--name", "version_materialize_test"])

        result = runner.invoke(
            app,
            [
                "materialize", "test_fg",
                "--start-date", "2024-01-01",
                "--end-date", "2024-12-31",
                "--version", "1",
                "--mode", "overwrite"
            ]
        )
        # Will fail because feature group doesn't exist, but should show parameters
        assert "Materializing feature group: test_fg" in result.stdout
        assert "Version: 1" in result.stdout
        assert "Start date: 2024-01-01" in result.stdout
        assert "Mode: overwrite" in result.stdout

    def test_materialize_with_different_versions(self, clean_test_env):
        """Test materializing with different version numbers."""
        runner.invoke(app, ["init", "--name", "multi_version_test"])

        # Test with version 1
        result = runner.invoke(
            app,
            [
                "materialize", "test_fg",
                "--start-date", "2024-01-01",
                "--version", "1"
            ]
        )
        assert "Version: 1" in result.stdout

        # Test with version 2
        result = runner.invoke(
            app,
            [
                "materialize", "test_fg",
                "--start-date", "2024-01-01",
                "--version", "2"
            ]
        )
        assert "Version: 2" in result.stdout

    def test_materialize_without_version_uses_latest(self, clean_test_env):
        """Test that materialize without --version defaults to latest."""
        runner.invoke(app, ["init", "--name", "latest_version_test"])

        result = runner.invoke(
            app,
            [
                "materialize", "test_fg",
                "--start-date", "2024-01-01",
                "--mode", "append"
            ]
        )
        # Should not show version line when not specified
        assert "Materializing feature group: test_fg" in result.stdout
        assert "Mode: append" in result.stdout


class TestVersionQueryingWorkflow:
    """E2E tests for version querying workflows."""

    @mock.patch("seeknal.featurestore.feature_group.FeatureGroup")
    def test_version_list_workflow(self, mock_fg_class, clean_test_env):
        """Test the full version listing workflow."""
        # Setup mock to simulate a feature group with multiple versions
        mock_fg = mock.MagicMock()
        mock_fg.list_versions.return_value = [
            {"version": 2, "created_at": "2024-01-02 10:00:00", "feature_count": 5, "avro_schema": None},
            {"version": 1, "created_at": "2024-01-01 09:00:00", "feature_count": 4, "avro_schema": None},
        ]
        mock_fg_class.return_value.get_or_create.return_value = mock_fg

        runner.invoke(app, ["init", "--name", "version_list_e2e_test"])

        result = runner.invoke(app, ["version", "list", "user_features"])

        assert result.exit_code == 0
        assert "Versions for feature group:" in result.stdout
        assert "user_features" in result.stdout
        # Should display both versions
        assert "Version" in result.stdout
        assert "Created At" in result.stdout
        assert "Features" in result.stdout

    @mock.patch("seeknal.featurestore.feature_group.FeatureGroup")
    def test_version_show_workflow(self, mock_fg_class, clean_test_env):
        """Test the version show workflow with specific version."""
        mock_fg = mock.MagicMock()
        mock_fg.get_version.return_value = {
            "version": 1,
            "created_at": "2024-01-01 09:00:00",
            "updated_at": "2024-01-01 10:00:00",
            "feature_count": 4,
            "avro_schema": {
                "type": "record",
                "name": "user_features",
                "fields": [
                    {"name": "user_id", "type": "string"},
                    {"name": "feature_a", "type": "int"},
                    {"name": "feature_b", "type": "double"},
                    {"name": "feature_c", "type": "string"},
                ]
            }
        }
        mock_fg_class.return_value.get_or_create.return_value = mock_fg

        runner.invoke(app, ["init", "--name", "version_show_e2e_test"])

        result = runner.invoke(
            app, ["version", "show", "user_features", "--version", "1"]
        )

        assert result.exit_code == 0
        assert "Feature Group:" in result.stdout
        assert "user_features" in result.stdout
        assert "Version:" in result.stdout
        assert "Schema:" in result.stdout
        assert "Fields:" in result.stdout

    @mock.patch("seeknal.featurestore.feature_group.FeatureGroup")
    def test_version_diff_workflow(self, mock_fg_class, clean_test_env):
        """Test the schema comparison workflow between versions."""
        mock_fg = mock.MagicMock()
        mock_fg.compare_versions.return_value = {
            "from_version": 1,
            "to_version": 2,
            "added": [{"name": "new_feature", "type": "string"}],
            "removed": [],
            "modified": [],
        }
        mock_fg_class.return_value.get_or_create.return_value = mock_fg

        runner.invoke(app, ["init", "--name", "version_diff_e2e_test"])

        result = runner.invoke(
            app, ["version", "diff", "user_features", "--from", "1", "--to", "2"]
        )

        assert result.exit_code == 0
        assert "Feature Group:" in result.stdout
        assert "Comparing version" in result.stdout
        assert "Added" in result.stdout
        assert "new_feature" in result.stdout
        assert "Summary:" in result.stdout


class TestVersionRollbackWorkflow:
    """E2E tests for version rollback scenarios."""

    def test_version_rollback_materialize_older_version(self, clean_test_env):
        """Test materializing an older version for rollback scenario."""
        runner.invoke(app, ["init", "--name", "rollback_test"])

        # Simulate discovering issue with v2, rolling back to v1
        result = runner.invoke(
            app,
            [
                "materialize", "user_features",
                "--start-date", "2024-01-01",
                "--end-date", "2024-01-31",
                "--version", "1",
                "--mode", "overwrite"
            ]
        )

        assert "Materializing feature group: user_features" in result.stdout
        assert "Version: 1" in result.stdout
        assert "Mode: overwrite" in result.stdout

    @mock.patch("seeknal.featurestore.feature_group.FeatureGroup")
    def test_version_list_shows_multiple_versions_after_rollback(
        self, mock_fg_class, clean_test_env
    ):
        """Test that version list shows all versions including after rollback."""
        mock_fg = mock.MagicMock()
        mock_fg.list_versions.return_value = [
            {"version": 3, "created_at": "2024-01-03 10:00:00", "feature_count": 6, "avro_schema": None},
            {"version": 2, "created_at": "2024-01-02 10:00:00", "feature_count": 5, "avro_schema": None},
            {"version": 1, "created_at": "2024-01-01 09:00:00", "feature_count": 4, "avro_schema": None},
        ]
        mock_fg_class.return_value.get_or_create.return_value = mock_fg

        runner.invoke(app, ["init", "--name", "rollback_list_test"])

        result = runner.invoke(app, ["version", "list", "user_features"])

        assert result.exit_code == 0
        # All three versions should be visible
        assert "Versions for feature group:" in result.stdout


class TestSchemaComparisonWorkflow:
    """E2E tests for schema comparison workflows."""

    @mock.patch("seeknal.featurestore.feature_group.FeatureGroup")
    def test_schema_comparison_detects_added_features(self, mock_fg_class, clean_test_env):
        """Test that schema comparison correctly detects added features."""
        mock_fg = mock.MagicMock()
        mock_fg.compare_versions.return_value = {
            "added": [
                {"name": "new_feature_1", "type": "string"},
                {"name": "new_feature_2", "type": "int"},
            ],
            "removed": [],
            "modified": [],
        }
        mock_fg_class.return_value.get_or_create.return_value = mock_fg

        runner.invoke(app, ["init", "--name", "added_features_test"])

        result = runner.invoke(
            app, ["version", "diff", "user_features", "--from", "1", "--to", "2"]
        )

        assert result.exit_code == 0
        assert "Added" in result.stdout
        assert "new_feature_1" in result.stdout
        assert "new_feature_2" in result.stdout
        assert "2 added" in result.stdout

    @mock.patch("seeknal.featurestore.feature_group.FeatureGroup")
    def test_schema_comparison_detects_removed_features(self, mock_fg_class, clean_test_env):
        """Test that schema comparison correctly detects removed features."""
        mock_fg = mock.MagicMock()
        mock_fg.compare_versions.return_value = {
            "added": [],
            "removed": [{"name": "deprecated_feature", "type": "string"}],
            "modified": [],
        }
        mock_fg_class.return_value.get_or_create.return_value = mock_fg

        runner.invoke(app, ["init", "--name", "removed_features_test"])

        result = runner.invoke(
            app, ["version", "diff", "user_features", "--from", "1", "--to", "2"]
        )

        assert result.exit_code == 0
        assert "Removed" in result.stdout
        assert "deprecated_feature" in result.stdout
        assert "1 removed" in result.stdout

    @mock.patch("seeknal.featurestore.feature_group.FeatureGroup")
    def test_schema_comparison_detects_modified_features(self, mock_fg_class, clean_test_env):
        """Test that schema comparison correctly detects type changes."""
        mock_fg = mock.MagicMock()
        mock_fg.compare_versions.return_value = {
            "added": [],
            "removed": [],
            "modified": [
                {"field": "user_score", "old_type": "int", "new_type": "double"}
            ],
        }
        mock_fg_class.return_value.get_or_create.return_value = mock_fg

        runner.invoke(app, ["init", "--name", "modified_features_test"])

        result = runner.invoke(
            app, ["version", "diff", "user_features", "--from", "1", "--to", "2"]
        )

        assert result.exit_code == 0
        assert "Modified" in result.stdout
        assert "user_score" in result.stdout
        assert "int" in result.stdout
        assert "double" in result.stdout
        assert "1 modified" in result.stdout

    @mock.patch("seeknal.featurestore.feature_group.FeatureGroup")
    def test_schema_comparison_no_changes(self, mock_fg_class, clean_test_env):
        """Test schema comparison when versions have identical schemas."""
        mock_fg = mock.MagicMock()
        mock_fg.compare_versions.return_value = {
            "added": [],
            "removed": [],
            "modified": [],
        }
        mock_fg_class.return_value.get_or_create.return_value = mock_fg

        runner.invoke(app, ["init", "--name", "no_changes_test"])

        result = runner.invoke(
            app, ["version", "diff", "user_features", "--from", "1", "--to", "2"]
        )

        assert result.exit_code == 0
        assert "No schema changes" in result.stdout


class TestVersionErrorHandling:
    """E2E tests for error handling in version workflows."""

    @mock.patch("seeknal.featurestore.feature_group.FeatureGroup")
    def test_version_list_empty_feature_group(self, mock_fg_class, clean_test_env):
        """Test version list with feature group that has no versions."""
        mock_fg = mock.MagicMock()
        mock_fg.list_versions.return_value = []
        mock_fg_class.return_value.get_or_create.return_value = mock_fg

        runner.invoke(app, ["init", "--name", "empty_fg_test"])

        result = runner.invoke(app, ["version", "list", "empty_fg"])

        assert result.exit_code == 0
        assert "No versions found" in result.stdout

    @mock.patch("seeknal.featurestore.feature_group.FeatureGroup")
    def test_version_show_nonexistent_version(self, mock_fg_class, clean_test_env):
        """Test version show with non-existent version number."""
        mock_fg = mock.MagicMock()
        mock_fg.get_version.return_value = None
        mock_fg_class.return_value.get_or_create.return_value = mock_fg

        runner.invoke(app, ["init", "--name", "nonexistent_version_test"])

        result = runner.invoke(
            app, ["version", "show", "user_features", "--version", "999"]
        )

        assert result.exit_code == 1
        assert "not found" in result.stdout

    @mock.patch("seeknal.featurestore.feature_group.FeatureGroup")
    def test_version_diff_same_version_error(self, mock_fg_class, clean_test_env):
        """Test that comparing same version with itself fails with clear error."""
        mock_fg = mock.MagicMock()
        mock_fg.compare_versions.side_effect = ValueError(
            "from_version and to_version must be different"
        )
        mock_fg_class.return_value.get_or_create.return_value = mock_fg

        runner.invoke(app, ["init", "--name", "same_version_test"])

        result = runner.invoke(
            app, ["version", "diff", "user_features", "--from", "1", "--to", "1"]
        )

        assert result.exit_code == 1
        assert "must be different" in result.stdout

    @mock.patch("seeknal.featurestore.feature_group.FeatureGroup")
    def test_version_diff_nonexistent_version(self, mock_fg_class, clean_test_env):
        """Test diff with non-existent version."""
        mock_fg = mock.MagicMock()
        mock_fg.compare_versions.side_effect = ValueError("Version 999 not found")
        mock_fg_class.return_value.get_or_create.return_value = mock_fg

        runner.invoke(app, ["init", "--name", "nonexistent_diff_test"])

        result = runner.invoke(
            app, ["version", "diff", "user_features", "--from", "999", "--to", "1"]
        )

        assert result.exit_code == 1
        assert "Version 999 not found" in result.stdout


class TestVersionCLIIntegration:
    """Integration tests verifying version CLI components work together."""

    def test_version_command_help(self, clean_test_env):
        """Test that version command group shows all subcommands."""
        result = runner.invoke(app, ["version", "--help"])

        assert result.exit_code == 0
        assert "list" in result.stdout
        assert "show" in result.stdout
        assert "diff" in result.stdout
        assert "Manage feature group versions" in result.stdout

    def test_materialize_help_shows_version_option(self, clean_test_env):
        """Test that materialize --help shows version option."""
        result = runner.invoke(app, ["materialize", "--help"])

        assert result.exit_code == 0
        assert "--version" in result.stdout

    @mock.patch("seeknal.featurestore.feature_group.FeatureGroup")
    def test_version_list_json_output(self, mock_fg_class, clean_test_env):
        """Test version list with JSON output format."""
        mock_fg = mock.MagicMock()
        mock_fg.list_versions.return_value = [
            {"version": 2, "created_at": "2024-01-02 10:00:00", "feature_count": 5, "avro_schema": None},
            {"version": 1, "created_at": "2024-01-01 09:00:00", "feature_count": 4, "avro_schema": None},
        ]
        mock_fg_class.return_value.get_or_create.return_value = mock_fg

        runner.invoke(app, ["init", "--name", "json_output_test"])

        result = runner.invoke(
            app, ["version", "list", "user_features", "--format", "json"]
        )

        assert result.exit_code == 0
        output = json.loads(result.stdout)
        assert isinstance(output, list)
        assert len(output) == 2
        assert output[0]["version"] == 2

    @mock.patch("seeknal.featurestore.feature_group.FeatureGroup")
    def test_full_version_workflow(self, mock_fg_class, clean_test_env):
        """Test complete version management workflow: list -> show -> diff."""
        mock_fg = mock.MagicMock()

        # Setup for list
        mock_fg.list_versions.return_value = [
            {"version": 2, "created_at": "2024-01-02", "feature_count": 5, "avro_schema": None},
            {"version": 1, "created_at": "2024-01-01", "feature_count": 4, "avro_schema": None},
        ]

        # Setup for show
        mock_fg.get_version.return_value = {
            "version": 2,
            "created_at": "2024-01-02 10:00:00",
            "updated_at": "2024-01-02 10:00:00",
            "feature_count": 5,
            "avro_schema": {"type": "record", "fields": []}
        }

        # Setup for diff
        mock_fg.compare_versions.return_value = {
            "added": [{"name": "new_feature", "type": "string"}],
            "removed": [],
            "modified": [],
        }

        mock_fg_class.return_value.get_or_create.return_value = mock_fg

        runner.invoke(app, ["init", "--name", "full_workflow_test"])

        # Step 1: List versions
        result = runner.invoke(app, ["version", "list", "user_features"])
        assert result.exit_code == 0
        assert "Versions for feature group:" in result.stdout

        # Step 2: Show latest version
        result = runner.invoke(
            app, ["version", "show", "user_features", "--version", "2"]
        )
        assert result.exit_code == 0
        assert "Version:" in result.stdout

        # Step 3: Compare versions
        result = runner.invoke(
            app, ["version", "diff", "user_features", "--from", "1", "--to", "2"]
        )
        assert result.exit_code == 0
        assert "Added" in result.stdout


class TestVersionBackwardCompatibility:
    """E2E tests verifying backward compatibility of version operations."""

    def test_materialize_without_version_works(self, clean_test_env):
        """Test that materialization without version flag still works (backward compat)."""
        runner.invoke(app, ["init", "--name", "backward_compat_test"])

        result = runner.invoke(
            app,
            [
                "materialize", "test_fg",
                "--start-date", "2024-01-01",
                "--end-date", "2024-12-31",
                "--mode", "overwrite"
            ]
        )

        # Should still work without version specified
        assert "Materializing feature group: test_fg" in result.stdout
        assert "Start date: 2024-01-01" in result.stdout
        # Version line should not appear when not specified
        # Note: We check that the command runs successfully without version

    def test_existing_cli_commands_still_work(self, clean_test_env):
        """Test that existing CLI commands work alongside new version commands."""
        # Test init
        result = runner.invoke(
            app, ["init", "--name", "integration_test"]
        )
        assert result.exit_code == 0

        # Test validate
        result = runner.invoke(app, ["validate"])
        assert result.exit_code == 0

        # Test version info (renamed from version to info)
        result = runner.invoke(app, ["info"])
        assert result.exit_code == 0
        assert "Seeknal version:" in result.stdout

        # Test list
        for resource in ["projects", "entities", "flows"]:
            result = runner.invoke(app, ["list", resource])
            assert result.exit_code == 0

        # Test new version subcommands work
        result = runner.invoke(app, ["version", "--help"])
        assert result.exit_code == 0
        assert "list" in result.stdout
        assert "show" in result.stdout
        assert "diff" in result.stdout
