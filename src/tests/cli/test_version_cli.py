"""Integration tests for feature group version CLI commands.

Tests cover:
- seeknal version list <feature_group>: List all versions of a feature group
- seeknal version show <feature_group>: Show version details
- seeknal version diff <feature_group>: Compare two versions
"""

import json
from unittest import mock

import pytest
from typer.testing import CliRunner

from seeknal.cli.main import app, OutputFormat


runner = CliRunner()


class TestVersionListCommand:
    """Tests for the version list command."""

    def test_version_list_requires_feature_group(self):
        """Version list command should require feature group argument."""
        result = runner.invoke(app, ["version", "list"])
        assert result.exit_code != 0

    def test_version_list_displays_help(self):
        """Version list command should display help."""
        result = runner.invoke(app, ["version", "list", "--help"])
        assert result.exit_code == 0
        assert "FEATURE_GROUP" in result.stdout
        assert "--format" in result.stdout
        assert "--limit" in result.stdout

    @mock.patch("seeknal.cli.main.FeatureGroup")
    def test_version_list_table_format(self, mock_fg_class):
        """Version list command should display versions in table format."""
        # Setup mock
        mock_fg = mock.MagicMock()
        mock_fg.list_versions.return_value = [
            {"version": 2, "created_at": "2024-01-02 10:00:00", "feature_count": 5},
            {"version": 1, "created_at": "2024-01-01 09:00:00", "feature_count": 4},
        ]
        mock_fg_class.return_value.get_or_create.return_value = mock_fg

        result = runner.invoke(app, ["version", "list", "test_fg"])

        assert result.exit_code == 0
        assert "Versions for feature group:" in result.stdout
        assert "test_fg" in result.stdout
        assert "Version" in result.stdout
        assert "Created At" in result.stdout
        assert "Features" in result.stdout

    @mock.patch("seeknal.cli.main.FeatureGroup")
    def test_version_list_json_format(self, mock_fg_class):
        """Version list command should display versions in JSON format."""
        # Setup mock
        mock_fg = mock.MagicMock()
        mock_fg.list_versions.return_value = [
            {"version": 2, "created_at": "2024-01-02 10:00:00", "feature_count": 5},
            {"version": 1, "created_at": "2024-01-01 09:00:00", "feature_count": 4},
        ]
        mock_fg_class.return_value.get_or_create.return_value = mock_fg

        result = runner.invoke(app, ["version", "list", "test_fg", "--format", "json"])

        assert result.exit_code == 0
        # Should be valid JSON
        output = json.loads(result.stdout)
        assert isinstance(output, list)
        assert len(output) == 2
        assert output[0]["version"] == 2

    @mock.patch("seeknal.cli.main.FeatureGroup")
    def test_version_list_with_limit(self, mock_fg_class):
        """Version list command should respect limit option."""
        # Setup mock with 5 versions
        mock_fg = mock.MagicMock()
        mock_fg.list_versions.return_value = [
            {"version": 5, "created_at": "2024-01-05", "feature_count": 5},
            {"version": 4, "created_at": "2024-01-04", "feature_count": 5},
            {"version": 3, "created_at": "2024-01-03", "feature_count": 5},
            {"version": 2, "created_at": "2024-01-02", "feature_count": 4},
            {"version": 1, "created_at": "2024-01-01", "feature_count": 3},
        ]
        mock_fg_class.return_value.get_or_create.return_value = mock_fg

        result = runner.invoke(
            app, ["version", "list", "test_fg", "--limit", "2", "--format", "json"]
        )

        assert result.exit_code == 0
        output = json.loads(result.stdout)
        assert len(output) == 2
        assert output[0]["version"] == 5
        assert output[1]["version"] == 4

    @mock.patch("seeknal.cli.main.FeatureGroup")
    def test_version_list_empty_versions(self, mock_fg_class):
        """Version list command should handle feature group with no versions."""
        mock_fg = mock.MagicMock()
        mock_fg.list_versions.return_value = []
        mock_fg_class.return_value.get_or_create.return_value = mock_fg

        result = runner.invoke(app, ["version", "list", "test_fg"])

        assert result.exit_code == 0
        assert "No versions found" in result.stdout

    @mock.patch("seeknal.cli.main.FeatureGroup")
    def test_version_list_handles_error(self, mock_fg_class):
        """Version list command should handle errors gracefully."""
        mock_fg_class.return_value.get_or_create.side_effect = Exception(
            "Feature group not found"
        )

        result = runner.invoke(app, ["version", "list", "nonexistent_fg"])

        assert result.exit_code == 1
        assert "Error listing versions" in result.stdout


class TestVersionShowCommand:
    """Tests for the version show command."""

    def test_version_show_requires_feature_group(self):
        """Version show command should require feature group argument."""
        result = runner.invoke(app, ["version", "show"])
        assert result.exit_code != 0

    def test_version_show_displays_help(self):
        """Version show command should display help."""
        result = runner.invoke(app, ["version", "show", "--help"])
        assert result.exit_code == 0
        assert "FEATURE_GROUP" in result.stdout
        assert "--version" in result.stdout
        assert "--format" in result.stdout

    @mock.patch("seeknal.cli.main.FeatureGroup")
    def test_version_show_displays_version_details(self, mock_fg_class):
        """Version show command should display version details."""
        mock_fg = mock.MagicMock()
        mock_fg.get_version.return_value = {
            "version": 1,
            "created_at": "2024-01-01 09:00:00",
            "updated_at": "2024-01-01 10:00:00",
            "feature_count": 4,
            "avro_schema": {
                "type": "record",
                "fields": [
                    {"name": "field_a", "type": "int"},
                    {"name": "field_b", "type": "string"},
                ],
            },
        }
        mock_fg_class.return_value.get_or_create.return_value = mock_fg

        result = runner.invoke(app, ["version", "show", "test_fg", "--version", "1"])

        assert result.exit_code == 0
        assert "Feature Group:" in result.stdout
        assert "test_fg" in result.stdout
        assert "Version:" in result.stdout
        assert "Created At:" in result.stdout
        assert "Feature Count:" in result.stdout

    @mock.patch("seeknal.cli.main.FeatureGroup")
    def test_version_show_displays_schema_fields(self, mock_fg_class):
        """Version show command should display schema fields."""
        mock_fg = mock.MagicMock()
        mock_fg.get_version.return_value = {
            "version": 1,
            "created_at": "2024-01-01 09:00:00",
            "updated_at": "2024-01-01 10:00:00",
            "feature_count": 2,
            "avro_schema": {
                "type": "record",
                "fields": [
                    {"name": "field_a", "type": "int"},
                    {"name": "field_b", "type": "string"},
                ],
            },
        }
        mock_fg_class.return_value.get_or_create.return_value = mock_fg

        result = runner.invoke(app, ["version", "show", "test_fg", "--version", "1"])

        assert result.exit_code == 0
        assert "Schema:" in result.stdout
        assert "Fields:" in result.stdout
        assert "field_a" in result.stdout
        assert "field_b" in result.stdout

    @mock.patch("seeknal.cli.main.FeatureGroup")
    def test_version_show_json_format(self, mock_fg_class):
        """Version show command should support JSON output format."""
        mock_fg = mock.MagicMock()
        mock_fg.get_version.return_value = {
            "version": 1,
            "created_at": "2024-01-01 09:00:00",
            "updated_at": "2024-01-01 10:00:00",
            "feature_count": 4,
            "avro_schema": {"type": "record", "fields": []},
        }
        mock_fg_class.return_value.get_or_create.return_value = mock_fg

        result = runner.invoke(
            app, ["version", "show", "test_fg", "--version", "1", "--format", "json"]
        )

        assert result.exit_code == 0
        output = json.loads(result.stdout)
        assert output["version"] == 1
        assert "created_at" in output

    @mock.patch("seeknal.cli.main.FeatureGroup")
    def test_version_show_defaults_to_latest(self, mock_fg_class):
        """Version show command should default to latest version."""
        mock_fg = mock.MagicMock()
        mock_fg.list_versions.return_value = [
            {"version": 3, "created_at": "2024-01-03", "updated_at": "2024-01-03", "feature_count": 6, "avro_schema": None},
            {"version": 2, "created_at": "2024-01-02", "updated_at": "2024-01-02", "feature_count": 5, "avro_schema": None},
            {"version": 1, "created_at": "2024-01-01", "updated_at": "2024-01-01", "feature_count": 4, "avro_schema": None},
        ]
        mock_fg_class.return_value.get_or_create.return_value = mock_fg

        result = runner.invoke(app, ["version", "show", "test_fg"])

        assert result.exit_code == 0
        # Should show version 3 (latest)
        assert "Version: 3" in result.stdout or "3" in result.stdout

    @mock.patch("seeknal.cli.main.FeatureGroup")
    def test_version_show_nonexistent_version(self, mock_fg_class):
        """Version show command should handle non-existent version."""
        mock_fg = mock.MagicMock()
        mock_fg.get_version.return_value = None
        mock_fg_class.return_value.get_or_create.return_value = mock_fg

        result = runner.invoke(app, ["version", "show", "test_fg", "--version", "999"])

        assert result.exit_code == 1
        assert "not found" in result.stdout

    @mock.patch("seeknal.cli.main.FeatureGroup")
    def test_version_show_no_versions_exist(self, mock_fg_class):
        """Version show command should handle feature group with no versions."""
        mock_fg = mock.MagicMock()
        mock_fg.list_versions.return_value = []
        mock_fg_class.return_value.get_or_create.return_value = mock_fg

        result = runner.invoke(app, ["version", "show", "test_fg"])

        assert result.exit_code == 0
        assert "No versions found" in result.stdout

    @mock.patch("seeknal.cli.main.FeatureGroup")
    def test_version_show_handles_nullable_types(self, mock_fg_class):
        """Version show command should handle nullable union types in schema."""
        mock_fg = mock.MagicMock()
        mock_fg.get_version.return_value = {
            "version": 1,
            "created_at": "2024-01-01 09:00:00",
            "updated_at": "2024-01-01 10:00:00",
            "feature_count": 1,
            "avro_schema": {
                "type": "record",
                "fields": [
                    {"name": "nullable_field", "type": ["null", "string"]},
                ],
            },
        }
        mock_fg_class.return_value.get_or_create.return_value = mock_fg

        result = runner.invoke(app, ["version", "show", "test_fg", "--version", "1"])

        assert result.exit_code == 0
        assert "nullable_field" in result.stdout
        # Union types should be displayed as "null | string"
        assert "null" in result.stdout


class TestVersionDiffCommand:
    """Tests for the version diff command."""

    def test_version_diff_requires_feature_group(self):
        """Version diff command should require feature group argument."""
        result = runner.invoke(app, ["version", "diff"])
        assert result.exit_code != 0

    def test_version_diff_requires_from_and_to(self):
        """Version diff command should require both --from and --to options."""
        result = runner.invoke(app, ["version", "diff", "test_fg"])
        assert result.exit_code != 0

        result = runner.invoke(app, ["version", "diff", "test_fg", "--from", "1"])
        assert result.exit_code != 0

        result = runner.invoke(app, ["version", "diff", "test_fg", "--to", "2"])
        assert result.exit_code != 0

    def test_version_diff_displays_help(self):
        """Version diff command should display help."""
        result = runner.invoke(app, ["version", "diff", "--help"])
        assert result.exit_code == 0
        assert "FEATURE_GROUP" in result.stdout
        assert "--from" in result.stdout
        assert "--to" in result.stdout
        assert "--format" in result.stdout

    @mock.patch("seeknal.cli.main.FeatureGroup")
    def test_version_diff_shows_no_changes(self, mock_fg_class):
        """Version diff command should show no changes for identical schemas."""
        mock_fg = mock.MagicMock()
        mock_fg.compare_versions.return_value = {
            "added": [],
            "removed": [],
            "modified": [],
        }
        mock_fg_class.return_value.get_or_create.return_value = mock_fg

        result = runner.invoke(
            app, ["version", "diff", "test_fg", "--from", "1", "--to", "2"]
        )

        assert result.exit_code == 0
        assert "No schema changes" in result.stdout

    @mock.patch("seeknal.cli.main.FeatureGroup")
    def test_version_diff_shows_added_fields(self, mock_fg_class):
        """Version diff command should show added fields."""
        mock_fg = mock.MagicMock()
        mock_fg.compare_versions.return_value = {
            "added": [{"name": "new_field", "type": "string"}],
            "removed": [],
            "modified": [],
        }
        mock_fg_class.return_value.get_or_create.return_value = mock_fg

        result = runner.invoke(
            app, ["version", "diff", "test_fg", "--from", "1", "--to", "2"]
        )

        assert result.exit_code == 0
        assert "Added" in result.stdout
        assert "new_field" in result.stdout

    @mock.patch("seeknal.cli.main.FeatureGroup")
    def test_version_diff_shows_removed_fields(self, mock_fg_class):
        """Version diff command should show removed fields."""
        mock_fg = mock.MagicMock()
        mock_fg.compare_versions.return_value = {
            "added": [],
            "removed": [{"name": "old_field", "type": "int"}],
            "modified": [],
        }
        mock_fg_class.return_value.get_or_create.return_value = mock_fg

        result = runner.invoke(
            app, ["version", "diff", "test_fg", "--from", "1", "--to", "2"]
        )

        assert result.exit_code == 0
        assert "Removed" in result.stdout
        assert "old_field" in result.stdout

    @mock.patch("seeknal.cli.main.FeatureGroup")
    def test_version_diff_shows_modified_fields(self, mock_fg_class):
        """Version diff command should show modified fields."""
        mock_fg = mock.MagicMock()
        mock_fg.compare_versions.return_value = {
            "added": [],
            "removed": [],
            "modified": [
                {"field": "changed_field", "old_type": "int", "new_type": "string"}
            ],
        }
        mock_fg_class.return_value.get_or_create.return_value = mock_fg

        result = runner.invoke(
            app, ["version", "diff", "test_fg", "--from", "1", "--to", "2"]
        )

        assert result.exit_code == 0
        assert "Modified" in result.stdout
        assert "changed_field" in result.stdout
        assert "int" in result.stdout
        assert "string" in result.stdout

    @mock.patch("seeknal.cli.main.FeatureGroup")
    def test_version_diff_shows_summary(self, mock_fg_class):
        """Version diff command should show summary of changes."""
        mock_fg = mock.MagicMock()
        mock_fg.compare_versions.return_value = {
            "added": [{"name": "field_a", "type": "string"}],
            "removed": [{"name": "field_b", "type": "int"}],
            "modified": [{"field": "field_c", "old_type": "int", "new_type": "long"}],
        }
        mock_fg_class.return_value.get_or_create.return_value = mock_fg

        result = runner.invoke(
            app, ["version", "diff", "test_fg", "--from", "1", "--to", "2"]
        )

        assert result.exit_code == 0
        assert "Summary:" in result.stdout
        assert "1 added" in result.stdout
        assert "1 removed" in result.stdout
        assert "1 modified" in result.stdout

    @mock.patch("seeknal.cli.main.FeatureGroup")
    def test_version_diff_json_format(self, mock_fg_class):
        """Version diff command should support JSON output format."""
        mock_fg = mock.MagicMock()
        mock_fg.compare_versions.return_value = {
            "added": [{"name": "new_field", "type": "string"}],
            "removed": [],
            "modified": [],
        }
        mock_fg_class.return_value.get_or_create.return_value = mock_fg

        result = runner.invoke(
            app, ["version", "diff", "test_fg", "--from", "1", "--to", "2", "--format", "json"]
        )

        assert result.exit_code == 0
        output = json.loads(result.stdout)
        assert "added" in output
        assert "removed" in output
        assert "modified" in output
        assert len(output["added"]) == 1

    @mock.patch("seeknal.cli.main.FeatureGroup")
    def test_version_diff_same_version_error(self, mock_fg_class):
        """Version diff command should fail when comparing same version."""
        mock_fg = mock.MagicMock()
        mock_fg.compare_versions.side_effect = ValueError(
            "from_version and to_version must be different"
        )
        mock_fg_class.return_value.get_or_create.return_value = mock_fg

        result = runner.invoke(
            app, ["version", "diff", "test_fg", "--from", "1", "--to", "1"]
        )

        assert result.exit_code == 1
        assert "must be different" in result.stdout

    @mock.patch("seeknal.cli.main.FeatureGroup")
    def test_version_diff_nonexistent_version(self, mock_fg_class):
        """Version diff command should fail for non-existent version."""
        mock_fg = mock.MagicMock()
        mock_fg.compare_versions.side_effect = ValueError("Version 999 not found")
        mock_fg_class.return_value.get_or_create.return_value = mock_fg

        result = runner.invoke(
            app, ["version", "diff", "test_fg", "--from", "999", "--to", "1"]
        )

        assert result.exit_code == 1
        assert "Version 999 not found" in result.stdout

    @mock.patch("seeknal.cli.main.FeatureGroup")
    def test_version_diff_handles_error(self, mock_fg_class):
        """Version diff command should handle errors gracefully."""
        mock_fg_class.return_value.get_or_create.side_effect = Exception(
            "Feature group not found"
        )

        result = runner.invoke(
            app, ["version", "diff", "nonexistent_fg", "--from", "1", "--to", "2"]
        )

        assert result.exit_code == 1
        assert "Error comparing versions" in result.stdout


class TestVersionSubcommandHelp:
    """Tests for version subcommand help output."""

    def test_version_help(self):
        """Version command group should display available subcommands."""
        result = runner.invoke(app, ["version", "--help"])
        assert result.exit_code == 0
        assert "list" in result.stdout
        assert "show" in result.stdout
        assert "diff" in result.stdout
        assert "Manage feature group versions" in result.stdout

    def test_main_help_shows_version_command(self):
        """Main help should display version command."""
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "version" in result.stdout


class TestVersionOutputFormats:
    """Tests for output format handling in version commands."""

    @mock.patch("seeknal.cli.main.FeatureGroup")
    def test_version_list_invalid_format_rejected(self, mock_fg_class):
        """Version list command should reject invalid format."""
        result = runner.invoke(
            app, ["version", "list", "test_fg", "--format", "invalid"]
        )
        assert result.exit_code != 0

    @mock.patch("seeknal.cli.main.FeatureGroup")
    def test_version_show_invalid_format_rejected(self, mock_fg_class):
        """Version show command should reject invalid format."""
        result = runner.invoke(
            app, ["version", "show", "test_fg", "--format", "invalid"]
        )
        assert result.exit_code != 0

    @mock.patch("seeknal.cli.main.FeatureGroup")
    def test_version_diff_invalid_format_rejected(self, mock_fg_class):
        """Version diff command should reject invalid format."""
        result = runner.invoke(
            app,
            ["version", "diff", "test_fg", "--from", "1", "--to", "2", "--format", "invalid"],
        )
        assert result.exit_code != 0
