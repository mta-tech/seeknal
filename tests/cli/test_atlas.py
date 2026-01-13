"""Tests for Atlas CLI integration with Seeknal.

This module tests the Atlas Data Platform CLI commands that are
integrated into the Seeknal CLI.
"""

import pytest
from typer.testing import CliRunner
from unittest.mock import patch, MagicMock

from seeknal.cli.main import app
from seeknal.cli.atlas import atlas_app


runner = CliRunner()


class TestAtlasCliIntegration:
    """Test Atlas CLI integration with main Seeknal CLI."""

    def test_atlas_command_appears_in_help(self):
        """Test that 'atlas' command appears in main CLI help."""
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "atlas" in result.output.lower()

    def test_atlas_help(self):
        """Test 'seeknal atlas --help' works."""
        result = runner.invoke(app, ["atlas", "--help"])
        assert result.exit_code == 0
        assert "api" in result.output.lower()
        assert "governance" in result.output.lower()
        assert "lineage" in result.output.lower()


class TestAtlasInfoCommand:
    """Test 'seeknal atlas info' command."""

    def test_atlas_info_shows_status(self):
        """Test that atlas info shows installation status."""
        result = runner.invoke(app, ["atlas", "info"])
        assert result.exit_code == 0
        assert "Atlas" in result.output
        # Should show either installed or not installed
        assert "installed" in result.output.lower()


class TestAtlasApiCommands:
    """Test 'seeknal atlas api' commands."""

    def test_api_help(self):
        """Test 'seeknal atlas api --help' works."""
        result = runner.invoke(app, ["atlas", "api", "--help"])
        assert result.exit_code == 0
        assert "start" in result.output.lower()
        assert "status" in result.output.lower()

    def test_api_start_help(self):
        """Test 'seeknal atlas api start --help' shows options."""
        result = runner.invoke(app, ["atlas", "api", "start", "--help"])
        assert result.exit_code == 0
        assert "--host" in result.output
        assert "--port" in result.output
        assert "--reload" in result.output

    @patch("httpx.get")
    def test_api_status_connection_error(self, mock_get):
        """Test api status when server is not running."""
        import httpx
        mock_get.side_effect = httpx.ConnectError("Connection refused")

        result = runner.invoke(app, ["atlas", "api", "status"])
        assert result.exit_code == 1
        assert "cannot connect" in result.output.lower()


class TestAtlasGovernanceCommands:
    """Test 'seeknal atlas governance' commands."""

    def test_governance_help(self):
        """Test 'seeknal atlas governance --help' works."""
        result = runner.invoke(app, ["atlas", "governance", "--help"])
        assert result.exit_code == 0
        assert "stats" in result.output.lower()
        assert "policies" in result.output.lower()
        assert "violations" in result.output.lower()

    def test_governance_stats_help(self):
        """Test 'seeknal atlas governance stats --help' shows options."""
        result = runner.invoke(app, ["atlas", "governance", "stats", "--help"])
        assert result.exit_code == 0
        assert "--host" in result.output
        assert "--port" in result.output
        assert "--format" in result.output

    def test_governance_policies_help(self):
        """Test 'seeknal atlas governance policies --help' shows options."""
        result = runner.invoke(app, ["atlas", "governance", "policies", "--help"])
        assert result.exit_code == 0
        assert "--status" in result.output
        assert "--type" in result.output

    def test_governance_violations_help(self):
        """Test 'seeknal atlas governance violations --help' shows options."""
        result = runner.invoke(app, ["atlas", "governance", "violations", "--help"])
        assert result.exit_code == 0
        assert "--severity" in result.output
        assert "--status" in result.output


class TestAtlasLineageCommands:
    """Test 'seeknal atlas lineage' commands."""

    def test_lineage_help(self):
        """Test 'seeknal atlas lineage --help' works."""
        result = runner.invoke(app, ["atlas", "lineage", "--help"])
        assert result.exit_code == 0
        assert "show" in result.output.lower()
        assert "publish" in result.output.lower()

    def test_lineage_show_help(self):
        """Test 'seeknal atlas lineage show --help' shows options."""
        result = runner.invoke(app, ["atlas", "lineage", "show", "--help"])
        assert result.exit_code == 0
        assert "--direction" in result.output
        assert "--depth" in result.output

    def test_lineage_publish_help(self):
        """Test 'seeknal atlas lineage publish --help' shows options."""
        result = runner.invoke(app, ["atlas", "lineage", "publish", "--help"])
        assert result.exit_code == 0
        assert "--inputs" in result.output
        assert "--outputs" in result.output
        assert "--run-id" in result.output

    def test_lineage_publish_requires_inputs_and_outputs(self):
        """Test lineage publish fails without inputs/outputs."""
        result = runner.invoke(app, ["atlas", "lineage", "publish", "test_pipeline"])
        assert result.exit_code == 1
        assert "inputs" in result.output.lower() or "outputs" in result.output.lower()


class TestAtlasModuleAvailability:
    """Test Atlas module availability checks."""

    def test_check_atlas_installed(self):
        """Test that _check_atlas_installed returns correct status."""
        from seeknal.cli.atlas import _check_atlas_installed
        # Since Atlas is installed in test env, should return True
        assert _check_atlas_installed() == True

    def test_helper_functions_exist(self):
        """Test that helper functions exist in atlas module."""
        from seeknal.cli import atlas
        assert hasattr(atlas, "_echo_error")
        assert hasattr(atlas, "_echo_success")
        assert hasattr(atlas, "_echo_info")
        assert hasattr(atlas, "_echo_warning")
        assert hasattr(atlas, "_require_atlas")


class TestAtlasAppExport:
    """Test that atlas_app is properly exported."""

    def test_atlas_app_is_typer(self):
        """Test that atlas_app is a Typer instance."""
        import typer
        assert isinstance(atlas_app, typer.Typer)

    def test_atlas_app_has_commands(self):
        """Test that atlas_app has expected command groups."""
        # Test by invoking help - this validates the structure
        result = runner.invoke(atlas_app, ["--help"])
        assert result.exit_code == 0
        assert "api" in result.output
        assert "governance" in result.output
        assert "lineage" in result.output
