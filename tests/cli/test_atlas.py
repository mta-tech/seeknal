"""Tests for Atlas CLI integration with Seeknal.

This module tests the Atlas Data Platform CLI commands that are
integrated into the Seeknal CLI.
"""

import re

import pytest
from typer.testing import CliRunner
from unittest.mock import patch, MagicMock

from seeknal.cli.main import app
from seeknal.cli.atlas import atlas_app


runner = CliRunner()


_ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")


def _assert_help_option(output: str, name: str) -> None:
    # Rich/Typer may split long option names across separately styled ANSI spans
    # in CI output (for example ``--run-id`` as ``-`` + ``-run`` + ``-id``).
    # Strip styling and compare a compact form so assertions stay focused on
    # whether the option is present rather than terminal rendering details.
    plain = _ANSI_ESCAPE_RE.sub("", output)
    compact = re.sub(r"[\s-]+", "", plain)
    assert name.replace("-", "") in compact


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
        assert "feature-service" in result.output.lower()
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
        _assert_help_option(result.output, "host")
        _assert_help_option(result.output, "port")
        _assert_help_option(result.output, "reload")

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
        _assert_help_option(result.output, "host")
        _assert_help_option(result.output, "port")
        _assert_help_option(result.output, "format")

    def test_governance_policies_help(self):
        """Test 'seeknal atlas governance policies --help' shows options."""
        result = runner.invoke(app, ["atlas", "governance", "policies", "--help"])
        assert result.exit_code == 0
        _assert_help_option(result.output, "status")
        _assert_help_option(result.output, "type")

    def test_governance_violations_help(self):
        """Test 'seeknal atlas governance violations --help' shows options."""
        result = runner.invoke(app, ["atlas", "governance", "violations", "--help"])
        assert result.exit_code == 0
        _assert_help_option(result.output, "severity")
        _assert_help_option(result.output, "status")


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
        _assert_help_option(result.output, "direction")
        _assert_help_option(result.output, "depth")

    def test_lineage_publish_help(self):
        """Test 'seeknal atlas lineage publish --help' shows options."""
        result = runner.invoke(app, ["atlas", "lineage", "publish", "--help"])
        assert result.exit_code == 0
        _assert_help_option(result.output, "inputs")
        _assert_help_option(result.output, "outputs")
        _assert_help_option(result.output, "run-id")

    def test_lineage_publish_requires_inputs_and_outputs(self):
        """Test lineage publish fails without inputs/outputs."""
        result = runner.invoke(app, ["atlas", "lineage", "publish", "test_pipeline"])
        assert result.exit_code == 1
        assert "inputs" in result.output.lower() or "outputs" in result.output.lower()


class TestAtlasFeatureServiceCommands:
    """Test ``seeknal atlas feature-service`` commands."""

    def test_publish_sends_complete_multi_group_draft_without_schema_version(
        self, tmp_path
    ):
        path = tmp_path / "customer-risk.yml"
        path.write_text(
            """\
schemaVersion: 1
serviceId: customer-risk
version: "1"
variant: default
owner: ml-platform
entityKeys:
  - semanticName: customer_id
    physicalName: customer_id
    dataType: string
    ordinal: 0
selections:
  - view:
      viewId: customer_profile
      revision: "4"
      schemaRevision: "4"
      sourceLocator: seeknal:feature-group:customer_profile:4
      fields:
        - name: age
          dataType: int64
      entityKeys:
        - semanticName: customer_id
          physicalName: customer_id
          dataType: string
          ordinal: 0
    features: [age]
    ordinal: 0
  - view:
      viewId: customer_activity
      revision: "7"
      schemaRevision: "7"
      sourceLocator: seeknal:feature-group:customer_activity:7
      fields:
        - name: spend_30d
          dataType: float64
      entityKeys:
        - semanticName: customer_id
          physicalName: customer_id
          dataType: string
          ordinal: 0
    features: [spend_30d]
    ordinal: 1
""",
            encoding="utf-8",
        )
        client = MagicMock()
        client.publish_feature_service.return_value = {
            "service": {
                "serviceId": "customer-risk",
                "version": "1",
                "variant": "default",
            },
            "replayed": False,
            "servingAuthorization": {
                "provisionedByPublication": False,
                "reason": "policy_binding_required",
            },
        }

        with patch(
            "seeknal.integrations.atlas_client.create_atlas_contract_client_from_env",
            return_value=client,
        ):
            result = runner.invoke(
                app, ["atlas", "feature-service", "publish", str(path)]
            )

        assert result.exit_code == 0
        draft = client.publish_feature_service.call_args.args[0]
        assert "schemaVersion" not in draft
        assert [item["view"]["viewId"] for item in draft["selections"]] == [
            "customer_profile",
            "customer_activity",
        ]
        assert "Created Feature Service" in result.output
        assert "separate policy binding" in result.output

    def test_publish_reports_replayed(self, tmp_path):
        path = tmp_path / "service.yml"
        path.write_text(
            "schemaVersion: 1\nserviceId: customer-risk\nversion: '1'\n"
            "variant: default\n",
            encoding="utf-8",
        )
        client = MagicMock()
        client.publish_feature_service.return_value = {
            "service": {
                "serviceId": "customer-risk",
                "version": "1",
                "variant": "default",
            },
            "replayed": True,
        }

        with patch(
            "seeknal.integrations.atlas_client.create_atlas_contract_client_from_env",
            return_value=client,
        ):
            result = runner.invoke(
                app, ["atlas", "feature-service", "publish", str(path)]
            )

        assert result.exit_code == 0
        assert "Replayed Feature Service" in result.output

    @pytest.mark.parametrize("schema_version", ["2", "true", "null"])
    def test_publish_rejects_unsupported_schema_version(
        self, tmp_path, schema_version
    ):
        path = tmp_path / "service.yml"
        path.write_text(
            f"schemaVersion: {schema_version}\nserviceId: customer-risk\n",
            encoding="utf-8",
        )

        result = runner.invoke(
            app, ["atlas", "feature-service", "publish", str(path)]
        )

        assert result.exit_code == 1
        assert "expected schemaVersion: 1" in result.output

    def test_publish_fails_clearly_without_atlas_config(self, tmp_path):
        path = tmp_path / "service.yml"
        path.write_text(
            "schemaVersion: 1\nserviceId: customer-risk\n",
            encoding="utf-8",
        )

        with patch(
            "seeknal.integrations.atlas_client.create_atlas_contract_client_from_env",
            return_value=None,
        ):
            result = runner.invoke(
                app, ["atlas", "feature-service", "publish", str(path)]
            )

        assert result.exit_code == 2
        assert "Atlas is not configured" in result.output
        assert "ATLAS_API_URL" in result.output

    @pytest.mark.parametrize(
        "field",
        [
            "createdBy",
            "createdAt",
            "publishedAt",
            "schemaHash",
            "lifecycle",
            "deployment",
        ],
    )
    def test_publish_rejects_server_owned_fields_before_http(self, tmp_path, field):
        path = tmp_path / "service.yml"
        path.write_text(
            f"schemaVersion: 1\nserviceId: customer-risk\n{field}: rejected\n",
            encoding="utf-8",
        )

        with patch(
            "seeknal.integrations.atlas_client.create_atlas_contract_client_from_env"
        ) as factory:
            result = runner.invoke(
                app, ["atlas", "feature-service", "publish", str(path)]
            )

        assert result.exit_code == 1
        assert "server-owned field" in result.output
        assert field in result.output
        factory.assert_not_called()

    @pytest.mark.parametrize(
        ("error", "expected"),
        [
            (
                "auth",
                "Run `seeknal auth login`",
            ),
            (
                "server",
                "Failed to publish Feature Service",
            ),
        ],
    )
    def test_publish_handles_atlas_errors(self, tmp_path, error, expected):
        from seeknal.integrations.atlas_client import (
            SESSION_EXPIRED_HINT,
            AtlasAuthError,
            AtlasContractError,
        )

        path = tmp_path / "service.yml"
        path.write_text(
            "schemaVersion: 1\nserviceId: customer-risk\n",
            encoding="utf-8",
        )
        client = MagicMock()
        client.publish_feature_service.side_effect = (
            AtlasAuthError(SESSION_EXPIRED_HINT)
            if error == "auth"
            else AtlasContractError("backend unavailable")
        )

        with patch(
            "seeknal.integrations.atlas_client.create_atlas_contract_client_from_env",
            return_value=client,
        ):
            result = runner.invoke(
                app, ["atlas", "feature-service", "publish", str(path)]
            )

        assert result.exit_code == 1
        assert expected in result.output


class TestAtlasModuleAvailability:
    """Test Atlas module availability checks."""

    def test_check_atlas_installed(self):
        """Test that _check_atlas_installed returns correct status."""
        from seeknal.cli.atlas import _check_atlas_installed
        # Atlas is an optional integration; the default CI environment does not
        # install the private atlas-data-platform package.  The helper should
        # report the actual import availability instead of forcing the default
        # test environment to include that extra.
        try:
            import atlas.seeknal  # noqa: F401
            expected = True
        except ImportError:
            expected = False

        assert _check_atlas_installed() is expected

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
        assert "feature-service" in result.output
        assert "governance" in result.output
        assert "lineage" in result.output
