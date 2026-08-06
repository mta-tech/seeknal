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

    @staticmethod
    def _compiled_draft():
        return {
            "serviceId": "customer-risk",
            "version": "1",
            "variant": "default",
            "owner": "ml-platform",
            "entityKeys": [
                {
                    "semanticName": "customer_id",
                    "physicalName": "customer_id",
                    "dataType": "string",
                    "ordinal": 0,
                }
            ],
            "requestFields": [],
            "selections": [
                {
                    "view": {
                        "viewId": "customer_profile",
                        "revision": "a" * 64,
                        "sourceLocator": (
                            "seeknal:feature-group:customer_profile:" + "a" * 64
                        ),
                    },
                    "features": ["age"],
                    "ordinal": 0,
                }
            ],
            "executionModes": ["batch"],
            "transformationOrder": [],
        }

    def test_plan_and_compile_use_applied_selector(self, tmp_path):
        compiled = MagicMock(payload=self._compiled_draft())
        with patch(
            "seeknal.integrations.atlas_feature_service.FeatureServiceCompiler"
        ) as compiler:
            compiler.return_value.compile.return_value = compiled
            planned = runner.invoke(
                app,
                [
                    "atlas",
                    "feature-service",
                    "plan",
                    "feature_service.customer-risk",
                    "--project-dir",
                    str(tmp_path),
                ],
            )
            emitted = runner.invoke(
                app,
                [
                    "atlas",
                    "feature-service",
                    "compile",
                    "feature_service.customer-risk",
                    "--project-dir",
                    str(tmp_path),
                ],
            )

        assert planned.exit_code == 0
        assert "publishable from applied state" in planned.output
        assert emitted.exit_code == 0
        assert '"serviceId": "customer-risk"' in emitted.output

    def test_publish_selector_compiles_before_sending(self, tmp_path):
        compiled = MagicMock(payload=self._compiled_draft())
        client = MagicMock()
        client.publish_feature_service.return_value = {
            "service": {
                "serviceId": "customer-risk",
                "version": "1",
                "variant": "default",
            },
            "replayed": False,
        }
        with (
            patch(
                "seeknal.integrations.atlas_feature_service.FeatureServiceCompiler"
            ) as compiler,
            patch(
                "seeknal.integrations.atlas_client.create_atlas_contract_client_from_env",
                return_value=client,
            ),
        ):
            compiler.return_value.compile.return_value = compiled
            result = runner.invoke(
                app,
                [
                    "atlas",
                    "feature-service",
                    "publish",
                    "feature_service.customer-risk",
                    "--project-dir",
                    str(tmp_path),
                ],
            )

        assert result.exit_code == 0
        compiler.assert_called_once_with(tmp_path.resolve(), environment=None)
        compiler.return_value.compile.assert_called_once_with(
            "feature_service.customer-risk"
        )
        client.publish_feature_service.assert_called_once_with(
            self._compiled_draft()
        )

    def test_publish_can_create_activation_request_without_granting_access(
        self, tmp_path
    ):
        compiled = MagicMock(payload=self._compiled_draft())
        client = MagicMock()
        client.publish_feature_service.return_value = {
            "service": {
                "serviceId": "customer-risk",
                "version": "1",
                "variant": "default",
            },
            "replayed": False,
            "activationPath": (
                "/feature-store/services/customer-risk/versions/1/"
                "variants/default#activation"
            ),
        }
        client.request_feature_service_activation.return_value = {
            "request": {
                "requestId": "48d2e044-8376-4f1f-8f22-9f01ac491901",
                "state": "pending_owner_review",
            }
        }
        with (
            patch(
                "seeknal.integrations.atlas_feature_service.FeatureServiceCompiler"
            ) as compiler,
            patch(
                "seeknal.integrations.atlas_client.create_atlas_contract_client_from_env",
                return_value=client,
            ),
        ):
            compiler.return_value.compile.return_value = compiled
            result = runner.invoke(
                app,
                [
                    "atlas",
                    "feature-service",
                    "publish",
                    "feature_service.customer-risk",
                    "--project-dir",
                    str(tmp_path),
                    "--environment",
                    "production",
                    "--request-activation",
                    "--consumer",
                    "recommendation-api",
                    "--capability",
                    "consume_online",
                    "--capability",
                    "operate",
                ],
            )

        assert result.exit_code == 0
        assert "Activation:" in result.output
        assert "pending_owner_review" in result.output
        client.request_feature_service_activation.assert_called_once_with(
            service_id="customer-risk",
            version="1",
            variant="default",
            environment="production",
            consumer_identity="recommendation-api",
            consumer_kind="application",
            capabilities=("consume_online", "operate"),
            justification="Requested by the Seeknal publish command.",
        )

    def test_publish_rejects_legacy_raw_contract_before_http(self, tmp_path):
        path = tmp_path / "customer-risk.yml"
        path.write_text(
            """\
schemaVersion: 1
serviceId: customer-risk
version: "1"
variant: default
""",
            encoding="utf-8",
        )
        with patch(
            "seeknal.integrations.atlas_client.create_atlas_contract_client_from_env"
        ) as factory:
            result = runner.invoke(
                app, ["atlas", "feature-service", "publish", str(path)]
            )

        assert result.exit_code == 1
        assert "schemaVersion: 1 YAML" in result.output
        assert "YAML or the Python builder" in result.output
        assert "feature_service.<name> selector" in result.output
        factory.assert_not_called()

    def test_publish_reports_replayed(self, tmp_path):
        compiled = MagicMock(payload=self._compiled_draft())
        client = MagicMock()
        client.publish_feature_service.return_value = {
            "service": {
                "serviceId": "customer-risk",
                "version": "1",
                "variant": "default",
            },
            "replayed": True,
        }

        with (
            patch(
                "seeknal.integrations.atlas_feature_service.FeatureServiceCompiler"
            ) as compiler,
            patch(
                "seeknal.integrations.atlas_client.create_atlas_contract_client_from_env",
                return_value=client,
            ),
        ):
            compiler.return_value.compile.return_value = compiled
            result = runner.invoke(
                app,
                [
                    "atlas",
                    "feature-service",
                    "publish",
                    "feature_service.customer-risk",
                    "--project-dir",
                    str(tmp_path),
                ],
            )

        assert result.exit_code == 0
        assert "Replayed Feature Service" in result.output

    def test_publish_fails_clearly_without_atlas_config(self, tmp_path):
        compiled = MagicMock(payload=self._compiled_draft())

        with (
            patch(
                "seeknal.integrations.atlas_feature_service.FeatureServiceCompiler"
            ) as compiler,
            patch(
                "seeknal.integrations.atlas_client.create_atlas_contract_client_from_env",
                return_value=None,
            ),
        ):
            compiler.return_value.compile.return_value = compiled
            result = runner.invoke(
                app,
                [
                    "atlas",
                    "feature-service",
                    "publish",
                    "feature_service.customer-risk",
                    "--project-dir",
                    str(tmp_path),
                ],
            )

        assert result.exit_code == 2
        assert "Atlas is not configured" in result.output
        assert "ATLAS_API_URL" in result.output

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

        compiled = MagicMock(payload=self._compiled_draft())
        client = MagicMock()
        client.publish_feature_service.side_effect = (
            AtlasAuthError(SESSION_EXPIRED_HINT)
            if error == "auth"
            else AtlasContractError("backend unavailable")
        )

        with (
            patch(
                "seeknal.integrations.atlas_feature_service.FeatureServiceCompiler"
            ) as compiler,
            patch(
                "seeknal.integrations.atlas_client.create_atlas_contract_client_from_env",
                return_value=client,
            ),
        ):
            compiler.return_value.compile.return_value = compiled
            result = runner.invoke(
                app,
                [
                    "atlas",
                    "feature-service",
                    "publish",
                    "feature_service.customer-risk",
                    "--project-dir",
                    str(tmp_path),
                ],
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
