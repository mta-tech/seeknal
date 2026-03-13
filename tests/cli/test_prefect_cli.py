"""
Tests for Prefect CLI commands (seeknal prefect serve/deploy/generate).

Tests CLI argument parsing and error handling with mocked SeeknalPrefectFlow.
"""
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from seeknal.cli.main import app

runner = CliRunner()


class TestPrefectServe:
    """Test seeknal prefect serve command."""

    @patch("seeknal.cli.main.SeeknalPrefectFlow")
    def test_serve_basic(self, mock_spf_cls):
        """Basic serve command creates flow and calls serve()."""
        mock_spf = MagicMock()
        mock_spf_cls.return_value = mock_spf

        result = runner.invoke(app, [
            "prefect", "serve",
            "--project-path", "/tmp/test",
            "--cron", "0 2 * * *",
        ])

        assert result.exit_code == 0, result.output
        mock_spf_cls.assert_called_once()
        mock_spf.serve.assert_called_once()
        call_kwargs = mock_spf.serve.call_args[1]
        assert call_kwargs["cron"] == "0 2 * * *"

    @patch("seeknal.cli.main.SeeknalPrefectFlow")
    def test_serve_with_all_options(self, mock_spf_cls):
        """Serve command passes all options correctly."""
        mock_spf = MagicMock()
        mock_spf_cls.return_value = mock_spf

        result = runner.invoke(app, [
            "prefect", "serve",
            "--project-path", "/tmp/test",
            "--name", "my-deploy",
            "--cron", "0 2 * * *",
            "--interval", "3600",
            "--full",
            "--continue-on-error",
            "--max-workers", "4",
            "--env", "staging",
            "--profile", "/tmp/profile.yml",
            "--params", '{"key": "val"}',
        ])

        assert result.exit_code == 0, result.output

        # Check SeeknalPrefectFlow constructor args
        init_kwargs = mock_spf_cls.call_args[1]
        assert init_kwargs["max_workers"] == 4
        assert init_kwargs["continue_on_error"] is True
        assert init_kwargs["env"] == "staging"
        assert init_kwargs["profile_path"] == "/tmp/profile.yml"
        assert init_kwargs["full_refresh"] is True
        assert init_kwargs["params"] == {"key": "val"}

        # Check serve() args
        serve_kwargs = mock_spf.serve.call_args[1]
        assert serve_kwargs["name"] == "my-deploy"
        assert serve_kwargs["cron"] == "0 2 * * *"
        assert serve_kwargs["interval"] == 3600

    @patch("seeknal.cli.main.SeeknalPrefectFlow")
    def test_serve_prefect_not_installed(self, mock_spf_cls):
        """Serve shows error when Prefect is not installed."""
        mock_spf_cls.side_effect = ImportError(
            "Prefect not installed. Install with: pip install seeknal[prefect]"
        )

        result = runner.invoke(app, [
            "prefect", "serve",
            "--project-path", "/tmp/test",
        ])

        assert result.exit_code != 0


class TestPrefectDeploy:
    """Test seeknal prefect deploy command."""

    @patch("seeknal.cli.main.SeeknalPrefectFlow")
    def test_deploy_basic(self, mock_spf_cls):
        """Basic deploy command creates flow and calls deploy()."""
        mock_spf = MagicMock()
        mock_spf_cls.return_value = mock_spf

        result = runner.invoke(app, [
            "prefect", "deploy",
            "--project-path", "/tmp/test",
            "--work-pool", "my-pool",
        ])

        assert result.exit_code == 0, result.output
        mock_spf.deploy.assert_called_once()
        call_kwargs = mock_spf.deploy.call_args[1]
        assert call_kwargs["work_pool"] == "my-pool"

    @patch("seeknal.cli.main.SeeknalPrefectFlow")
    def test_deploy_exposure(self, mock_spf_cls):
        """Deploy with --exposure flag calls deploy_exposure()."""
        mock_spf = MagicMock()
        mock_spf_cls.return_value = mock_spf

        result = runner.invoke(app, [
            "prefect", "deploy",
            "--project-path", "/tmp/test",
            "--work-pool", "my-pool",
            "--exposure", "weekly_kpis",
        ])

        assert result.exit_code == 0, result.output
        mock_spf.deploy_exposure.assert_called_once_with("weekly_kpis", work_pool="my-pool")
        mock_spf.deploy.assert_not_called()

    @patch("seeknal.cli.main.SeeknalPrefectFlow")
    def test_deploy_with_schedule(self, mock_spf_cls):
        """Deploy with cron schedule passes it through."""
        mock_spf = MagicMock()
        mock_spf_cls.return_value = mock_spf

        result = runner.invoke(app, [
            "prefect", "deploy",
            "--project-path", "/tmp/test",
            "--work-pool", "my-pool",
            "--cron", "0 8 * * MON",
        ])

        assert result.exit_code == 0, result.output
        call_kwargs = mock_spf.deploy.call_args[1]
        assert call_kwargs["cron"] == "0 8 * * MON"


class TestPrefectGenerate:
    """Test seeknal prefect generate command."""

    @patch("seeknal.workflow.prefect_integration.SeeknalPrefectFlow")
    def test_generate_writes_yaml(self, mock_spf_cls, tmp_path):
        """Generate command writes a prefect.yaml file."""
        output_path = tmp_path / "prefect.yaml"

        result = runner.invoke(app, [
            "prefect", "generate",
            "--project-path", str(tmp_path),
            "--output", str(output_path),
        ])

        assert result.exit_code == 0, result.output
        assert output_path.exists()

        import yaml
        content = yaml.safe_load(output_path.read_text())
        assert "deployments" in content

    def test_generate_default_output(self, tmp_path):
        """Generate writes to project_path/prefect.yaml by default."""
        result = runner.invoke(app, [
            "prefect", "generate",
            "--project-path", str(tmp_path),
        ])

        assert result.exit_code == 0, result.output
        assert (tmp_path / "prefect.yaml").exists()


class TestPrefectNoArgsHelp:
    """Test that seeknal prefect shows help with no subcommand."""

    def test_no_args_shows_help(self):
        """Running seeknal prefect with no args shows help."""
        result = runner.invoke(app, ["prefect"])
        # Should show help (not error)
        assert "serve" in result.output or "deploy" in result.output or result.exit_code == 0


class TestExposureScheduleValidation:
    """Test schedule field validation in exposure YAML."""

    def test_valid_schedule(self):
        """Valid schedule with cron field passes validation."""
        from seeknal.ask.report.exposure import _validate_report_exposure

        data = {
            "kind": "exposure",
            "type": "report",
            "name": "test",
            "sections": [{"title": "Test"}],
            "schedule": {"cron": "0 8 * * MON"},
        }
        result = _validate_report_exposure(data, Path("test.yml"))
        assert result["schedule"]["cron"] == "0 8 * * MON"

    def test_schedule_with_timezone(self):
        """Schedule with timezone is preserved."""
        from seeknal.ask.report.exposure import _validate_report_exposure

        data = {
            "kind": "exposure",
            "type": "report",
            "name": "test",
            "sections": [{"title": "Test"}],
            "schedule": {"cron": "0 8 * * MON", "timezone": "US/Eastern"},
        }
        result = _validate_report_exposure(data, Path("test.yml"))
        assert result["schedule"]["timezone"] == "US/Eastern"

    def test_schedule_without_cron_fails(self):
        """Schedule without cron field fails validation."""
        from seeknal.ask.report.exposure import _validate_report_exposure

        data = {
            "kind": "exposure",
            "type": "report",
            "name": "test",
            "sections": [{"title": "Test"}],
            "schedule": {"timezone": "UTC"},
        }
        with pytest.raises(ValueError, match="schedule.cron is required"):
            _validate_report_exposure(data, Path("test.yml"))

    def test_invalid_cron_format(self):
        """Invalid cron (wrong number of fields) fails validation."""
        from seeknal.ask.report.exposure import _validate_report_exposure

        data = {
            "kind": "exposure",
            "type": "report",
            "name": "test",
            "sections": [{"title": "Test"}],
            "schedule": {"cron": "0 8 *"},  # Only 3 fields
        }
        with pytest.raises(ValueError, match="5 fields"):
            _validate_report_exposure(data, Path("test.yml"))

    def test_schedule_not_dict_fails(self):
        """Schedule as a string fails validation."""
        from seeknal.ask.report.exposure import _validate_report_exposure

        data = {
            "kind": "exposure",
            "type": "report",
            "name": "test",
            "sections": [{"title": "Test"}],
            "schedule": "0 8 * * MON",
        }
        with pytest.raises(ValueError, match="schedule must be a mapping"):
            _validate_report_exposure(data, Path("test.yml"))

    def test_no_schedule_still_works(self):
        """Exposures without schedule continue to work."""
        from seeknal.ask.report.exposure import _validate_report_exposure

        data = {
            "kind": "exposure",
            "type": "report",
            "name": "test",
            "sections": [{"title": "Test"}],
        }
        result = _validate_report_exposure(data, Path("test.yml"))
        assert "schedule" not in result or result.get("schedule") is None
