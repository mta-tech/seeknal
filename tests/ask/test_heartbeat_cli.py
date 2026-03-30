"""Tests for seeknal heartbeat CLI commands."""

import json
from unittest.mock import AsyncMock, patch

from typer.testing import CliRunner

runner = CliRunner()


class TestHeartbeatApp:
    def test_app_created(self):
        from seeknal.cli.heartbeat import heartbeat_app

        assert heartbeat_app is not None
        assert heartbeat_app.info.name == "heartbeat"

    def test_help_shows_subcommands(self):
        from seeknal.cli.heartbeat import heartbeat_app

        result = runner.invoke(heartbeat_app, ["--help"])
        assert result.exit_code == 0
        assert "run" in result.output
        assert "status" in result.output


class TestHeartbeatStatus:
    def test_no_state_shows_never_run(self, tmp_path):
        from seeknal.cli.heartbeat import heartbeat_app

        result = runner.invoke(
            heartbeat_app, ["status", "--project", str(tmp_path)]
        )
        assert result.exit_code == 0
        assert "Never run" in result.output

    def test_with_state_shows_details(self, tmp_path):
        from seeknal.cli.heartbeat import heartbeat_app

        # Create a state file
        seeknal_dir = tmp_path / ".seeknal"
        seeknal_dir.mkdir()
        state = {
            "last_run": "2026-03-27T10:00:00",
            "last_result": "ok",
            "last_message": "HEARTBEAT_OK all good",
        }
        (seeknal_dir / "heartbeat_state.json").write_text(
            json.dumps(state)
        )

        result = runner.invoke(
            heartbeat_app, ["status", "--project", str(tmp_path)]
        )
        assert result.exit_code == 0
        assert "2026-03-27T10:00:00" in result.output
        assert "ok" in result.output


class TestHeartbeatRun:
    def test_help_shows_options(self):
        from seeknal.cli.heartbeat import heartbeat_app

        result = runner.invoke(heartbeat_app, ["run", "--help"])
        assert result.exit_code == 0
        assert "--project" in result.output

    @patch(
        "seeknal.ask.gateway.heartbeat.runner.HeartbeatRunner.run_once",
        new_callable=AsyncMock,
        return_value="HEARTBEAT_OK all checks passed",
    )
    def test_run_prints_result(self, mock_run_once, tmp_path):
        from seeknal.cli.heartbeat import heartbeat_app

        with patch(
            "seeknal.ask.config.load_agent_config", return_value={}
        ):
            result = runner.invoke(
                heartbeat_app,
                ["run", "--project", str(tmp_path)],
            )

        assert result.exit_code == 0
        assert "Running heartbeat" in result.output
        assert "HEARTBEAT_OK" in result.output
        mock_run_once.assert_called_once()
        # Verify project_path was passed
        call_args = mock_run_once.call_args[0]
        assert str(call_args[0]) == str(tmp_path)


class TestRegisteredInMainApp:
    def test_heartbeat_registered(self):
        """Heartbeat sub-app is registered on the main CLI app."""
        from seeknal.cli.main import app

        group_names = [g.name for g in app.registered_groups]
        assert "heartbeat" in group_names


# ---------------------------------------------------------------------------
# Path validation tests
# ---------------------------------------------------------------------------


class TestPathValidation:
    def test_run_rejects_insecure_path(self):
        from seeknal.cli.heartbeat import heartbeat_app

        result = runner.invoke(
            heartbeat_app, ["run", "--project", "/tmp/evil"]
        )
        assert result.exit_code != 0
        assert "Insecure" in result.output

    def test_status_rejects_insecure_path(self):
        from seeknal.cli.heartbeat import heartbeat_app

        result = runner.invoke(
            heartbeat_app, ["status", "--project", "/tmp/evil"]
        )
        assert result.exit_code != 0
        assert "Insecure" in result.output
