"""Tests for seeknal gateway CLI commands."""

from pathlib import Path
from unittest.mock import AsyncMock, patch

from typer.testing import CliRunner

runner = CliRunner()


class TestGatewayApp:
    def test_app_created(self):
        from seeknal.cli.gateway import gateway_app

        assert gateway_app is not None
        assert gateway_app.info.name == "gateway"
        assert gateway_app.info.help == "Gateway server for seeknal ask agent."


class TestGatewayStart:
    def test_help_shows_options(self):
        from seeknal.cli.gateway import gateway_app

        result = runner.invoke(gateway_app, ["start", "--help"])
        assert result.exit_code == 0
        assert "--host" in result.output
        assert "--port" in result.output
        assert "--project" in result.output

    def test_top_level_help_shows_commands(self):
        from seeknal.cli.gateway import gateway_app

        result = runner.invoke(gateway_app, ["--help"])
        assert result.exit_code == 0
        assert "start" in result.output
        assert "temporal-worker" in result.output

    @patch("seeknal.ask.gateway.server.run_gateway", new_callable=AsyncMock)
    def test_start_defaults(self, mock_run, tmp_path):
        """Start with no flags uses config defaults and calls run_gateway."""
        from seeknal.cli.gateway import gateway_app

        with patch("seeknal.ask.config.load_agent_config", return_value={}):
            result = runner.invoke(
                gateway_app, ["start", "--project", str(tmp_path)]
            )

        assert result.exit_code == 0
        mock_run.assert_called_once()
        call_kwargs = mock_run.call_args[1]
        assert call_kwargs["host"] == "127.0.0.1"
        assert call_kwargs["port"] == 18789
        assert call_kwargs["project_path"] == Path(tmp_path)

    @patch("seeknal.ask.gateway.server.run_gateway", new_callable=AsyncMock)
    def test_start_cli_overrides(self, mock_run, tmp_path):
        """CLI flags override config file values."""
        from seeknal.cli.gateway import gateway_app

        with patch("seeknal.ask.config.load_agent_config", return_value={}):
            result = runner.invoke(
                gateway_app,
                [
                    "start",
                    "--host", "0.0.0.0",
                    "--port", "9999",
                    "--project", str(tmp_path),
                ],
            )

        assert result.exit_code == 0
        mock_run.assert_called_once()
        call_kwargs = mock_run.call_args[1]
        assert call_kwargs["host"] == "0.0.0.0"
        assert call_kwargs["port"] == 9999

    @patch("seeknal.ask.gateway.server.run_gateway", new_callable=AsyncMock)
    def test_start_uses_config_values(self, mock_run, tmp_path):
        """When no CLI flags, uses values from seeknal_agent.yml gateway section."""
        from seeknal.cli.gateway import gateway_app

        config = {"gateway": {"host": "10.0.0.1", "port": 8080}}
        with patch("seeknal.ask.config.load_agent_config", return_value=config):
            result = runner.invoke(
                gateway_app, ["start", "--project", str(tmp_path)]
            )

        assert result.exit_code == 0
        mock_run.assert_called_once()
        call_kwargs = mock_run.call_args[1]
        assert call_kwargs["host"] == "10.0.0.1"
        assert call_kwargs["port"] == 8080

    @patch("seeknal.ask.gateway.server.run_gateway", new_callable=AsyncMock)
    def test_start_shows_project_path(self, mock_run, tmp_path):
        """Output includes the project path."""
        from seeknal.cli.gateway import gateway_app

        with patch("seeknal.ask.config.load_agent_config", return_value={}):
            result = runner.invoke(
                gateway_app, ["start", "--project", str(tmp_path)]
            )

        assert result.exit_code == 0
        assert str(tmp_path) in result.output

    @patch("seeknal.ask.gateway.server.run_gateway", new_callable=AsyncMock)
    def test_start_passes_config_to_run_gateway(self, mock_run, tmp_path):
        """Agent config dict is passed through to run_gateway."""
        from seeknal.cli.gateway import gateway_app

        config = {"model": "gemini-2.5-pro", "gateway": {"host": "0.0.0.0"}}
        with patch("seeknal.ask.config.load_agent_config", return_value=config):
            result = runner.invoke(
                gateway_app, ["start", "--project", str(tmp_path)]
            )

        assert result.exit_code == 0
        call_kwargs = mock_run.call_args[1]
        assert call_kwargs["config"] == config


class TestTemporalWorkerCLI:
    def test_help_shows_options(self):
        from seeknal.cli.gateway import gateway_app

        result = runner.invoke(gateway_app, ["temporal-worker", "--help"])
        assert result.exit_code == 0
        assert "--server" in result.output
        assert "--namespace" in result.output
        assert "--task-queue" in result.output
        assert "--project" in result.output

    @patch("seeknal.ask.gateway.temporal.worker.run_temporal_worker", new_callable=AsyncMock)
    def test_temporal_worker_defaults(self, mock_run, tmp_path):
        """Temporal worker with no flags uses defaults."""
        from seeknal.cli.gateway import gateway_app

        with patch("seeknal.ask.config.load_agent_config", return_value={}):
            result = runner.invoke(
                gateway_app,
                ["temporal-worker", "--project", str(tmp_path)],
            )

        assert result.exit_code == 0
        mock_run.assert_called_once()
        call_kwargs = mock_run.call_args[1]
        assert call_kwargs["server"] == "localhost:7233"
        assert call_kwargs["namespace"] == "seeknal"
        assert call_kwargs["task_queue"] == "seeknal-ask"
        assert call_kwargs["project_path"] == Path(tmp_path)

    @patch("seeknal.ask.gateway.temporal.worker.run_temporal_worker", new_callable=AsyncMock)
    def test_temporal_worker_cli_overrides(self, mock_run, tmp_path):
        """CLI flags override config and defaults."""
        from seeknal.cli.gateway import gateway_app

        with patch("seeknal.ask.config.load_agent_config", return_value={}):
            result = runner.invoke(
                gateway_app,
                [
                    "temporal-worker",
                    "--server", "temporal.example.com:7233",
                    "--namespace", "production",
                    "--task-queue", "my-queue",
                    "--project", str(tmp_path),
                ],
            )

        assert result.exit_code == 0
        mock_run.assert_called_once()
        call_kwargs = mock_run.call_args[1]
        assert call_kwargs["server"] == "temporal.example.com:7233"
        assert call_kwargs["namespace"] == "production"
        assert call_kwargs["task_queue"] == "my-queue"

    @patch("seeknal.ask.gateway.temporal.worker.run_temporal_worker", new_callable=AsyncMock)
    def test_temporal_worker_uses_config_values(self, mock_run, tmp_path):
        """When no CLI flags, uses values from seeknal_agent.yml temporal section."""
        from seeknal.cli.gateway import gateway_app

        config = {
            "gateway": {
                "temporal": {
                    "server": "my-temporal:7233",
                    "namespace": "my-ns",
                    "task_queue": "my-q",
                }
            }
        }
        with patch("seeknal.ask.config.load_agent_config", return_value=config):
            result = runner.invoke(
                gateway_app,
                ["temporal-worker", "--project", str(tmp_path)],
            )

        assert result.exit_code == 0
        mock_run.assert_called_once()
        call_kwargs = mock_run.call_args[1]
        assert call_kwargs["server"] == "my-temporal:7233"
        assert call_kwargs["namespace"] == "my-ns"
        assert call_kwargs["task_queue"] == "my-q"

    def test_registered_in_main_app(self):
        """Gateway sub-app is registered on the main CLI app."""
        from seeknal.cli.main import app

        group_names = [g.name for g in app.registered_groups]
        assert "gateway" in group_names


# ---------------------------------------------------------------------------
# Path validation tests
# ---------------------------------------------------------------------------


class TestPathValidation:
    def test_start_rejects_insecure_path(self):
        from seeknal.cli.gateway import gateway_app

        result = runner.invoke(
            gateway_app, ["start", "--project", "/tmp/evil"]
        )
        assert result.exit_code != 0
        assert "Insecure" in result.output

    def test_temporal_worker_rejects_insecure_path(self):
        from seeknal.cli.gateway import gateway_app

        result = runner.invoke(
            gateway_app, ["temporal-worker", "--project", "/tmp/evil"]
        )
        assert result.exit_code != 0
        assert "Insecure" in result.output

    def test_setup_rejects_insecure_path(self):
        from seeknal.cli.gateway import gateway_app

        result = runner.invoke(
            gateway_app, ["setup", "--project", "/tmp/evil"]
        )
        assert result.exit_code != 0
        assert "Insecure" in result.output


# ---------------------------------------------------------------------------
# Setup wizard tests
# ---------------------------------------------------------------------------


class TestGatewaySetup:
    def test_help_shows_options(self):
        from seeknal.cli.gateway import gateway_app

        result = runner.invoke(gateway_app, ["setup", "--help"])
        assert result.exit_code == 0
        assert "--project" in result.output

    def test_all_sections_skipped_writes_nothing(self, tmp_path):
        """Skipping all sections produces no config file."""
        from seeknal.cli.gateway import gateway_app

        result = runner.invoke(
            gateway_app,
            ["setup", "--project", str(tmp_path)],
            input="n\nn\nn\nn\n",
        )
        assert result.exit_code == 0
        assert "No sections configured" in result.output
        assert not (tmp_path / "seeknal_agent.yml").exists()

    def test_agent_section_only(self, tmp_path):
        """Configuring only agent model produces valid YAML."""
        import yaml

        from seeknal.cli.gateway import gateway_app

        # y=configure agent, defaults for model/temp/profile, n=telegram, n=temporal, n=heartbeat
        result = runner.invoke(
            gateway_app,
            ["setup", "--project", str(tmp_path)],
            input="y\ngemini-2.5-flash\n0.0\nanalysis\nn\nn\nn\n",
        )
        assert result.exit_code == 0
        assert "Setup Complete" in result.output

        config_path = tmp_path / "seeknal_agent.yml"
        assert config_path.exists()

        config = yaml.safe_load(config_path.read_text())
        assert config["model"] == "gemini-2.5-flash"
        assert config["temperature"] == 0.0
        assert config["default_profile"] == "analysis"

    def test_telegram_with_valid_token(self, tmp_path):
        """Telegram section with valid token stores env var reference."""
        import yaml

        from seeknal.cli.gateway import gateway_app

        with patch("seeknal.cli.gateway._validate_telegram_token", return_value="test_bot"):
            # n=agent, y=telegram, token, n=pair, y=polling, n=temporal, n=heartbeat
            result = runner.invoke(
                gateway_app,
                ["setup", "--project", str(tmp_path)],
                input="n\ny\nfake-token-123\nn\ny\nn\nn\n",
            )

        assert result.exit_code == 0
        assert "@test_bot" in result.output
        assert "Setup Complete" in result.output

        config = yaml.safe_load((tmp_path / "seeknal_agent.yml").read_text())
        assert config["gateway"]["telegram"]["token"] == "${TELEGRAM_BOT_TOKEN}"
        assert config["gateway"]["telegram"]["polling"] is True

    def test_telegram_with_invalid_token_skips(self, tmp_path):
        """Invalid token with skip produces no telegram section."""
        from seeknal.cli.gateway import gateway_app

        with patch("seeknal.cli.gateway._validate_telegram_token", return_value=None):
            # n=agent, y=telegram, bad-token, n=try again, n=skip validation,
            # n=temporal, n=heartbeat
            result = runner.invoke(
                gateway_app,
                ["setup", "--project", str(tmp_path)],
                input="n\ny\nbad-token\nn\nn\nn\nn\n",
            )

        assert result.exit_code == 0
        assert not (tmp_path / "seeknal_agent.yml").exists()

    def test_all_sections_configured(self, tmp_path):
        """Full setup with all sections produces complete YAML."""
        import yaml

        from seeknal.cli.gateway import gateway_app

        with patch("seeknal.cli.gateway._validate_telegram_token", return_value="my_bot"), \
             patch("seeknal.cli.gateway._wait_for_pairing", return_value={
                 "chat_id": "-100123", "chat_title": "Test Group",
                 "user_id": "42", "first_name": "User",
             }):
            result = runner.invoke(
                gateway_app,
                ["setup", "--project", str(tmp_path)],
                input=(
                    "y\ngemini-2.5-flash\n0.0\nanalysis\n"  # agent
                    "y\nmy-token\ny\ny\n"  # telegram (token, pair=y, polling=y)
                    "y\nlocalhost:7233\nseeknalns\nseeknalq\n"  # temporal
                    "y\n1h\ntelegram\n-100123\nn\n"  # heartbeat (chat_id pre-filled)
                ),
            )

        assert result.exit_code == 0
        assert "Setup Complete" in result.output
        assert "Agent model" in result.output
        assert "Telegram channel" in result.output
        assert "Temporal worker" in result.output
        assert "Heartbeat monitoring" in result.output

        config = yaml.safe_load((tmp_path / "seeknal_agent.yml").read_text())
        assert config["model"] == "gemini-2.5-flash"
        assert config["gateway"]["telegram"]["token"] == "${TELEGRAM_BOT_TOKEN}"
        assert config["gateway"]["temporal"]["server"] == "localhost:7233"
        assert config["heartbeat"]["every"] == "1h"

    def test_merge_preserves_existing_config(self, tmp_path):
        """Merge mode preserves untouched sections from existing config."""
        import yaml

        from seeknal.cli.gateway import gateway_app

        # Write existing config
        existing = {"model": "old-model", "custom_key": "preserved"}
        (tmp_path / "seeknal_agent.yml").write_text(
            yaml.safe_dump(existing, default_flow_style=False)
        )

        with patch("seeknal.cli.gateway._validate_telegram_token", return_value="bot"):
            # y=agent (overwrite model), n=telegram, n=temporal, n=heartbeat, y=merge
            result = runner.invoke(
                gateway_app,
                ["setup", "--project", str(tmp_path)],
                input="y\nnew-model\n0.5\nbuild\nn\nn\nn\ny\n",
            )

        assert result.exit_code == 0
        config = yaml.safe_load((tmp_path / "seeknal_agent.yml").read_text())
        assert config["model"] == "new-model"  # Updated
        assert config["custom_key"] == "preserved"  # Preserved from existing

    def test_overwrite_replaces_existing_config(self, tmp_path):
        """Overwrite mode replaces entire existing config."""
        import yaml

        from seeknal.cli.gateway import gateway_app

        existing = {"model": "old-model", "custom_key": "will-be-lost"}
        (tmp_path / "seeknal_agent.yml").write_text(
            yaml.safe_dump(existing, default_flow_style=False)
        )

        # y=agent, n=telegram, n=temporal, n=heartbeat, n=merge (overwrite)
        result = runner.invoke(
            gateway_app,
            ["setup", "--project", str(tmp_path)],
            input="y\nnew-model\n0.5\nbuild\nn\nn\nn\nn\n",
        )

        assert result.exit_code == 0
        config = yaml.safe_load((tmp_path / "seeknal_agent.yml").read_text())
        assert config["model"] == "new-model"
        assert "custom_key" not in config  # Overwritten

    def test_roundtrip_with_load_agent_config(self, tmp_path):
        """Generated YAML is loadable by load_agent_config."""
        from seeknal.ask.config import load_agent_config
        from seeknal.cli.gateway import gateway_app

        with patch("seeknal.cli.gateway._validate_telegram_token", return_value="bot"):
            runner.invoke(
                gateway_app,
                ["setup", "--project", str(tmp_path)],
                input="y\ngemini-2.5-flash\n0.0\nanalysis\ny\ntoken\nn\ny\nn\nn\n",
            )

        config = load_agent_config(tmp_path)
        assert config["model"] == "gemini-2.5-flash"
        assert config["gateway"]["telegram"]["token"] == "${TELEGRAM_BOT_TOKEN}"

    def test_pairing_captures_chat_id_for_heartbeat(self, tmp_path):
        """Pairing chat_id is auto-filled as heartbeat default."""
        import yaml

        from seeknal.cli.gateway import gateway_app

        with patch("seeknal.cli.gateway._validate_telegram_token", return_value="bot"), \
             patch("seeknal.cli.gateway._wait_for_pairing", return_value={
                 "chat_id": "-999", "chat_title": "My Group",
                 "user_id": "7", "first_name": "Dev",
             }):
            result = runner.invoke(
                gateway_app,
                ["setup", "--project", str(tmp_path)],
                input=(
                    "n\n"  # skip agent
                    "y\nmy-token\ny\ny\n"  # telegram + pair + polling
                    "n\n"  # skip temporal
                    "y\n30m\ntelegram\n-999\nn\n"  # heartbeat (chat_id default from pairing)
                ),
            )

        assert result.exit_code == 0
        config = yaml.safe_load((tmp_path / "seeknal_agent.yml").read_text())
        assert config["heartbeat"]["target"]["chat_id"] == "-999"

    def test_pairing_timeout_continues(self, tmp_path):
        """Pairing timeout doesn't block the wizard."""
        from seeknal.cli.gateway import gateway_app

        with patch("seeknal.cli.gateway._validate_telegram_token", return_value="bot"), \
             patch("seeknal.cli.gateway._wait_for_pairing", return_value=None):
            result = runner.invoke(
                gateway_app,
                ["setup", "--project", str(tmp_path)],
                input="n\ny\nmy-token\ny\ny\nn\nn\n",
            )

        assert result.exit_code == 0
        assert "timed out" in result.output.lower()
        assert "Setup Complete" in result.output

    def test_summary_includes_start_command(self, tmp_path):
        """Summary output includes the gateway start command."""
        from seeknal.cli.gateway import gateway_app

        result = runner.invoke(
            gateway_app,
            ["setup", "--project", str(tmp_path)],
            input="y\ngemini-2.5-flash\n0.0\nanalysis\nn\nn\nn\n",
        )

        assert "seeknal gateway start" in result.output
        assert str(tmp_path) in result.output


class TestValidateTelegramToken:
    def test_valid_token_returns_username(self):
        from seeknal.cli.gateway import _validate_telegram_token

        mock_resp = b'{"ok": true, "result": {"username": "my_bot"}}'

        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_urlopen.return_value.__enter__ = lambda s: s
            mock_urlopen.return_value.__exit__ = lambda s, *a: None
            mock_urlopen.return_value.read.return_value = mock_resp
            result = _validate_telegram_token("valid-token")

        assert result == "my_bot"

    def test_invalid_token_returns_none(self):
        import urllib.error

        from seeknal.cli.gateway import _validate_telegram_token

        with patch("urllib.request.urlopen", side_effect=urllib.error.HTTPError(
            url="", code=401, msg="Unauthorized", hdrs=None, fp=None
        )):
            result = _validate_telegram_token("bad-token")

        assert result is None

    def test_network_error_returns_none(self):
        import urllib.error

        from seeknal.cli.gateway import _validate_telegram_token

        with patch("urllib.request.urlopen", side_effect=urllib.error.URLError("unreachable")):
            result = _validate_telegram_token("any-token")

        assert result is None
