"""Tests for seeknal gateway CLI commands."""

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
        assert "--project-path" in result.output

    def test_top_level_help_shows_commands(self):
        from seeknal.cli.gateway import gateway_app

        result = runner.invoke(gateway_app, ["--help"])
        assert result.exit_code == 0
        assert "start" in result.output
        assert "temporal-worker" in result.output

    @patch("seeknal.ask.gateway.server.run_gateway", new_callable=AsyncMock)
    @patch("seeknal.cli.gateway.asyncio")
    def test_start_defaults(self, mock_asyncio, mock_run, tmp_path):
        """Start with no flags uses config defaults."""
        from seeknal.cli.gateway import gateway_app

        mock_asyncio.run = lambda coro: None

        with patch("seeknal.ask.config.load_agent_config", return_value={}):
            result = runner.invoke(
                gateway_app, ["start", "--project-path", str(tmp_path)]
            )

        assert result.exit_code == 0
        assert "127.0.0.1:18789" in result.output

    @patch("seeknal.ask.gateway.server.run_gateway", new_callable=AsyncMock)
    @patch("seeknal.cli.gateway.asyncio")
    def test_start_cli_overrides(self, mock_asyncio, mock_run, tmp_path):
        """CLI flags override config file values."""
        from seeknal.cli.gateway import gateway_app

        mock_asyncio.run = lambda coro: None

        with patch("seeknal.ask.config.load_agent_config", return_value={}):
            result = runner.invoke(
                gateway_app,
                [
                    "start",
                    "--host", "0.0.0.0",
                    "--port", "9999",
                    "--project-path", str(tmp_path),
                ],
            )

        assert result.exit_code == 0
        assert "0.0.0.0:9999" in result.output

    @patch("seeknal.ask.gateway.server.run_gateway", new_callable=AsyncMock)
    @patch("seeknal.cli.gateway.asyncio")
    def test_start_uses_config_values(self, mock_asyncio, mock_run, tmp_path):
        """When no CLI flags, uses values from seeknal_agent.yml gateway section."""
        from seeknal.cli.gateway import gateway_app

        mock_asyncio.run = lambda coro: None

        config = {"gateway": {"host": "10.0.0.1", "port": 8080}}
        with patch("seeknal.ask.config.load_agent_config", return_value=config):
            result = runner.invoke(
                gateway_app, ["start", "--project-path", str(tmp_path)]
            )

        assert result.exit_code == 0
        assert "10.0.0.1:8080" in result.output

    @patch("seeknal.ask.gateway.server.run_gateway", new_callable=AsyncMock)
    @patch("seeknal.cli.gateway.asyncio")
    def test_start_shows_project_path(self, mock_asyncio, mock_run, tmp_path):
        """Output includes the project path."""
        from seeknal.cli.gateway import gateway_app

        mock_asyncio.run = lambda coro: None

        with patch("seeknal.ask.config.load_agent_config", return_value={}):
            result = runner.invoke(
                gateway_app, ["start", "--project-path", str(tmp_path)]
            )

        assert result.exit_code == 0
        assert f"Project: {tmp_path}" in result.output


class TestTemporalWorkerCLI:
    def test_help_shows_options(self):
        from seeknal.cli.gateway import gateway_app

        result = runner.invoke(gateway_app, ["temporal-worker", "--help"])
        assert result.exit_code == 0
        assert "--server" in result.output
        assert "--namespace" in result.output
        assert "--task-queue" in result.output
        assert "--project-path" in result.output

    @patch("seeknal.ask.gateway.temporal.worker.run_temporal_worker", new_callable=AsyncMock)
    @patch("seeknal.cli.gateway.asyncio")
    def test_temporal_worker_defaults(self, mock_asyncio, mock_run, tmp_path):
        """Temporal worker with no flags uses defaults."""
        from seeknal.cli.gateway import gateway_app

        mock_asyncio.run = lambda coro: None

        with patch("seeknal.ask.config.load_agent_config", return_value={}):
            result = runner.invoke(
                gateway_app,
                ["temporal-worker", "--project-path", str(tmp_path)],
            )

        assert result.exit_code == 0
        assert "localhost:7233" in result.output
        assert "seeknal" in result.output
        assert "seeknal-ask" in result.output

    @patch("seeknal.ask.gateway.temporal.worker.run_temporal_worker", new_callable=AsyncMock)
    @patch("seeknal.cli.gateway.asyncio")
    def test_temporal_worker_cli_overrides(self, mock_asyncio, mock_run, tmp_path):
        """CLI flags override config and defaults."""
        from seeknal.cli.gateway import gateway_app

        mock_asyncio.run = lambda coro: None

        with patch("seeknal.ask.config.load_agent_config", return_value={}):
            result = runner.invoke(
                gateway_app,
                [
                    "temporal-worker",
                    "--server", "temporal.example.com:7233",
                    "--namespace", "production",
                    "--task-queue", "my-queue",
                    "--project-path", str(tmp_path),
                ],
            )

        assert result.exit_code == 0
        assert "temporal.example.com:7233" in result.output
        assert "production" in result.output
        assert "my-queue" in result.output

    @patch("seeknal.ask.gateway.temporal.worker.run_temporal_worker", new_callable=AsyncMock)
    @patch("seeknal.cli.gateway.asyncio")
    def test_temporal_worker_uses_config_values(self, mock_asyncio, mock_run, tmp_path):
        """When no CLI flags, uses values from seeknal_agent.yml temporal section."""
        from seeknal.cli.gateway import gateway_app

        mock_asyncio.run = lambda coro: None

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
                ["temporal-worker", "--project-path", str(tmp_path)],
            )

        assert result.exit_code == 0
        assert "my-temporal:7233" in result.output
        assert "my-ns" in result.output
        assert "my-q" in result.output

    def test_registered_in_main_app(self):
        """Gateway sub-app is registered on the main CLI app."""
        from seeknal.cli.main import app

        # Collect all registered group names
        group_names = [g.name for g in app.registered_groups]
        assert "gateway" in group_names
