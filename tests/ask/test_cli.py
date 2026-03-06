"""Tests for seeknal ask CLI registration and basic structure."""

import pytest
from typer.testing import CliRunner
from unittest.mock import patch, MagicMock


runner = CliRunner()


class TestAskCLIRegistration:
    """Test that ask commands register properly in the main CLI."""

    def test_ask_command_exists(self):
        """The ask command should be registered (either real or placeholder)."""
        from seeknal.cli.main import app

        result = runner.invoke(app, ["ask", "--help"])
        # Should not crash — either shows help or "not installed"
        assert result.exit_code in (0, 1)

    def test_ask_app_has_chat_command(self):
        """The ask app should have a chat subcommand."""
        from seeknal.cli.ask import ask_app

        result = runner.invoke(ask_app, ["chat", "--help"])
        assert result.exit_code == 0
        assert "interactive" in result.output.lower() or "chat" in result.output.lower()


class TestFindProjectPath:
    """Test project auto-detection."""

    def test_finds_seeknal_yml(self, tmp_path, monkeypatch):
        (tmp_path / "seeknal.yml").touch()
        monkeypatch.chdir(tmp_path)

        from seeknal.cli.ask import _find_project_path

        assert _find_project_path() == tmp_path

    def test_finds_target_dir(self, tmp_path, monkeypatch):
        (tmp_path / "target").mkdir()
        monkeypatch.chdir(tmp_path)

        from seeknal.cli.ask import _find_project_path

        assert _find_project_path() == tmp_path

    def test_defaults_to_cwd(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)

        from seeknal.cli.ask import _find_project_path

        assert _find_project_path() == tmp_path
