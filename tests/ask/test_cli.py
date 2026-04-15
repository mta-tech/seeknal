"""Tests for seeknal ask CLI registration and basic structure."""

import pytest
from typer.testing import CliRunner

from seeknal.ask.project import find_project_path


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
    """Test project auto-detection via shared find_project_path."""

    def test_finds_seeknal_yml(self, tmp_path, monkeypatch):
        (tmp_path / "seeknal.yml").touch()
        monkeypatch.chdir(tmp_path)
        assert find_project_path() == tmp_path

    def test_finds_seeknal_dir(self, tmp_path, monkeypatch):
        (tmp_path / "seeknal").mkdir()
        monkeypatch.chdir(tmp_path)
        assert find_project_path() == tmp_path

    def test_finds_target_dir(self, tmp_path, monkeypatch):
        (tmp_path / "target").mkdir()
        monkeypatch.chdir(tmp_path)
        assert find_project_path() == tmp_path

    def test_defaults_to_cwd(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        assert find_project_path() == tmp_path

    def test_explicit_path(self, tmp_path):
        assert find_project_path(str(tmp_path)) == tmp_path

    def test_explicit_path_nonexistent(self):
        with pytest.raises(FileNotFoundError, match="not found"):
            find_project_path("/nonexistent/xyz")


class TestLoadProjectEnv:
    """Regression tests for project .env auto-loading.

    The post-refactor live verification caught that `seeknal ask chat --project
    <elsewhere>` failed to publish to the Seeknal Report Server because the
    project's .env was never loaded — so `${SEEKNAL_PUBLISH_TOKEN}` in
    profiles.yml resolved to empty and `ProfileLoader.load_publish_profile()`
    raised. The fix loads `<project>/.env` at CLI bootstrap with
    `override=False` so shell env vars still win.
    """

    def test_loads_project_env_file(self, tmp_path, monkeypatch):
        from seeknal.cli.ask import _load_project_env

        (tmp_path / ".env").write_text(
            "SEEKNAL_TEST_FROM_PROJECT_ENV=from_project\n"
        )
        monkeypatch.delenv("SEEKNAL_TEST_FROM_PROJECT_ENV", raising=False)

        _load_project_env(tmp_path)

        import os
        assert os.environ.get("SEEKNAL_TEST_FROM_PROJECT_ENV") == "from_project"

    def test_shell_env_overrides_project_env(self, tmp_path, monkeypatch):
        """override=False: existing shell env wins over .env."""
        from seeknal.cli.ask import _load_project_env

        (tmp_path / ".env").write_text(
            "SEEKNAL_TEST_OVERRIDE_CHECK=from_project\n"
        )
        monkeypatch.setenv("SEEKNAL_TEST_OVERRIDE_CHECK", "from_shell")

        _load_project_env(tmp_path)

        import os
        assert os.environ.get("SEEKNAL_TEST_OVERRIDE_CHECK") == "from_shell"

    def test_missing_env_file_is_silent(self, tmp_path):
        """No .env in project dir is NOT an error."""
        from seeknal.cli.ask import _load_project_env

        # No raise, no side effects.
        _load_project_env(tmp_path)

    def test_publish_token_pattern(self, tmp_path, monkeypatch):
        """The specific bug pattern from live verification: a project's .env
        has SEEKNAL_PUBLISH_TOKEN which profiles.yml references via
        `${SEEKNAL_PUBLISH_TOKEN}`. After `_load_project_env`, the token
        should be available to downstream loaders."""
        from seeknal.cli.ask import _load_project_env

        (tmp_path / ".env").write_text(
            "SEEKNAL_PUBLISH_TOKEN=sek_test_abc123\n"
            "GOOGLE_API_KEY=test_google_key\n"
            "SEEKNAL_ASK_LLM_PROVIDER=google\n"
            "SEEKNAL_ASK_MODEL=gemini-2.5-flash\n"
        )
        for k in (
            "SEEKNAL_PUBLISH_TOKEN", "GOOGLE_API_KEY",
            "SEEKNAL_ASK_LLM_PROVIDER", "SEEKNAL_ASK_MODEL",
        ):
            monkeypatch.delenv(k, raising=False)

        _load_project_env(tmp_path)

        import os
        assert os.environ.get("SEEKNAL_PUBLISH_TOKEN") == "sek_test_abc123"
        assert os.environ.get("GOOGLE_API_KEY") == "test_google_key"
        assert os.environ.get("SEEKNAL_ASK_LLM_PROVIDER") == "google"
        assert os.environ.get("SEEKNAL_ASK_MODEL") == "gemini-2.5-flash"
