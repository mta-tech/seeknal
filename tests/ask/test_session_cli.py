"""Tests for seeknal session CLI commands."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

runner = CliRunner()


@pytest.fixture
def mock_store(tmp_path):
    """Create a real SessionStore for testing."""
    from seeknal.ask.sessions import SessionStore

    store = SessionStore(tmp_path)
    yield store, tmp_path
    store.close()


class TestSessionList:
    def test_empty_shows_help(self, mock_store):
        store, tmp_path = mock_store
        from seeknal.cli.session import session_app

        result = runner.invoke(session_app, ["list", "--project", str(tmp_path)])
        assert result.exit_code == 0
        assert "No sessions found" in result.output

    def test_with_sessions(self, mock_store):
        store, tmp_path = mock_store
        store.create("test-session-1")
        store.create("test-session-2")
        store.update("test-session-1", message_count=5, last_question="How many?")

        from seeknal.cli.session import session_app

        result = runner.invoke(session_app, ["list", "--project", str(tmp_path)])
        assert result.exit_code == 0
        assert "test-session-1" in result.output
        assert "test-session-2" in result.output
        assert "How many?" in result.output


class TestSessionShow:
    def test_shows_details(self, mock_store):
        store, tmp_path = mock_store
        store.create("my-analysis")
        store.update("my-analysis", message_count=3, last_question="Build pipeline")

        from seeknal.cli.session import session_app

        result = runner.invoke(session_app, ["show", "my-analysis", "--project", str(tmp_path)])
        assert result.exit_code == 0
        assert "my-analysis" in result.output
        assert "active" in result.output
        assert "3" in result.output
        assert "Build pipeline" in result.output

    def test_not_found(self, mock_store):
        _, tmp_path = mock_store
        from seeknal.cli.session import session_app

        result = runner.invoke(session_app, ["show", "nonexistent", "--project", str(tmp_path)])
        assert result.exit_code == 1
        assert "not found" in result.output


class TestSessionDelete:
    def test_delete_with_force(self, mock_store):
        store, tmp_path = mock_store
        store.create("to-delete")

        from seeknal.cli.session import session_app

        result = runner.invoke(session_app, ["delete", "to-delete", "--force", "--project", str(tmp_path)])
        assert result.exit_code == 0
        assert "deleted" in result.output
        assert store.get("to-delete") is None

    def test_delete_not_found(self, mock_store):
        _, tmp_path = mock_store
        from seeknal.cli.session import session_app

        result = runner.invoke(session_app, ["delete", "nonexistent", "--force", "--project", str(tmp_path)])
        assert result.exit_code == 1
        assert "not found" in result.output

    def test_delete_cancelled(self, mock_store):
        store, tmp_path = mock_store
        store.create("keep-me")

        from seeknal.cli.session import session_app

        result = runner.invoke(session_app, ["delete", "keep-me", "--project", str(tmp_path)], input="n\n")
        assert result.exit_code == 0
        assert "Cancelled" in result.output
        assert store.get("keep-me") is not None
