"""Tests for seeknal.ask.sessions — session management module."""

import re
from pathlib import Path

import pytest


class TestGenerateSessionName:
    """Test human-readable session name generation."""

    def test_format_matches_pattern(self):
        from seeknal.ask.sessions import generate_session_name

        name = generate_session_name()
        assert re.match(r"^[a-z]+-[a-z]+-\d{3}$", name), f"Bad format: {name}"

    def test_generates_different_names(self):
        from seeknal.ask.sessions import generate_session_name

        names = {generate_session_name() for _ in range(20)}
        assert len(names) > 1, "Should generate varied names"


class TestSessionStore:
    """Test SessionStore CRUD operations."""

    def test_creates_seeknal_directory(self, tmp_path):
        from seeknal.ask.sessions import SessionStore

        store = SessionStore(tmp_path)
        assert (tmp_path / ".seeknal").is_dir()
        assert (tmp_path / ".seeknal" / "sessions.db").exists()
        store.close()

    def test_create_auto_name(self, tmp_path):
        from seeknal.ask.sessions import SessionStore

        store = SessionStore(tmp_path)
        name = store.create()
        assert re.match(r"^[a-z]+-[a-z]+-\d{3}$", name)
        store.close()

    def test_create_custom_name(self, tmp_path):
        from seeknal.ask.sessions import SessionStore

        store = SessionStore(tmp_path)
        name = store.create("my-analysis")
        assert name == "my-analysis"
        store.close()

    def test_get_existing(self, tmp_path):
        from seeknal.ask.sessions import SessionStore

        store = SessionStore(tmp_path)
        name = store.create("test-session")
        session = store.get(name)
        assert session is not None
        assert session["name"] == "test-session"
        assert session["status"] == "active"
        assert session["message_count"] == 0
        store.close()

    def test_get_nonexistent(self, tmp_path):
        from seeknal.ask.sessions import SessionStore

        store = SessionStore(tmp_path)
        assert store.get("nonexistent") is None
        store.close()

    def test_list_ordered_by_updated_at(self, tmp_path):
        from seeknal.ask.sessions import SessionStore

        store = SessionStore(tmp_path)
        store.create("session-a")
        store.create("session-b")
        # Update session-a to make it more recent
        store.update("session-a", message_count=5)
        sessions = store.list()
        assert len(sessions) == 2
        assert sessions[0]["name"] == "session-a"  # Most recently updated
        store.close()

    def test_update_fields(self, tmp_path):
        from seeknal.ask.sessions import SessionStore

        store = SessionStore(tmp_path)
        store.create("test")
        store.update("test", message_count=3, last_question="How many customers?")
        session = store.get("test")
        assert session["message_count"] == 3
        assert session["last_question"] == "How many customers?"
        store.close()

    def test_delete_existing(self, tmp_path):
        from seeknal.ask.sessions import SessionStore

        store = SessionStore(tmp_path)
        store.create("to-delete")
        assert store.delete("to-delete") is True
        assert store.get("to-delete") is None
        store.close()

    def test_delete_nonexistent(self, tmp_path):
        from seeknal.ask.sessions import SessionStore

        store = SessionStore(tmp_path)
        assert store.delete("nonexistent") is False
        store.close()

    def test_get_checkpointer(self, tmp_path):
        from seeknal.ask.sessions import SessionStore

        store = SessionStore(tmp_path)
        checkpointer = store.get_checkpointer()
        # SqliteSaver should be returned
        assert checkpointer is not None
        assert hasattr(checkpointer, "put")
        assert hasattr(checkpointer, "get_tuple")
        store.close()

    def test_list_empty(self, tmp_path):
        from seeknal.ask.sessions import SessionStore

        store = SessionStore(tmp_path)
        assert store.list() == []
        store.close()

    def test_update_ignored_fields(self, tmp_path):
        """Fields not in allowed set are silently ignored."""
        from seeknal.ask.sessions import SessionStore

        store = SessionStore(tmp_path)
        store.create("test")
        store.update("test", bad_field="hacker")
        session = store.get("test")
        assert "bad_field" not in session
        store.close()

    def test_context_manager_closes_connection(self, tmp_path):
        """SessionStore used as context manager closes on exit."""
        from seeknal.ask.sessions import SessionStore

        with SessionStore(tmp_path) as store:
            store.create("ctx-test")
            session = store.get("ctx-test")
            assert session is not None

        # Connection should be closed — operations should fail
        import sqlite3

        with pytest.raises(sqlite3.ProgrammingError):
            store._conn.execute("SELECT 1")
