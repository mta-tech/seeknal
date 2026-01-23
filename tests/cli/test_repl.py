"""Tests for seeknal.cli.repl module."""
import pytest
from unittest.mock import MagicMock, patch
from seeknal.cli.repl import REPL


@pytest.fixture
def repl():
    """Create REPL instance with mocked DuckDB."""
    with patch("seeknal.cli.repl.duckdb") as mock_duckdb:
        mock_conn = MagicMock()
        mock_duckdb.connect.return_value = mock_conn
        r = REPL()
        yield r, mock_conn


def test_connect_postgres(repl):
    """PostgreSQL URLs are parsed and attached correctly."""
    r, mock_conn = repl
    r._connect("postgres://user:pass@localhost:5432/mydb")

    mock_conn.execute.assert_called()
    call_args = str(mock_conn.execute.call_args)
    assert "TYPE postgres" in call_args
    assert "db0" in r.attached


def test_connect_mysql(repl):
    """MySQL URLs are parsed and attached correctly."""
    r, mock_conn = repl
    r._connect("mysql://user:pass@localhost:3306/mydb")

    mock_conn.execute.assert_called()
    call_args = str(mock_conn.execute.call_args)
    assert "TYPE mysql" in call_args
    assert "db0" in r.attached


def test_connect_sqlite(repl):
    """SQLite URLs are attached correctly."""
    r, mock_conn = repl
    with patch("seeknal.cli.repl.is_insecure_path", return_value=False):
        r._connect("sqlite:///path/to/db.sqlite")

    mock_conn.execute.assert_called()
    call_args = str(mock_conn.execute.call_args)
    assert "TYPE sqlite" in call_args


def test_connect_expands_env_vars(repl, monkeypatch):
    """Environment variables in URLs are expanded."""
    monkeypatch.setenv("DATABASE_URL", "postgres://user:pass@host/db")
    r, mock_conn = repl
    r._connect("$DATABASE_URL")

    call_args = str(mock_conn.execute.call_args)
    assert "host=host" in call_args


def test_connect_insecure_path_rejected(repl):
    """Insecure paths are rejected."""
    r, _ = repl
    with patch("seeknal.cli.repl.is_insecure_path", return_value=True):
        with patch("builtins.print") as mock_print:
            r._connect("/tmp/evil.parquet")
    # Should print error message about connection failing
    output = " ".join(str(c) for c in mock_print.call_args_list)
    assert "Connection failed" in output or "Insecure path" in output


def test_execute_limits_output(repl):
    """Large result sets are limited to 100 rows."""
    r, mock_conn = repl
    mock_result = MagicMock()
    mock_result.description = [("col1",)]
    mock_result.fetchall.return_value = [(i,) for i in range(500)]
    mock_conn.execute.return_value = mock_result

    with patch("builtins.print") as mock_print:
        r._execute("SELECT * FROM big_table")

    # Should mention limited rows
    output = " ".join(str(c) for c in mock_print.call_args_list)
    assert "100 of 500" in output


def test_error_messages_sanitized(repl):
    """Error messages don't expose credentials."""
    r, mock_conn = repl
    mock_conn.execute.side_effect = Exception("password=secret123 failed")

    with patch(
        "seeknal.cli.repl.sanitize_error_message", return_value="connection failed"
    ) as mock_sanitize:
        with patch("builtins.print"):
            r._execute("SELECT 1")

    mock_sanitize.assert_called()


def test_show_tables_no_connections(repl):
    """Show tables warns when no databases connected."""
    r, _ = repl
    with patch("builtins.print") as mock_print:
        r._show_tables()

    output = " ".join(str(c) for c in mock_print.call_args_list)
    assert "No databases connected" in output


def test_show_schema_requires_table_name(repl):
    """Show schema requires table name."""
    r, _ = repl
    with patch("builtins.print") as mock_print:
        r._show_schema("")

    output = " ".join(str(c) for c in mock_print.call_args_list)
    assert "Usage:" in output


def test_show_help(repl):
    """Help command shows available commands."""
    r, _ = repl
    with patch("builtins.print") as mock_print:
        r._show_help()

    output = " ".join(str(c) for c in mock_print.call_args_list)
    assert ".connect" in output
    assert ".tables" in output
    assert ".quit" in output
