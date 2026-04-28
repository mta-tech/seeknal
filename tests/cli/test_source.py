"""Tests for seeknal.cli.source module."""
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock
from typer.testing import CliRunner
from cryptography.fernet import Fernet

from seeknal.cli.source import app, _parse_and_encrypt_url


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def valid_key():
    return Fernet.generate_key().decode()


def test_parse_and_encrypt_url_masks_password(valid_key, monkeypatch):
    """URL password is masked in masked_url."""
    monkeypatch.setenv("SEEKNAL_ENCRYPT_KEY", valid_key)

    result = _parse_and_encrypt_url("postgres://user:secret123@host/db")

    assert result["source_type"] == "postgres"
    assert "secret123" not in result["masked_url"]
    assert "****" in result["masked_url"]
    assert result["encrypted_credentials"] is not None
    assert result["host"] == "host"
    assert result["username"] == "user"


def test_parse_and_encrypt_url_detects_mysql(valid_key, monkeypatch):
    """MySQL URLs are correctly identified."""
    monkeypatch.setenv("SEEKNAL_ENCRYPT_KEY", valid_key)

    result = _parse_and_encrypt_url("mysql://user:pass@localhost:3306/testdb")

    assert result["source_type"] == "mysql"
    assert result["port"] == 3306
    assert result["database"] == "testdb"


def test_parse_and_encrypt_url_detects_sqlite(valid_key, monkeypatch):
    """SQLite URLs are correctly identified."""
    monkeypatch.setenv("SEEKNAL_ENCRYPT_KEY", valid_key)

    result = _parse_and_encrypt_url("sqlite:///path/to/db.sqlite")

    assert result["source_type"] == "sqlite"
    assert result["encrypted_credentials"] is None


def test_parse_and_encrypt_url_detects_parquet(valid_key, monkeypatch):
    """Parquet file paths are correctly identified."""
    monkeypatch.setenv("SEEKNAL_ENCRYPT_KEY", valid_key)

    result = _parse_and_encrypt_url("/path/to/data.parquet")

    assert result["source_type"] == "parquet"
    assert result["encrypted_credentials"] is None


def test_parse_and_encrypt_url_detects_csv(valid_key, monkeypatch):
    """CSV file paths are correctly identified."""
    monkeypatch.setenv("SEEKNAL_ENCRYPT_KEY", valid_key)

    result = _parse_and_encrypt_url("/path/to/data.csv")

    assert result["source_type"] == "csv"
    assert result["encrypted_credentials"] is None


def test_add_source_requires_encryption_key(runner, monkeypatch):
    """Adding source fails without SEEKNAL_ENCRYPT_KEY."""
    monkeypatch.delenv("SEEKNAL_ENCRYPT_KEY", raising=False)

    result = runner.invoke(app, ["add", "mydb", "--url", "postgres://u:p@h/d"])

    assert result.exit_code == 1
    assert "SEEKNAL_ENCRYPT_KEY not set" in result.stdout


def test_list_sources_empty(runner):
    """List shows message when no sources."""
    with patch("seeknal.cli.source.ReplSourceRequest") as mock:
        mock.select_all.return_value = []

        result = runner.invoke(app, ["list"])

    assert "No sources configured" in result.stdout


def test_list_sources_shows_table(runner):
    """List shows sources in tabular format."""
    mock_source = MagicMock()
    mock_source.name = "mydb"
    mock_source.source_type = "postgres"
    mock_source.masked_url = "postgres://user:****@host/db"

    with patch("seeknal.cli.source.ReplSourceRequest") as mock:
        mock.select_all.return_value = [mock_source]

        result = runner.invoke(app, ["list"])

    assert "mydb" in result.stdout
    assert "postgres" in result.stdout
    assert "****" in result.stdout


def test_remove_source_not_found(runner):
    """Remove fails if source doesn't exist."""
    with patch("seeknal.cli.source.ReplSourceRequest") as mock:
        mock.select_by_name.return_value = None

        result = runner.invoke(app, ["remove", "nonexistent"])

    assert result.exit_code == 1
    assert "not found" in result.stdout


def test_list_configured_sources_from_agent_config(runner):
    """List shows seeknal_agent.yml sources before legacy REPL sources."""
    with runner.isolated_filesystem():
        Path("seeknal_agent.yml").write_text(
            """
sources:
  warehouse:
    source_kind: connected
    source_type: database
    connector: postgresql
    namespace: wh
    access: read_only
    role: business_source_of_truth
""".strip()
        )

        result = runner.invoke(app, ["list"])

    assert result.exit_code == 0
    assert "warehouse" in result.stdout
    assert "wh" in result.stdout
    assert "read_only" in result.stdout


def test_status_shows_context_sync_state(runner):
    with runner.isolated_filesystem():
        Path("seeknal_agent.yml").write_text(
            """
sources:
  warehouse:
    source_kind: connected
    source_type: database
    connector: postgresql
    namespace: wh
    access: read_only
    role: business_source_of_truth
""".strip()
        )

        sync = runner.invoke(app, ["sync", "warehouse"])
        status = runner.invoke(app, ["status"])

    assert sync.exit_code == 0
    assert "metadata_only" in sync.stdout
    assert status.exit_code == 0
    assert "warehouse" in status.stdout
    assert "metadata_only" in status.stdout


def test_inspect_configured_source(runner):
    with runner.isolated_filesystem():
        Path("seeknal_agent.yml").write_text(
            """
sources:
  warehouse:
    source_kind: connected
    source_type: database
    connector: postgresql
    namespace: wh
    access: read_only
    role: business_source_of_truth
    description: Analytics warehouse
""".strip()
        )

        result = runner.invoke(app, ["inspect", "wh"])

    assert result.exit_code == 0
    assert "Name: warehouse" in result.stdout
    assert "Namespace: wh" in result.stdout
    assert "Analytics warehouse" in result.stdout


def test_connect_writes_source_registry_entry(runner):
    with runner.isolated_filesystem():
        result = runner.invoke(
            app,
            [
                "connect",
                "warehouse",
                "--connector",
                "postgresql",
                "--namespace",
                "wh",
                "--dsn-env",
                "WAREHOUSE_DSN",
                "--description",
                "Analytics warehouse",
            ],
        )

        assert result.exit_code == 0
        text = Path("seeknal_agent.yml").read_text()
        assert "warehouse:" in text
        assert "namespace: wh" in text
        assert "dsn_env: WAREHOUSE_DSN" in text
        assert "access: read_only" in text
        assert "default: auto" in text


def test_connect_refuses_duplicate_without_force(runner):
    with runner.isolated_filesystem():
        Path("seeknal_agent.yml").write_text(
            """
sources:
  warehouse:
    source_kind: connected
    source_type: database
    connector: postgresql
    namespace: wh
    access: read_only
    role: business_source_of_truth
""".strip()
        )

        result = runner.invoke(app, ["connect", "warehouse", "--connector", "postgresql"])

        assert result.exit_code == 1
        assert "already exists" in result.stdout or "already exists" in result.stderr


def test_source_test_uses_read_only_repl_discovery(runner):
    class FakeRepl:
        attached = {"wh"}

        def __init__(self, project_path=None, skip_history=False):
            self.project_path = project_path
            self.skip_history = skip_history

        def execute_oneshot(self, sql, limit=None):
            assert '"wh".information_schema.tables' in sql
            return ["table_count"], [(7,)]

    with runner.isolated_filesystem():
        Path("seeknal_agent.yml").write_text(
            """
sources:
  warehouse:
    source_kind: connected
    source_type: database
    connector: postgresql
    namespace: wh
    access: read_only
    role: business_source_of_truth
""".strip()
        )
        with patch("seeknal.cli.repl.REPL", FakeRepl):
            result = runner.invoke(app, ["test", "wh"])

    assert result.exit_code == 0
    assert "7 table(s) visible" in result.stdout


def test_source_test_fails_when_namespace_not_attached(runner):
    class FakeRepl:
        attached = set()

        def __init__(self, project_path=None, skip_history=False):
            pass

    with runner.isolated_filesystem():
        Path("seeknal_agent.yml").write_text(
            """
sources:
  warehouse:
    source_kind: connected
    source_type: database
    connector: postgresql
    namespace: wh
    access: read_only
    role: business_source_of_truth
""".strip()
        )
        with patch("seeknal.cli.repl.REPL", FakeRepl):
            result = runner.invoke(app, ["test", "wh"])

    assert result.exit_code == 1
    assert "was not attached" in result.stdout
