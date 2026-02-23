"""Tests for seeknal.cli.source module."""
import pytest
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
