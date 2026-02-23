"""End-to-end tests for SQL REPL feature.

These tests verify complete REPL workflows including:
- Starting the REPL and viewing help
- Connecting to and querying parquet files
- Source management commands integration
"""

from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from seeknal.cli.main import app


runner = CliRunner()


@pytest.fixture
def valid_key():
    """Generate a valid Fernet key for testing."""
    from cryptography.fernet import Fernet

    return Fernet.generate_key().decode()


class TestReplE2E:
    """E2E tests for interactive SQL REPL."""

    def test_repl_help_command(self):
        """REPL shows help on .help command."""
        result = runner.invoke(app, ["repl"], input=".help\n.quit\n")

        assert ".connect" in result.stdout
        assert ".tables" in result.stdout
        assert ".sources" in result.stdout
        assert ".schema" in result.stdout

    def test_repl_quit_command(self):
        """REPL exits on .quit command."""
        result = runner.invoke(app, ["repl"], input=".quit\n")

        assert result.exit_code == 0

    def test_repl_parquet_file(self, tmp_path):
        """Can connect to and query parquet files."""
        import pandas as pd

        df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        parquet_path = tmp_path / "test.parquet"
        df.to_parquet(parquet_path)

        result = runner.invoke(
            app,
            ["repl"],
            input=f".connect {parquet_path}\nSELECT * FROM db0.data\n.quit\n",
        )

        assert "Connected as 'db0'" in result.stdout
        assert "id" in result.stdout

    def test_repl_csv_file(self, tmp_path):
        """Can connect to and query CSV files."""
        import pandas as pd

        df = pd.DataFrame({"x": [10, 20], "y": [100, 200]})
        csv_path = tmp_path / "test.csv"
        df.to_csv(csv_path, index=False)

        result = runner.invoke(
            app,
            ["repl"],
            input=f".connect {csv_path}\nSELECT * FROM db0.data\n.quit\n",
        )

        assert "Connected as 'db0'" in result.stdout
        assert "x" in result.stdout
        assert "y" in result.stdout

    def test_repl_tables_shows_connected_sources(self, tmp_path):
        """The .tables command shows tables from connected sources."""
        import pandas as pd

        df = pd.DataFrame({"col1": [1, 2]})
        parquet_path = tmp_path / "data.parquet"
        df.to_parquet(parquet_path)

        result = runner.invoke(
            app,
            ["repl"],
            input=f".connect {parquet_path}\n.tables\n.quit\n",
        )

        assert "db0" in result.stdout

    def test_repl_no_sources_shows_warning(self):
        """The .tables command warns when no sources connected."""
        result = runner.invoke(
            app,
            ["repl"],
            input=".tables\n.quit\n",
        )

        assert "No databases connected" in result.stdout


class TestSourceManagementE2E:
    """E2E tests for source management CLI."""

    def test_source_list_empty(self):
        """Source list shows message when empty."""
        with patch("seeknal.cli.source.ReplSourceRequest") as mock:
            mock.select_all.return_value = []

            result = runner.invoke(app, ["source", "list"])

        assert "No sources configured" in result.stdout

    def test_source_add_requires_encryption_key(self, monkeypatch):
        """Adding source without encryption key fails with clear message."""
        monkeypatch.delenv("SEEKNAL_ENCRYPT_KEY", raising=False)

        result = runner.invoke(
            app,
            ["source", "add", "testdb", "--url", "postgres://user:pass@localhost/db"],
        )

        assert result.exit_code == 1
        assert "SEEKNAL_ENCRYPT_KEY not set" in result.stdout

    def test_source_remove_nonexistent(self):
        """Removing non-existent source shows error."""
        with patch("seeknal.cli.source.ReplSourceRequest") as mock:
            mock.select_by_name.return_value = None

            result = runner.invoke(app, ["source", "remove", "nonexistent"])

        assert result.exit_code == 1
        assert "not found" in result.stdout
