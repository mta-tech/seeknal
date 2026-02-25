"""Tests for seeknal repl --exec one-shot query execution."""
import json
import pytest
from unittest.mock import MagicMock, patch

from seeknal.cli.repl import REPL, validate_sql


# ---------------------------------------------------------------------------
# Unit tests for REPL.execute_oneshot()
# ---------------------------------------------------------------------------

@pytest.fixture
def repl_exec():
    """Create REPL instance with mocked DuckDB and skip_history=True."""
    with patch("seeknal.cli.repl.duckdb") as mock_duckdb:
        mock_conn = MagicMock()
        mock_duckdb.connect.return_value = mock_conn
        r = REPL(skip_history=True)
        yield r, mock_conn


class TestExecuteOneshot:
    """Unit tests for REPL.execute_oneshot()."""

    def test_basic_query(self, repl_exec):
        """Basic SELECT returns (columns, rows)."""
        r, mock_conn = repl_exec
        mock_result = MagicMock()
        mock_result.description = [("id",), ("name",)]
        mock_result.fetchall.return_value = [(1, "alice"), (2, "bob")]
        mock_conn.execute.return_value = mock_result

        columns, rows = r.execute_oneshot("SELECT * FROM users")

        assert columns == ["id", "name"]
        assert rows == [(1, "alice"), (2, "bob")]

    def test_empty_result_set(self, repl_exec):
        """Query with no rows returns empty lists."""
        r, mock_conn = repl_exec
        mock_result = MagicMock()
        mock_result.description = [("id",)]
        mock_result.fetchall.return_value = []
        mock_conn.execute.return_value = mock_result

        columns, rows = r.execute_oneshot("SELECT * FROM empty_table")

        assert columns == ["id"]
        assert rows == []

    def test_no_description_returns_empty(self, repl_exec):
        """Query with no result description returns ([], [])."""
        r, mock_conn = repl_exec
        mock_result = MagicMock()
        mock_result.description = None
        mock_conn.execute.return_value = mock_result

        columns, rows = r.execute_oneshot("SHOW TABLES")

        assert columns == []
        assert rows == []

    def test_sql_validation_reject(self, repl_exec):
        """Dangerous SQL is rejected with ValueError."""
        r, _ = repl_exec

        with pytest.raises(ValueError, match="Query rejected"):
            r.execute_oneshot("DROP TABLE users")

    def test_multi_statement_rejected(self, repl_exec):
        """Multiple SQL statements separated by ; are rejected."""
        r, _ = repl_exec

        with pytest.raises(ValueError, match="single SQL statements"):
            r.execute_oneshot("SELECT 1; SELECT 2")

    def test_empty_sql_passes_validation(self, repl_exec):
        """Empty SQL passes validate_sql but is harmless."""
        r, mock_conn = repl_exec
        mock_result = MagicMock()
        mock_result.description = None
        mock_conn.execute.return_value = mock_result

        columns, rows = r.execute_oneshot("")

        assert columns == []
        assert rows == []

    def test_limit_wraps_query(self, repl_exec):
        """--limit wraps the SQL with SELECT * FROM (...) LIMIT N."""
        r, mock_conn = repl_exec
        mock_result = MagicMock()
        mock_result.description = [("id",)]
        mock_result.fetchall.return_value = [(1,), (2,)]
        mock_conn.execute.return_value = mock_result

        r.execute_oneshot("SELECT * FROM orders", limit=10)

        call_sql = mock_conn.execute.call_args[0][0]
        assert "LIMIT 10" in call_sql
        assert "SELECT * FROM orders" in call_sql

    def test_stdin_dash(self, repl_exec):
        """sql='-' reads from stdin."""
        r, mock_conn = repl_exec
        mock_result = MagicMock()
        mock_result.description = [("c",)]
        mock_result.fetchall.return_value = [(1,)]
        mock_conn.execute.return_value = mock_result

        with patch("sys.stdin") as mock_stdin:
            mock_stdin.isatty.return_value = False
            mock_stdin.read.return_value = "SELECT 1 AS c"

            columns, rows = r.execute_oneshot("-")

        assert columns == ["c"]
        assert rows == [(1,)]

    def test_stdin_empty_raises(self, repl_exec):
        """Empty stdin raises ValueError."""
        r, _ = repl_exec

        with patch("sys.stdin") as mock_stdin:
            mock_stdin.isatty.return_value = False
            mock_stdin.read.return_value = ""

            with pytest.raises(ValueError, match="No SQL provided"):
                r.execute_oneshot("-")

    def test_stdin_multi_statement_rejected(self, repl_exec):
        """Multi-statement SQL from stdin is rejected."""
        r, _ = repl_exec

        with patch("sys.stdin") as mock_stdin:
            mock_stdin.isatty.return_value = False
            mock_stdin.read.return_value = "SELECT 1; SELECT 2"

            with pytest.raises(ValueError, match="single SQL statements"):
                r.execute_oneshot("-")

    def test_stdin_tty_prints_hint(self, repl_exec, capsys):
        """When stdin is a TTY, a hint is printed to stderr."""
        r, mock_conn = repl_exec
        mock_result = MagicMock()
        mock_result.description = [("c",)]
        mock_result.fetchall.return_value = [(1,)]
        mock_conn.execute.return_value = mock_result

        with patch("sys.stdin") as mock_stdin:
            mock_stdin.isatty.return_value = True
            mock_stdin.read.return_value = "SELECT 1 AS c"

            r.execute_oneshot("-")

        captured = capsys.readouterr()
        assert "Reading SQL from stdin" in captured.err

    def test_trailing_semicolon_ok(self, repl_exec):
        """A single statement with trailing semicolon is accepted."""
        r, mock_conn = repl_exec
        mock_result = MagicMock()
        mock_result.description = [("c",)]
        mock_result.fetchall.return_value = [(1,)]
        mock_conn.execute.return_value = mock_result

        columns, rows = r.execute_oneshot("SELECT 1 AS c;")

        assert columns == ["c"]

    def test_no_max_rows_limit(self, repl_exec):
        """execute_oneshot returns all rows (no MAX_ROWS=100 cap)."""
        r, mock_conn = repl_exec
        mock_result = MagicMock()
        mock_result.description = [("id",)]
        mock_result.fetchall.return_value = [(i,) for i in range(500)]
        mock_conn.execute.return_value = mock_result

        columns, rows = r.execute_oneshot("SELECT * FROM big_table")

        assert len(rows) == 500


# ---------------------------------------------------------------------------
# Unit tests for skip_history parameter
# ---------------------------------------------------------------------------

class TestSkipHistory:
    """Tests for skip_history parameter on REPL.__init__."""

    def test_skip_history_true(self):
        """skip_history=True skips readline history setup."""
        with patch("seeknal.cli.repl.duckdb") as mock_duckdb:
            mock_duckdb.connect.return_value = MagicMock()
            with patch.object(REPL, "_setup_history") as mock_setup:
                REPL(skip_history=True)
                mock_setup.assert_not_called()

    def test_skip_history_false_default(self):
        """skip_history=False (default) calls _setup_history."""
        with patch("seeknal.cli.repl.duckdb") as mock_duckdb:
            mock_duckdb.connect.return_value = MagicMock()
            with patch.object(REPL, "_setup_history") as mock_setup:
                REPL(skip_history=False)
                mock_setup.assert_called_once()


# ---------------------------------------------------------------------------
# CLI integration tests for seeknal repl --exec
# ---------------------------------------------------------------------------

from typer.testing import CliRunner
from seeknal.cli.main import app

cli_runner = CliRunner()


class TestReplExecCLI:
    """CLI integration tests for --exec flag."""

    @patch("seeknal.cli.repl.REPL")
    def test_exec_table_format(self, MockREPL):
        """--exec with default table format outputs tabulate."""
        mock_instance = MagicMock()
        mock_instance.execute_oneshot.return_value = (
            ["id", "name"],
            [(1, "alice"), (2, "bob")],
        )
        MockREPL.return_value = mock_instance

        result = cli_runner.invoke(app, ["repl", "--exec", "SELECT * FROM t"])

        assert result.exit_code == 0
        assert "id" in result.stdout
        assert "alice" in result.stdout

    @patch("seeknal.cli.repl.REPL")
    def test_exec_json_format(self, MockREPL):
        """--exec --format json outputs JSON array."""
        mock_instance = MagicMock()
        mock_instance.execute_oneshot.return_value = (
            ["id", "name"],
            [(1, "alice")],
        )
        MockREPL.return_value = mock_instance

        result = cli_runner.invoke(
            app, ["repl", "--exec", "SELECT * FROM t", "--format", "json"]
        )

        assert result.exit_code == 0
        data = json.loads(result.stdout)
        assert data == [{"id": 1, "name": "alice"}]

    @patch("seeknal.cli.repl.REPL")
    def test_exec_csv_format(self, MockREPL):
        """--exec --format csv outputs CSV with header."""
        mock_instance = MagicMock()
        mock_instance.execute_oneshot.return_value = (
            ["id", "name"],
            [(1, "alice"), (2, "bob")],
        )
        MockREPL.return_value = mock_instance

        result = cli_runner.invoke(
            app, ["repl", "--exec", "SELECT * FROM t", "--format", "csv"]
        )

        assert result.exit_code == 0
        lines = result.stdout.strip().split("\n")
        assert lines[0] == "id,name"
        assert lines[1] == "1,alice"
        assert lines[2] == "2,bob"

    @patch("seeknal.cli.repl.REPL")
    def test_exec_output_csv(self, MockREPL, tmp_path):
        """--exec --output file.csv exports to CSV."""
        mock_instance = MagicMock()
        mock_instance.execute_oneshot.return_value = (
            ["id", "val"],
            [(1, 10), (2, 20)],
        )
        MockREPL.return_value = mock_instance

        out_file = tmp_path / "out.csv"
        result = cli_runner.invoke(
            app,
            ["repl", "--exec", "SELECT * FROM t", "--output", str(out_file)],
        )

        assert result.exit_code == 0
        content = out_file.read_text()
        assert "id,val" in content
        assert "1,10" in content

    @patch("seeknal.cli.repl.REPL")
    def test_exec_output_json(self, MockREPL, tmp_path):
        """--exec --output file.json exports to JSON."""
        mock_instance = MagicMock()
        mock_instance.execute_oneshot.return_value = (
            ["id"],
            [(1,), (2,)],
        )
        MockREPL.return_value = mock_instance

        out_file = tmp_path / "out.json"
        result = cli_runner.invoke(
            app,
            ["repl", "--exec", "SELECT * FROM t", "--output", str(out_file)],
        )

        assert result.exit_code == 0
        data = json.loads(out_file.read_text())
        assert len(data) == 2

    @patch("seeknal.cli.repl.REPL")
    def test_exec_output_parquet(self, MockREPL, tmp_path):
        """--exec --output file.parquet exports to Parquet."""
        mock_instance = MagicMock()
        mock_instance.execute_oneshot.return_value = (
            ["id", "val"],
            [(1, 10), (2, 20)],
        )
        MockREPL.return_value = mock_instance

        out_file = tmp_path / "out.parquet"
        result = cli_runner.invoke(
            app,
            ["repl", "--exec", "SELECT * FROM t", "--output", str(out_file)],
        )

        assert result.exit_code == 0
        assert out_file.exists()

        import pandas as pd
        df = pd.read_parquet(out_file)
        assert len(df) == 2
        assert list(df.columns) == ["id", "val"]

    @patch("seeknal.cli.repl.REPL")
    def test_exec_output_unsupported_ext(self, MockREPL, tmp_path):
        """--exec --output file.xlsx fails with unsupported format."""
        mock_instance = MagicMock()
        mock_instance.execute_oneshot.return_value = (["id"], [(1,)])
        MockREPL.return_value = mock_instance

        out_file = tmp_path / "out.xlsx"
        result = cli_runner.invoke(
            app,
            ["repl", "--exec", "SELECT 1", "--output", str(out_file)],
        )

        assert result.exit_code == 1

    @patch("seeknal.cli.repl.REPL")
    def test_exec_validation_error_exit_code_1(self, MockREPL):
        """SQL validation failure returns exit code 1."""
        mock_instance = MagicMock()
        mock_instance.execute_oneshot.side_effect = ValueError("Query rejected: blocked")
        MockREPL.return_value = mock_instance

        result = cli_runner.invoke(
            app, ["repl", "--exec", "DROP TABLE users"]
        )

        assert result.exit_code == 1

    @patch("seeknal.cli.repl.REPL")
    def test_exec_execution_error_exit_code_1(self, MockREPL):
        """DuckDB execution error returns exit code 1."""
        mock_instance = MagicMock()
        mock_instance.execute_oneshot.side_effect = Exception("table not found")
        MockREPL.return_value = mock_instance

        result = cli_runner.invoke(
            app, ["repl", "--exec", "SELECT * FROM nonexistent"]
        )

        assert result.exit_code == 1

    def test_format_without_exec_fails(self):
        """--format without --exec fails."""
        result = cli_runner.invoke(
            app, ["repl", "--format", "json"]
        )

        assert result.exit_code == 1

    def test_output_without_exec_fails(self, tmp_path):
        """--output without --exec fails."""
        result = cli_runner.invoke(
            app, ["repl", "--output", str(tmp_path / "x.csv")]
        )

        assert result.exit_code == 1

    def test_limit_without_exec_fails(self):
        """--limit without --exec fails."""
        result = cli_runner.invoke(
            app, ["repl", "--limit", "10"]
        )

        assert result.exit_code == 1

    @patch("seeknal.cli.repl.REPL")
    def test_exec_with_limit(self, MockREPL):
        """--exec with --limit passes limit to execute_oneshot."""
        mock_instance = MagicMock()
        mock_instance.execute_oneshot.return_value = (["id"], [(1,)])
        MockREPL.return_value = mock_instance

        result = cli_runner.invoke(
            app, ["repl", "--exec", "SELECT * FROM t", "--limit", "5"]
        )

        assert result.exit_code == 0
        mock_instance.execute_oneshot.assert_called_once_with(
            "SELECT * FROM t", limit=5
        )

    @patch("seeknal.cli.repl.REPL")
    def test_exec_no_result_query(self, MockREPL):
        """--exec with no-result query (e.g., PRAGMA) exits 0."""
        mock_instance = MagicMock()
        mock_instance.execute_oneshot.return_value = ([], [])
        MockREPL.return_value = mock_instance

        result = cli_runner.invoke(
            app, ["repl", "--exec", "PRAGMA version"]
        )

        assert result.exit_code == 0

    @patch("seeknal.cli.repl.REPL")
    def test_exec_empty_result_json(self, MockREPL):
        """--exec with empty result set outputs [] for JSON."""
        mock_instance = MagicMock()
        mock_instance.execute_oneshot.return_value = (["id"], [])
        MockREPL.return_value = mock_instance

        result = cli_runner.invoke(
            app, ["repl", "--exec", "SELECT * FROM t WHERE 1=0", "--format", "json"]
        )

        assert result.exit_code == 0
        data = json.loads(result.stdout)
        assert data == []

    @patch("seeknal.cli.repl.REPL")
    def test_exec_empty_result_csv(self, MockREPL):
        """--exec with empty result set outputs header-only CSV."""
        mock_instance = MagicMock()
        mock_instance.execute_oneshot.return_value = (["id", "name"], [])
        MockREPL.return_value = mock_instance

        result = cli_runner.invoke(
            app,
            ["repl", "--exec", "SELECT * FROM t WHERE 1=0", "--format", "csv"],
        )

        assert result.exit_code == 0
        lines = result.stdout.strip().split("\n")
        assert lines[0] == "id,name"
        assert len(lines) == 1

    @patch("seeknal.cli.repl.REPL")
    def test_exec_skip_history(self, MockREPL):
        """--exec creates REPL with skip_history=True."""
        mock_instance = MagicMock()
        mock_instance.execute_oneshot.return_value = (["c"], [(1,)])
        MockREPL.return_value = mock_instance

        cli_runner.invoke(app, ["repl", "--exec", "SELECT 1"])

        MockREPL.assert_called_once()
        call_kwargs = MockREPL.call_args[1]
        assert call_kwargs["skip_history"] is True
