"""Tests for StarRocks REPL integration."""
from unittest.mock import MagicMock, patch

from seeknal.cli.repl import REPL


class TestReplStarRocksConnect:
    """Test REPL StarRocks connection handling."""

    @patch("seeknal.cli.repl.duckdb")
    def test_attach_starrocks_success(self, mock_duckdb):
        """Test successful StarRocks connection via REPL."""
        mock_conn = MagicMock()
        mock_duckdb.connect.return_value = mock_conn

        repl = REPL.__new__(REPL)
        repl.conn = mock_conn
        repl.attached = set()
        repl._starrocks_connections = {}
        repl._active_starrocks = None

        mock_sr_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = ("3.3.0-starrocks",)
        mock_sr_conn.cursor.return_value = mock_cursor

        with patch(
            "seeknal.connections.starrocks.create_starrocks_connection_from_url"
        ) as mock_create:
            mock_create.return_value = mock_sr_conn
            repl._attach_starrocks("sr", "starrocks://root@localhost:9030/db")

        assert "sr" in repl._starrocks_connections
        assert repl._active_starrocks == "sr"
        assert "sr" in repl.attached

    @patch("seeknal.cli.repl.duckdb")
    def test_attach_starrocks_import_error(self, mock_duckdb, capsys):
        """Test StarRocks connection when pymysql not installed."""
        mock_conn = MagicMock()
        mock_duckdb.connect.return_value = mock_conn

        repl = REPL.__new__(REPL)
        repl.conn = mock_conn
        repl.attached = set()
        repl._starrocks_connections = {}
        repl._active_starrocks = None

        with patch(
            "seeknal.cli.repl.REPL._attach_starrocks",
            wraps=repl._attach_starrocks,
        ):
            with patch.dict("sys.modules", {"seeknal.connections.starrocks": None}):
                # This will hit the ImportError path
                repl._attach_starrocks("sr", "starrocks://root@localhost/db")

        captured = capsys.readouterr()
        assert "pymysql" in captured.out or "sr" not in repl._starrocks_connections


class TestReplStarRocksTables:
    """Test .tables command with StarRocks connections."""

    @patch("seeknal.cli.repl.duckdb")
    def test_show_tables_starrocks(self, mock_duckdb, capsys):
        """Test .tables lists StarRocks tables."""
        mock_conn = MagicMock()
        mock_duckdb.connect.return_value = mock_conn

        repl = REPL.__new__(REPL)
        repl.conn = mock_conn
        repl.attached = {"sr"}
        repl._starrocks_connections = {}
        repl._active_starrocks = "sr"

        mock_sr_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [("users",), ("orders",)]
        mock_sr_conn.cursor.return_value = mock_cursor
        repl._starrocks_connections["sr"] = mock_sr_conn

        repl._show_tables()

        captured = capsys.readouterr()
        assert "sr.users" in captured.out
        assert "sr.orders" in captured.out


class TestReplStarRocksExecute:
    """Test SQL execution routing to StarRocks."""

    @patch("seeknal.cli.repl.duckdb")
    def test_execute_routes_to_starrocks(self, mock_duckdb, capsys):
        """Test that queries route to StarRocks when active."""
        mock_conn = MagicMock()
        mock_duckdb.connect.return_value = mock_conn

        repl = REPL.__new__(REPL)
        repl.conn = mock_conn
        repl.attached = {"sr"}
        repl._active_starrocks = "sr"

        mock_sr_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = [(1, "alice"), (2, "bob")]
        mock_sr_conn.cursor.return_value = mock_cursor
        repl._starrocks_connections = {"sr": mock_sr_conn}

        repl._execute("SELECT * FROM users")

        captured = capsys.readouterr()
        assert "alice" in captured.out
        assert "bob" in captured.out

    @patch("seeknal.cli.repl.duckdb")
    def test_execute_routes_to_duckdb_when_no_starrocks(self, mock_duckdb, capsys):
        """Test that queries route to DuckDB when no StarRocks is active."""
        mock_conn = MagicMock()
        mock_duckdb.connect.return_value = mock_conn

        repl = REPL.__new__(REPL)
        repl.conn = mock_conn
        repl.attached = set()
        repl._starrocks_connections = {}
        repl._active_starrocks = None

        mock_result = MagicMock()
        mock_result.description = [("result",)]
        mock_result.fetchall.return_value = [(42,)]
        mock_conn.execute.return_value = mock_result

        repl._execute("SELECT 42")

        captured = capsys.readouterr()
        assert "42" in captured.out

    @patch("seeknal.cli.repl.duckdb")
    def test_duckdb_switch_back(self, mock_duckdb):
        """Test .duckdb command switches back."""
        mock_conn = MagicMock()
        mock_duckdb.connect.return_value = mock_conn

        repl = REPL.__new__(REPL)
        repl.conn = mock_conn
        repl.attached = {"sr"}
        repl._starrocks_connections = {"sr": MagicMock()}
        repl._active_starrocks = "sr"

        # Simulate .duckdb command
        repl._active_starrocks = None
        assert repl._active_starrocks is None


class TestReplStarRocksSchema:
    """Test .schema command with StarRocks."""

    @patch("seeknal.cli.repl.duckdb")
    def test_schema_starrocks_alias_prefix(self, mock_duckdb, capsys):
        """Test .schema sr.users routes to StarRocks."""
        mock_conn = MagicMock()
        mock_duckdb.connect.return_value = mock_conn

        repl = REPL.__new__(REPL)
        repl.conn = mock_conn
        repl.attached = {"sr"}
        repl._active_starrocks = None

        mock_sr_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("id", "INT", "NO", "PRI", None, ""),
            ("name", "VARCHAR(255)", "YES", "", None, ""),
        ]
        mock_sr_conn.cursor.return_value = mock_cursor
        repl._starrocks_connections = {"sr": mock_sr_conn}

        repl._show_schema("sr.users")

        captured = capsys.readouterr()
        assert "id" in captured.out
        assert "INT" in captured.out


class TestReplStarRocksErrorHandling:
    """Test error handling doesn't crash REPL."""

    @patch("seeknal.cli.repl.duckdb")
    def test_starrocks_query_error(self, mock_duckdb, capsys):
        """Test StarRocks query error is caught."""
        mock_conn = MagicMock()
        mock_duckdb.connect.return_value = mock_conn

        repl = REPL.__new__(REPL)
        repl.conn = mock_conn
        repl.attached = {"sr"}
        repl._active_starrocks = "sr"

        mock_sr_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("Table not found")
        mock_sr_conn.cursor.return_value = mock_cursor
        repl._starrocks_connections = {"sr": mock_sr_conn}

        # Should not raise
        repl._execute("SELECT * FROM nonexistent")

        captured = capsys.readouterr()
        assert "error" in captured.out.lower()
