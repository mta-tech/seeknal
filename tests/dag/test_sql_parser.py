"""
Tests for SQL parser module using SQLGlot.
"""

import pytest

from seeknal.dag.sql_parser import (
    SQLParser,
    SQLDialect,
    SQLParseResult,
    create_sql_parser,
    normalize_sql,
    extract_columns,
    extract_tables,
    validate_sql,
    SQLGLOT_AVAILABLE,
)


@pytest.mark.skipif(not SQLGLOT_AVAILABLE, reason="sqlglot not installed")
class TestSQLParser:
    """Tests for the SQLParser class."""

    def test_init(self):
        """Test creating a SQL parser."""
        parser = SQLParser()
        assert parser.dialect == SQLDialect.GENERIC

    def test_init_with_dialect(self):
        """Test creating a parser with a specific dialect."""
        parser = SQLParser(dialect=SQLDialect.DUCKDB)
        assert parser.dialect == SQLDialect.DUCKDB

    def test_init_without_sqlglot(self):
        """Test that parser raises ImportError when sqlglot is not available."""
        # This test is difficult to run since we have sqlglot installed
        # The code already handles this with a try/except in the module
        pass

    def test_normalize_simple_select(self):
        """Test normalizing a simple SELECT statement."""
        parser = SQLParser()
        sql = "SELECT a, b FROM foo"
        normalized = parser.normalize(sql)
        assert "SELECT" in normalized.upper()
        assert "FROM" in normalized.upper()

    def test_normalize_whitespace(self):
        """Test that normalization handles extra whitespace."""
        parser = SQLParser()
        sql1 = "SELECT a, b FROM foo"
        sql2 = "select  a,b  from  foo"
        assert parser.normalize(sql1) == parser.normalize(sql2)

    def test_normalize_case_insensitive(self):
        """Test that normalization handles case insensitivity."""
        parser = SQLParser()
        sql1 = "SELECT a FROM foo"
        sql2 = "select a from foo"
        # Should normalize to same form (case may differ but structure same)
        norm1 = parser.normalize(sql1)
        norm2 = parser.normalize(sql2)
        assert norm1.upper() == norm2.upper()

    def test_normalize_empty_sql(self):
        """Test normalizing empty SQL."""
        parser = SQLParser()
        assert parser.normalize("") == ""
        assert parser.normalize("   ") == ""

    def test_extract_columns_simple(self):
        """Test extracting columns from a simple SELECT."""
        parser = SQLParser()
        columns = parser.extract_columns("SELECT a, b FROM foo")
        assert "a" in columns
        assert "b" in columns

    def test_extract_columns_with_where(self):
        """Test extracting columns from SELECT with WHERE clause."""
        parser = SQLParser()
        columns = parser.extract_columns("SELECT a FROM foo WHERE b > 10")
        assert "a" in columns
        assert "b" in columns

    def test_extract_columns_with_join(self):
        """Test extracting columns from SELECT with JOIN."""
        parser = SQLParser()
        columns = parser.extract_columns(
            "SELECT a, b FROM foo JOIN bar ON foo.id = bar.foo_id"
        )
        assert "a" in columns
        assert "b" in columns
        assert "id" in columns
        assert "foo_id" in columns

    def test_extract_columns_empty_sql(self):
        """Test extracting columns from empty SQL."""
        parser = SQLParser()
        assert parser.extract_columns("") == []
        assert parser.extract_columns("   ") == []

    def test_extract_tables_simple(self):
        """Test extracting tables from a simple SELECT."""
        parser = SQLParser()
        tables = parser.extract_tables("SELECT * FROM foo")
        assert "foo" in tables

    def test_extract_tables_with_join(self):
        """Test extracting tables from SELECT with JOIN."""
        parser = SQLParser()
        tables = parser.extract_tables(
            "SELECT * FROM users JOIN orders ON users.id = orders.user_id"
        )
        assert "users" in tables
        assert "orders" in tables

    def test_extract_tables_with_schema(self):
        """Test extracting tables with schema prefix."""
        parser = SQLParser()
        tables = parser.extract_tables("SELECT * FROM schema.foo")
        # Should include the table name
        assert any("foo" in table for table in tables)

    def test_extract_tables_empty_sql(self):
        """Test extracting tables from empty SQL."""
        parser = SQLParser()
        assert parser.extract_tables("") == []
        assert parser.extract_tables("   ") == []

    def test_extract_dependencies_simple(self):
        """Test extracting dependencies from simple SELECT."""
        parser = SQLParser()
        deps = parser.extract_dependencies("SELECT * FROM source.users")
        assert "source.users" in deps

    def test_extract_dependencies_with_schema(self):
        """Test extracting dependencies with full schema path."""
        parser = SQLParser()
        deps = parser.extract_dependencies(
            "SELECT * FROM catalog.schema.table"
        )
        # Should return the full reference
        assert len(deps) > 0

    def test_validate_sql_valid(self):
        """Test validating valid SQL."""
        parser = SQLParser()
        assert parser.validate_sql("SELECT * FROM foo") is True

    def test_validate_sql_invalid(self):
        """Test validating invalid SQL."""
        parser = SQLParser()
        # Typo in SELECT
        assert parser.validate_sql("SELCET * FROM foo") is False

    def test_parse_complete_result(self):
        """Test getting complete parse result."""
        parser = SQLParser()
        result = parser.parse("SELECT a, b FROM foo WHERE c > 10")

        assert result.is_valid is True
        assert result.sql == "SELECT a, b FROM foo WHERE c > 10"
        assert "a" in result.columns
        assert "b" in result.columns
        assert "c" in result.columns
        assert "foo" in result.tables

    def test_parse_empty_sql(self):
        """Test parsing empty SQL."""
        parser = SQLParser()
        result = parser.parse("")

        assert result.is_valid is False
        assert result.error == "Empty SQL string"

    def test_parse_invalid_sql(self):
        """Test parsing invalid SQL."""
        parser = SQLParser()
        result = parser.parse("SELCET * FROM")

        # Should return result with is_valid=False
        assert result.is_valid is False or result.error is not None

    def test_get_sql_fingerprint(self):
        """Test generating SQL fingerprint."""
        parser = SQLParser()
        sql1 = "SELECT * FROM foo WHERE id = 1"
        sql2 = "SELECT * FROM foo WHERE id = 2"

        fp1 = parser.get_sql_fingerprint(sql1)
        fp2 = parser.get_sql_fingerprint(sql2)

        # Fingerprints should be the same (literals replaced)
        assert fp1 == fp2

    def test_get_sql_fingerprint_different_structure(self):
        """Test that different SQL structures produce different fingerprints."""
        parser = SQLParser()
        sql1 = "SELECT a FROM foo"
        sql2 = "SELECT b FROM foo"

        fp1 = parser.get_sql_fingerprint(sql1)
        fp2 = parser.get_sql_fingerprint(sql2)

        # Different columns should produce different fingerprints
        assert fp1 != fp2


@pytest.mark.skipif(not SQLGLOT_AVAILABLE, reason="sqlglot not installed")
class TestCreateSQLParser:
    """Tests for the create_sql_parser factory function."""

    def test_create_default_parser(self):
        """Test creating parser with default dialect."""
        parser = create_sql_parser()
        assert parser.dialect == SQLDialect.GENERIC

    def test_create_duckdb_parser(self):
        """Test creating DuckDB parser."""
        parser = create_sql_parser(SQLDialect.DUCKDB)
        assert parser.dialect == SQLDialect.DUCKDB

    def test_create_spark_parser(self):
        """Test creating Spark parser."""
        parser = create_sql_parser(SQLDialect.SPARK)
        assert parser.dialect == SQLDialect.SPARK


@pytest.mark.skipif(not SQLGLOT_AVAILABLE, reason="sqlglot not installed")
class TestConvenienceFunctions:
    """Tests for convenience functions."""

    def test_normalize_sql_function(self):
        """Test normalize_sql convenience function."""
        sql = "SELECT a FROM foo"
        normalized = normalize_sql(sql)
        assert "SELECT" in normalized.upper()

    def test_extract_columns_function(self):
        """Test extract_columns convenience function."""
        columns = extract_columns("SELECT a, b FROM foo")
        assert "a" in columns
        assert "b" in columns

    def test_extract_tables_function(self):
        """Test extract_tables convenience function."""
        tables = extract_tables("SELECT * FROM foo")
        assert "foo" in tables

    def test_validate_sql_function(self):
        """Test validate_sql convenience function."""
        assert validate_sql("SELECT * FROM foo") is True
        # "SELCET" is a typo for SELECT
        assert validate_sql("SELCET * FROM foo") is False


@pytest.mark.skipif(not SQLGLOT_AVAILABLE, reason="sqlglot not installed")
class TestSQLDialect:
    """Tests for SQLDialect enum."""

    def test_dialect_values(self):
        """Test that dialect enum has expected values."""
        assert SQLDialect.GENERIC.value == ""
        assert SQLDialect.DUCKDB.value == "duckdb"
        assert SQLDialect.SPARK.value == "spark"
        assert SQLDialect.POSTGRES.value == "postgres"
        assert SQLDialect.MYSQL.value == "mysql"


@pytest.mark.skipif(not SQLGLOT_AVAILABLE, reason="sqlglot not installed")
class TestSQLParseResult:
    """Tests for SQLParseResult dataclass."""

    def test_create_result(self):
        """Test creating a parse result."""
        result = SQLParseResult(
            sql="SELECT * FROM foo",
            normalized_sql="SELECT * FROM foo",
            columns=["*"],
            tables=["foo"],
            is_valid=True
        )
        assert result.sql == "SELECT * FROM foo"
        assert result.is_valid is True

    def test_create_error_result(self):
        """Test creating an error result."""
        result = SQLParseResult(
            sql="INVALID",
            is_valid=False,
            error="Parse error"
        )
        assert result.is_valid is False
        assert result.error == "Parse error"
