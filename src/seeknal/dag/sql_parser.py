"""
SQL parsing module using SQLGlot.

This module provides SQL parsing capabilities for the Seeknal DAG,
including SQL normalization, column extraction, and dependency analysis.

Key features:
- SQL normalization for consistent comparison
- Column extraction from SELECT statements
- Table dependency extraction
- CTE (Common Table Expression) detection
"""

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional, Set, Dict, Any

try:
    import sqlglot
    from sqlglot import exp, parse
    from sqlglot.optimizer import optimize
    SQLGLOT_AVAILABLE = True
except ImportError:
    SQLGLOT_AVAILABLE = False
    # Create dummy types for type annotations when sqlglot is not available
    class exp:
        class Table:
            pass
        class Column:
            pass
        class Select:
            pass
        class CTE:
            pass
        class Literal:
            pass

logger = logging.getLogger(__name__)


class SQLDialect(Enum):
    """Supported SQL dialects."""
    GENERIC = ""  # Generic SQL (no specific dialect)
    DUCKDB = "duckdb"
    SPARK = "spark"  # Use Hive for Spark compatibility
    POSTGRES = "postgres"
    MYSQL = "mysql"
    SNOWFLAKE = "snowflake"
    BIGQUERY = "bigquery"
    TRINO = "trino"  # Use Presto for Trino compatibility


@dataclass
class SQLParseResult:
    """Result of parsing a SQL statement.

    Attributes:
        sql: The original SQL string
        normalized_sql: The normalized SQL string
        columns: List of column names referenced in the statement
        tables: List of table names referenced in the statement
        ctes: Dictionary of CTE name -> definition
        is_valid: Whether the SQL is valid
        error: Error message if parsing failed
    """
    sql: str
    normalized_sql: str = ""
    columns: List[str] = field(default_factory=list)
    tables: List[str] = field(default_factory=list)
    ctes: Dict[str, str] = field(default_factory=dict)
    is_valid: bool = True
    error: Optional[str] = None


class SQLParser:
    """
    SQL parser using SQLGlot for SQL analysis and transformation.

    Provides methods for normalizing SQL, extracting columns and tables,
    and analyzing SQL dependencies for DAG construction.

    Attributes:
        dialect: The SQL dialect to use for parsing (default: ANSI)
        read: The sqlglot read dialect
        write: The sqlglot write dialect
    """

    def __init__(self, dialect: SQLDialect = SQLDialect.GENERIC):
        """
        Initialize the SQL parser.

        Args:
            dialect: The SQL dialect to use for parsing

        Raises:
            ImportError: If sqlglot is not installed
        """
        if not SQLGLOT_AVAILABLE:
            raise ImportError(
                "sqlglot is required for SQL parsing. "
                "Install with: pip install sqlglot~=25.0.0"
            )

        self.dialect = dialect
        # For sqlglot, use the dialect value or empty string for generic
        self.read = dialect.value if dialect.value else None
        self.write = dialect.value if dialect.value else None

    def normalize(self, sql: str, target_dialect: Optional[SQLDialect] = None) -> str:
        """
        Normalize a SQL string for consistent comparison.

        Normalization includes:
        - Removing comments
        - Standardizing whitespace
        - Normalizing identifiers (case, quotes)
        - Reordering SELECT columns
        - Expanding wildcards (optional)

        Args:
            sql: The SQL string to normalize
            target_dialect: Optional target dialect for normalization

        Returns:
            Normalized SQL string

        Examples:
            >>> parser = SQLParser()
            >>> sql1 = "SELECT a, b FROM foo"
            >>> sql2 = "select  a,b  from  foo"
            >>> parser.normalize(sql1) == parser.normalize(sql2)
            True
        """
        if not sql or not sql.strip():
            return ""

        try:
            write_dialect = target_dialect.value if target_dialect else self.write

            # Parse the SQL
            parsed = parse(sql, read=self.read, dialect=self.read)

            if not parsed:
                return sql.strip()

            # Get the first statement
            statement = parsed[0] if isinstance(parsed, list) else parsed

            # Normalize and convert to string
            normalized = statement.sql(dialect=write_dialect, normalize=True, pretty=False)

            return normalized.strip()

        except Exception as e:
            logger.warning(f"Failed to normalize SQL: {e}")
            return sql.strip()

    def extract_columns(self, sql: str) -> List[str]:
        """
        Extract column names referenced in a SQL statement.

        Returns all column names used in SELECT, WHERE, JOIN, GROUP BY,
        HAVING, and ORDER BY clauses.

        Args:
            sql: The SQL string to analyze

        Returns:
            List of column names

        Examples:
            >>> parser = SQLParser()
            >>> columns = parser.extract_columns(
            ...     "SELECT a, b FROM foo WHERE c > 10"
            ... )
            >>> set(columns) >= {"a", "b", "c"}
            True
        """
        if not sql or not sql.strip():
            return []

        try:
            parsed = parse(sql, read=self.read, dialect=self.read)
            if not parsed:
                return []

            statement = parsed[0] if isinstance(parsed, list) else parsed
            columns = set()

            # Traverse the AST to find all column references
            for node in statement.walk():
                if isinstance(node, exp.Column):
                    # Get the column name (without table prefix)
                    column_name = node.name
                    columns.add(column_name)

            return sorted(columns)

        except Exception as e:
            logger.warning(f"Failed to extract columns: {e}")
            return []

    def extract_tables(self, sql: str) -> List[str]:
        """
        Extract table names referenced in a SQL statement.

        Returns all table names from FROM, JOIN, and INSERT INTO clauses.
        CTEs are excluded from the results.

        Args:
            sql: The SQL string to analyze

        Returns:
            List of table names

        Examples:
            >>> parser = SQLParser()
            >>> tables = parser.extract_tables(
            ...     "SELECT * FROM users JOIN orders ON users.id = orders.user_id"
            ... )
            >>> tables
            ['users', 'orders']
        """
        if not sql or not sql.strip():
            return []

        try:
            parsed = parse(sql, read=self.read, dialect=self.read)
            if not parsed:
                return []

            statement = parsed[0] if isinstance(parsed, list) else parsed
            tables = set()
            ctes = self._extract_ctes(sql)

            # Find table references
            for node in statement.walk():
                if isinstance(node, exp.Table):
                    table_name = self._get_table_name(node)
                    if table_name and table_name not in ctes:
                        tables.add(table_name)

            return sorted(tables)

        except Exception as e:
            logger.warning(f"Failed to extract tables: {e}")
            return []

    def extract_dependencies(self, sql: str) -> List[str]:
        """
        Extract table dependencies from a SQL statement.

        Similar to extract_tables but returns table references in the
        format expected by the DAG (e.g., "source.users", "transform.clean_data").

        Args:
            sql: The SQL string to analyze

        Returns:
            List of table dependencies

        Examples:
            >>> parser = SQLParser()
            >>> deps = parser.extract_dependencies(
            ...     "SELECT * FROM source.users"
            ... )
            >>> deps
            ['source.users']
        """
        if not sql or not sql.strip():
            return []

        try:
            parsed = parse(sql, read=self.read, dialect=self.read)
            if not parsed:
                return []

            statement = parsed[0] if isinstance(parsed, list) else parsed
            dependencies = set()
            ctes = self._extract_ctes(sql)

            for node in statement.walk():
                if isinstance(node, exp.Table):
                    # Build full reference with catalog/schema if present
                    table_ref = self._get_full_table_reference(node)
                    if table_ref and table_ref not in ctes:
                        dependencies.add(table_ref)

            return sorted(dependencies)

        except Exception as e:
            logger.warning(f"Failed to extract dependencies: {e}")
            return []

    def _extract_ctes(self, sql: str) -> Set[str]:
        """Extract CTE names from a SQL statement."""
        try:
            parsed = parse(sql, read=self.read, dialect=self.read)
            if not parsed:
                return set()

            statement = parsed[0] if isinstance(parsed, list) else parsed
            ctes = set()

            # Find CTE definitions
            if isinstance(statement, exp.Select):
                for cte in statement.args.get("with"):
                    if isinstance(cte, exp.CTE):
                        ctes.add(cte.alias)

            return ctes

        except Exception:
            return set()

    def _get_table_name(self, table_node: exp.Table) -> Optional[str]:
        """Get the simple table name from a table node."""
        if table_node.this:
            # In sqlglot 25.x, this is an Identifier object
            return table_node.this.name if hasattr(table_node.this, 'name') else str(table_node.this)
        return None

    def _get_full_table_reference(self, table_node: exp.Table) -> Optional[str]:
        """Get the full table reference including catalog and schema."""
        parts = []
        # In sqlglot 25.x, catalog/db/this are Identifier objects
        catalog = table_node.catalog
        db = table_node.db
        this = table_node.this

        if catalog:
            parts.append(catalog.name if hasattr(catalog, 'name') else str(catalog))
        if db:
            parts.append(db.name if hasattr(db, 'name') else str(db))
        if this:
            parts.append(this.name if hasattr(this, 'name') else str(this))

        return ".".join(parts) if parts else None

    def parse(self, sql: str) -> SQLParseResult:
        """
        Parse a SQL statement and extract all information.

        This is a convenience method that combines normalize, extract_columns,
        extract_tables, and CTE extraction into a single call.

        Args:
            sql: The SQL string to parse

        Returns:
            SQLParseResult with all extracted information

        Examples:
            >>> parser = SQLParser()
            >>> result = parser.parse("SELECT a, b FROM foo")
            >>> result.is_valid
            True
            >>> result.columns
            ['a', 'b']
            >>> result.tables
            ['foo']
        """
        if not sql or not sql.strip():
            return SQLParseResult(
                sql=sql,
                is_valid=False,
                error="Empty SQL string"
            )

        # First, try to parse - this will raise ParseError for invalid SQL
        try:
            parsed = parse(sql, read=self.read)
        except Exception as parse_error:
            return SQLParseResult(
                sql=sql,
                is_valid=False,
                error=str(parse_error)
            )

        # If we got here, SQL is valid, extract information
        try:
            normalized = self.normalize(sql)
            columns = self.extract_columns(sql)
            tables = self.extract_tables(sql)
            ctes = self._extract_ctes(sql)

            return SQLParseResult(
                sql=sql,
                normalized_sql=normalized,
                columns=columns,
                tables=tables,
                ctes={name: "" for name in ctes},
                is_valid=True,
                error=None
            )

        except Exception as e:
            # Parsing succeeded but extraction failed - still valid SQL
            return SQLParseResult(
                sql=sql,
                normalized_sql="",
                columns=[],
                tables=[],
                ctes={},
                is_valid=True,
                error=f"Extraction failed: {e}"
            )

    def validate_sql(self, sql: str) -> bool:
        """
        Validate that a SQL string is syntactically correct.

        Args:
            sql: The SQL string to validate

        Returns:
            True if the SQL is valid, False otherwise

        Examples:
            >>> parser = SQLParser()
            >>> parser.validate_sql("SELECT * FROM foo")
            True
            >>> parser.validate_sql("SELCET * FROM foo")
            False
        """
        if not sql or not sql.strip():
            return False

        try:
            # Try to parse - this will raise ParseError for invalid SQL
            parsed = parse(sql, read=self.read)
            # Also verify we can normalize it
            normalized = self.normalize(sql)
            return bool(normalized)
        except Exception:
            return False

    def get_sql_fingerprint(self, sql: str) -> str:
        """
        Generate a fingerprint of a SQL statement for comparison.

        The fingerprint is based on the normalized SQL with literal
        values replaced with placeholders.

        Args:
            sql: The SQL string to fingerprint

        Returns:
            Fingerprint string

        Examples:
            >>> parser = SQLParser()
            >>> sql1 = "SELECT * FROM foo WHERE id = 1"
            >>> sql2 = "SELECT * FROM foo WHERE id = 2"
            >>> parser.get_sql_fingerprint(sql1) == parser.get_sql_fingerprint(sql2)
            True
        """
        if not sql or not sql.strip():
            return ""

        try:
            # Parse and normalize
            normalized = self.normalize(sql)

            # Replace literal values with placeholders
            parsed = parse(normalized, read=self.write)
            if not parsed:
                return normalized

            statement = parsed[0] if isinstance(parsed, list) else parsed

            # Replace literals with placeholders
            for node in statement.walk():
                if isinstance(node, exp.Literal):
                    node.set("this", "?")

            return statement.sql(dialect=self.write)

        except Exception as e:
            logger.warning(f"Failed to generate fingerprint: {e}")
            return self.normalize(sql)


def create_sql_parser(dialect: SQLDialect = SQLDialect.GENERIC) -> SQLParser:
    """
    Factory function to create a SQL parser.

    Args:
        dialect: The SQL dialect to use for parsing

    Returns:
        Configured SQLParser instance

    Examples:
        >>> parser = create_sql_parser(SQLDialect.DUCKDB)
        >>> parser.normalize("SELECT * FROM foo")
        'SELECT * FROM foo'
    """
    return SQLParser(dialect=dialect)


# Convenience functions for common operations

def normalize_sql(sql: str, dialect: SQLDialect = SQLDialect.GENERIC) -> str:
    """Normalize a SQL string."""
    parser = SQLParser(dialect=dialect)
    return parser.normalize(sql)


def extract_columns(sql: str, dialect: SQLDialect = SQLDialect.GENERIC) -> List[str]:
    """Extract columns from a SQL string."""
    parser = SQLParser(dialect=dialect)
    return parser.extract_columns(sql)


def extract_tables(sql: str, dialect: SQLDialect = SQLDialect.GENERIC) -> List[str]:
    """Extract tables from a SQL string."""
    parser = SQLParser(dialect=dialect)
    return parser.extract_tables(sql)


def validate_sql(sql: str, dialect: SQLDialect = SQLDialect.GENERIC) -> bool:
    """Validate a SQL string."""
    parser = SQLParser(dialect=dialect)
    return parser.validate_sql(sql)
