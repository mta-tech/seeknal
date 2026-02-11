"""
Column lineage tracking module using SQLGlot AST analysis.

This module provides column-level lineage tracking for the Seeknal DAG,
including building dependency graphs between input and output columns.

Key features:
- Column-level lineage extraction from SQL
- Input dependency tracking for each output column
- Column transformation detection (direct mapping, expressions, aggregations)
- Lineage graph construction for data flow analysis
"""

import logging
from dataclasses import dataclass, field
from typing import List, Optional, Set, Dict, Any, Tuple

from .sql_parser import SQLParser, SQLDialect, SQLParseResult, SQLGLOT_AVAILABLE

logger = logging.getLogger(__name__)


class ColumnTransformationType:
    """Types of column transformations."""
    DIRECT = "direct"           # Direct column reference (e.g., output_col = input_col)
    EXPRESSION = "expression"   # Expression involving multiple columns
    AGGREGATION = "aggregation" # Aggregate function
    LITERAL = "literal"         # Literal value
    UNKNOWN = "unknown"         # Cannot determine type


@dataclass
class ColumnDependency:
    """
    A dependency from an output column to input columns.

    Attributes:
        output_column: The output column name
        input_columns: Set of input column names this output depends on
        transformation_type: Type of transformation (direct, expression, etc.)
        expression: The SQL expression (if applicable)
        source_table: The source table (if applicable)
    """
    output_column: str
    input_columns: Set[str]
    transformation_type: str = ColumnTransformationType.UNKNOWN
    expression: Optional[str] = None
    source_table: Optional[str] = None

    def __str__(self) -> str:
        """String representation of the dependency."""
        if self.transformation_type == ColumnTransformationType.DIRECT and len(self.input_columns) == 1:
            input_col = next(iter(self.input_columns))
            return f"{self.output_column} <- {input_col}"
        elif self.input_columns:
            inputs = ", ".join(sorted(self.input_columns))
            return f"{self.output_column} <- f({inputs})"
        else:
            return f"{self.output_column} <- (literal or unknown)"


@dataclass
class ColumnLineage:
    """
    Column-level lineage for a SQL statement.

    Tracks the dependency graph from output columns to their input sources.

    Attributes:
        output_columns: List of output column names
        input_dependencies: Dict mapping output column to set of input columns
        dependencies: List of ColumnDependency objects
        source_tables: Set of source table names referenced
    """
    output_columns: List[str]
    input_dependencies: Dict[str, Set[str]]
    dependencies: List[ColumnDependency]
    source_tables: Set[str] = field(default_factory=set)

    def get_dependencies_for_column(self, column: str) -> Optional[Set[str]]:
        """
        Get input dependencies for a specific output column.

        Args:
            column: The output column name

        Returns:
            Set of input column names, or None if column not found
        """
        return self.input_dependencies.get(column)

    def is_direct_mapping(self, column: str) -> bool:
        """
        Check if an output column is a direct mapping from an input column.

        Args:
            column: The output column name

        Returns:
            True if the column is a direct mapping (e.g., output_col = input_col)
        """
        dep = self.get_dependencies_for_column(column)
        if dep and len(dep) == 1:
            # Check if it's a direct mapping (same name or single input)
            return True
        return False

    def get_lineage_graph(self) -> Dict[str, List[str]]:
        """
        Get the lineage graph as an adjacency list.

        Returns:
            Dict mapping output column to list of input columns
        """
        return {
            col: sorted(deps) for col, deps in self.input_dependencies.items()
        }


class LineageBuilder:
    """
    Column lineage builder using SQLGlot AST analysis.

    Analyzes SQL statements to extract column-level dependencies
    and build lineage graphs.

    Attributes:
        parser: SQLParser instance for parsing SQL
    """

    def __init__(self, dialect: SQLDialect = SQLDialect.GENERIC):
        """
        Initialize the lineage builder.

        Args:
            dialect: The SQL dialect to use for parsing

        Raises:
            ImportError: If sqlglot is not installed
        """
        if not SQLGLOT_AVAILABLE:
            raise ImportError(
                "sqlglot is required for column lineage. "
                "Install with: pip install sqlglot~=25.0.0"
            )

        self.parser = SQLParser(dialect=dialect)

    def build_lineage(self, sql: str) -> Optional[ColumnLineage]:
        """
        Build column lineage for a SQL statement.

        Analyzes the SQL to extract output columns and their input dependencies.

        Args:
            sql: The SQL statement to analyze

        Returns:
            ColumnLineage object, or None if parsing fails

        Examples:
            >>> builder = LineageBuilder()
            >>> lineage = builder.build_lineage("SELECT a, b FROM foo")
            >>> lineage.output_columns
            ['a', 'b']
        """
        # Parse the SQL
        parse_result = self.parser.parse(sql)
        if not parse_result.is_valid:
            logger.warning(f"Failed to parse SQL for lineage: {parse_result.error}")
            return None

        # Extract output columns
        output_columns = self._extract_output_columns(sql)
        if not output_columns:
            logger.warning("No output columns found in SQL")
            return None

        # Extract input dependencies for each output column
        input_dependencies: Dict[str, Set[str]] = {}
        dependencies: List[ColumnDependency] = []

        for output_col in output_columns:
            deps = self._extract_dependencies_for_column(sql, output_col)
            input_dependencies[output_col] = deps["input_columns"]

            dep_obj = ColumnDependency(
                output_column=output_col,
                input_columns=deps["input_columns"],
                transformation_type=deps["transformation_type"],
                expression=deps.get("expression"),
                source_table=deps.get("source_table")
            )
            dependencies.append(dep_obj)

        # Extract source tables
        source_tables = set(parse_result.tables)

        return ColumnLineage(
            output_columns=output_columns,
            input_dependencies=input_dependencies,
            dependencies=dependencies,
            source_tables=source_tables
        )

    def _extract_output_columns(self, sql: str) -> List[str]:
        """Extract output column names from a SELECT statement."""
        try:
            from sqlglot import parse, exp

            parsed = parse(sql, read=self.parser.read)
            if not parsed:
                return []

            statement = parsed[0] if isinstance(parsed, list) else parsed

            if not isinstance(statement, exp.Select):
                return []

            output_columns = []

            # Get SELECT clause projections
            for projection in statement.expressions:
                if isinstance(projection, exp.Column):
                    # Simple column reference
                    col_name = projection.alias or projection.name
                    output_columns.append(col_name)
                elif isinstance(projection, exp.Alias):
                    # Column with alias
                    alias_name = projection.alias if hasattr(projection, 'alias') else projection.this.alias if hasattr(projection.this, 'alias') else None
                    if alias_name:
                        output_columns.append(alias_name)
                    else:
                        # Use the aliased expression's name
                        if hasattr(projection.this, 'name'):
                            output_columns.append(projection.this.name)
                else:
                    # Expression - try to get alias or generate one
                    if hasattr(projection, 'alias') and projection.alias:
                        output_columns.append(projection.alias)
                    else:
                        # Generate a name for the expression
                        output_columns.append(f"expr_{len(output_columns)}")

            return output_columns

        except Exception as e:
            logger.warning(f"Failed to extract output columns: {e}")
            return []

    def _extract_dependencies_for_column(self, sql: str, output_column: str) -> Dict[str, Any]:
        """
        Extract input dependencies for a specific output column.

        Args:
            sql: The SQL statement
            output_column: The output column name

        Returns:
            Dict with input_columns (set), transformation_type, expression, source_table
        """
        try:
            from sqlglot import parse, exp

            parsed = parse(sql, read=self.parser.read)
            if not parsed:
                return {"input_columns": set(), "transformation_type": ColumnTransformationType.UNKNOWN}

            statement = parsed[0] if isinstance(parsed, list) else parsed

            if not isinstance(statement, exp.Select):
                return {"input_columns": set(), "transformation_type": ColumnTransformationType.UNKNOWN}

            # Find the projection for this output column
            target_projection = None
            for projection in statement.expressions:
                # Check if this projection matches our output column
                projection_name = None
                if isinstance(projection, exp.Column):
                    projection_name = projection.alias or projection.name
                elif isinstance(projection, exp.Alias):
                    projection_name = projection.alias
                elif hasattr(projection, 'alias') and projection.alias:
                    projection_name = projection.alias

                if projection_name == output_column:
                    target_projection = projection
                    break

            if not target_projection:
                return {"input_columns": set(), "transformation_type": ColumnTransformationType.UNKNOWN}

            # Extract column references from the projection
            input_columns = set()
            transformation_type = ColumnTransformationType.UNKNOWN
            expression = None
            source_table = None

            # Walk the projection tree to find column references
            for node in target_projection.walk():
                if isinstance(node, exp.Column):
                    col_name = node.name
                    input_columns.add(col_name)

                    # Try to get the table name
                    if hasattr(node, 'table') and node.table:
                        if hasattr(node.table, 'name'):
                            source_table = node.table.name

            # Determine transformation type
            if isinstance(target_projection, exp.Column):
                if len(input_columns) == 1:
                    transformation_type = ColumnTransformationType.DIRECT
            elif any(isinstance(target_projection, t) for t in [exp.AggFunc, exp.Count, exp.Sum, exp.Avg, exp.Max, exp.Min]):
                transformation_type = ColumnTransformationType.AGGREGATION
            elif len(input_columns) > 1:
                transformation_type = ColumnTransformationType.EXPRESSION
            elif len(input_columns) == 0:
                transformation_type = ColumnTransformationType.LITERAL
            else:
                transformation_type = ColumnTransformationType.EXPRESSION

            # Get expression string
            try:
                expression = target_projection.sql()
            except:
                expression = None

            return {
                "input_columns": input_columns,
                "transformation_type": transformation_type,
                "expression": expression,
                "source_table": source_table
            }

        except Exception as e:
            logger.warning(f"Failed to extract dependencies for column {output_column}: {e}")
            return {"input_columns": set(), "transformation_type": ColumnTransformationType.UNKNOWN}

    def get_upstream_columns(self, sql: str, column: str) -> Set[str]:
        """
        Get all upstream columns that a given output column depends on.

        Args:
            sql: The SQL statement
            column: The output column name

        Returns:
            Set of upstream column names
        """
        lineage = self.build_lineage(sql)
        if not lineage:
            return set()

        return lineage.get_dependencies_for_column(column) or set()

    def get_downstream_impact(self, sql: str, removed_input_column: str) -> List[str]:
        """
        Find which output columns would be affected if an input column is removed.

        Args:
            sql: The SQL statement
            removed_input_column: The input column that would be removed

        Returns:
            List of output columns that depend on the input
        """
        lineage = self.build_lineage(sql)
        if not lineage:
            return []

        affected = []
        for output_col, input_cols in lineage.input_dependencies.items():
            if removed_input_column in input_cols:
                affected.append(output_col)

        return affected


def create_lineage_builder(dialect: SQLDialect = SQLDialect.GENERIC) -> LineageBuilder:
    """
    Factory function to create a lineage builder.

    Args:
        dialect: The SQL dialect to use for parsing

    Returns:
        Configured LineageBuilder instance

    Examples:
        >>> builder = create_lineage_builder(SQLDialect.DUCKDB)
        >>> lineage = builder.build_lineage("SELECT a, b FROM foo")
    """
    return LineageBuilder(dialect=dialect)


# Convenience functions

def build_column_lineage(
    sql: str,
    dialect: SQLDialect = SQLDialect.GENERIC
) -> Optional[ColumnLineage]:
    """Build column lineage for a SQL statement."""
    builder = LineageBuilder(dialect=dialect)
    return builder.build_lineage(sql)


def get_upstream_columns(
    sql: str,
    column: str,
    dialect: SQLDialect = SQLDialect.GENERIC
) -> Set[str]:
    """Get upstream columns for an output column."""
    builder = LineageBuilder(dialect=dialect)
    return builder.get_upstream_columns(sql, column)


def get_downstream_impact(
    sql: str,
    removed_input_column: str,
    dialect: SQLDialect = SQLDialect.GENERIC
) -> List[str]:
    """Get downstream impact of removing an input column."""
    builder = LineageBuilder(dialect=dialect)
    return builder.get_downstream_impact(sql, removed_input_column)
