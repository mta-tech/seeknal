"""
Base transformer classes for DuckDB.

This module provides the foundation for all DuckDB transformers, including
enumerations and base classes that mirror the SparkEngine architecture.
"""

from enum import Enum
from typing import List, Optional
from pydantic import BaseModel


class DuckDBClassName(str, Enum):
    """Enumeration of DuckDB transformer class names.

    Follows same naming convention as SparkEngine for consistency.
    """

    FILTER_BY_EXPR = "seeknal.tasks.duckdb.transformers.FilterByExpr"
    COLUMN_RENAMED = "seeknal.tasks.duckdb.transformers.ColumnRenamed"
    ADD_COLUMN_BY_EXPR = "seeknal.tasks.duckdb.transformers.AddColumnByExpr"
    SQL = "seeknal.tasks.duckdb.transformers.SQL"
    ADD_WINDOW_FUNCTION = "seeknal.tasks.duckdb.transformers.AddWindowFunction"
    POINT_IN_TIME = "seeknal.tasks.duckdb.transformers.PointInTime"
    JOIN_BY_EXPR = "seeknal.tasks.duckdb.transformers.JoinTablesByExpr"
    SELECT_COLUMNS = "seeknal.tasks.duckdb.transformers.SelectColumns"
    DROP_COLS = "seeknal.tasks.duckdb.transformers.DropCols"
    CAST_COLUMN = "seeknal.tasks.duckdb.transformers.CastColumn"


class DuckDBTransformer(BaseModel):
    """Base class for all DuckDB transformers.

    Mirrors the structure of SparkEngine Transformer but adapted for DuckDB.
    All transformers must implement the to_sql() method to generate SQL.

    Attributes:
        kind: Optional transformer kind from DuckDBClassName enum
        class_name: Fully qualified class name
        params: Dictionary of parameters for the transformer
        description: Optional description of the transformer

    Example:
        class SQL(DuckDBTransformer):
            statement: str

            def to_sql(self) -> str:
                return self.statement
    """

    kind: Optional[DuckDBClassName] = None
    class_name: str = ""
    params: dict = {}
    description: Optional[str] = None

    def to_sql(self) -> str:
        """Convert transformer to SQL statement.

        Returns:
            SQL string that implements this transformation.

        Raises:
            NotImplementedError: If subclass doesn't implement this method
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement to_sql() method"
        )

    def update_param(self, key: str, value):
        """Update a parameter.

        Args:
            key: Parameter name
            value: New value

        Returns:
            self for method chaining
        """
        self.params[key] = value
        return self

    def update_description(self, description: str):
        """Update description.

        Args:
            description: New description

        Returns:
            self for method chaining
        """
        self.description = description
        return self

    model_config = {"arbitrary_types_allowed": True, "frozen": False}
