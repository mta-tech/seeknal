"""
Base aggregator classes for DuckDB.

This module provides the foundation for all DuckDB aggregators, mirroring
the SparkEngine aggregator architecture.
"""

from typing import List, Optional
from pydantic import BaseModel
from enum import Enum

from seeknal.validation import validate_column_name


class AggregateValueType(str, Enum):
    """Data types for aggregate values."""

    STRING = "string"
    INT = "int"
    LONG = "bigint"
    DOUBLE = "double"
    FLOAT = "float"


class ColByExpression(BaseModel):
    """Add computed column after aggregation.

    Attributes:
        newName: Name of the new column
        expression: SQL expression to create the column

    Example:
        >>> col = ColByExpression(
        ...     newName="ratio",
        ...     expression="sum_a / sum_b"
        ... )
    """

    newName: str
    expression: str


class RenamedCols(BaseModel):
    """Rename aggregated columns.

    Attributes:
        name: Current column name
        newName: New column name

    Example:
        >>> rename = RenamedCols(name="sum_col", newName="total")
    """

    name: str
    newName: str


class DuckDBAggregatorFunction(BaseModel):
    """Base aggregator function for DuckDB.

    All aggregator functions must implement to_sql() to generate
    the SQL aggregation fragment.

    Attributes:
        class_name: Fully qualified class name
        params: Parameters for the aggregation function
    """

    class_name: str = ""
    params: dict = {}

    def to_sql(self) -> str:
        """Convert to SQL aggregation fragment.

        Returns:
            SQL fragment for the aggregation (e.g., 'SUM("col") AS "total"')

        Raises:
            NotImplementedError: If subclass doesn't implement this method
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement to_sql() method"
        )


class DuckDBAggregator(BaseModel):
    """Main aggregator class for DuckDB.

    Aggregates data by grouping columns and applying aggregation functions.
    Can also include pre and post transformations.

    Attributes:
        group_by_cols: Columns to group by
        pivot_key_col: Optional pivot column for pivoting
        pivot_value_cols: Optional pivot value columns
        col_by_expression: Optional computed columns after aggregation
        renamed_cols: Optional column renames after aggregation
        aggregators: List of aggregation functions to apply
        pre_stages: Optional transformations before aggregation
        post_stages: Optional transformations after aggregation

    Example:
        >>> agg = DuckDBAggregator(
        ...     group_by_cols=["user_id"],
        ...     aggregators=[
        ...         FunctionAggregator(
        ...             inputCol="amount",
        ...             outputCol="total",
        ...             accumulatorFunction="sum"
        ...         )
        ...     ]
        ... )
    """

    group_by_cols: List[str]
    pivot_key_col: Optional[str] = None
    pivot_value_cols: Optional[List[str]] = None
    col_by_expression: Optional[List[ColByExpression]] = None
    renamed_cols: Optional[List[RenamedCols]] = None
    aggregators: List[DuckDBAggregatorFunction]
    pre_stages: Optional[List] = None
    post_stages: Optional[List] = None

    model_config = {"arbitrary_types_allowed": True}
