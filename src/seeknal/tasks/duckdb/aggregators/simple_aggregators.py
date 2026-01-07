"""
Simple DuckDB aggregators.

These aggregators handle basic SQL aggregation operations like sum, avg, count, etc.
"""

from typing import Optional
from enum import Enum

from seeknal.validation import validate_column_name

from .base_aggregator import (
    DuckDBAggregatorFunction,
    AggregateValueType,
)


class FunctionAggregator(DuckDBAggregatorFunction):
    """Aggregate using standard SQL functions.

    Supports common SQL aggregation functions: count, sum, avg, min, max, stddev, etc.

    Attributes:
        inputCol: Column to aggregate
        outputCol: Result column name
        accumulatorFunction: SQL aggregation function (sum, avg, count, min, max, etc.)
        defaultAggregateValue: Optional default value if result is NULL
        defaultAggregateValueType: Optional type for default value

    Example:
        >>> agg = FunctionAggregator(
        ...     inputCol="amount",
        ...     outputCol="total_amount",
        ...     accumulatorFunction="sum"
        ... )
        >>> sql = agg.to_sql()
        >>> # Returns: SUM("amount") AS "total_amount"
    """

    inputCol: str
    outputCol: str
    accumulatorFunction: str
    defaultAggregateValue: Optional[str] = None
    defaultAggregateValueType: Optional[AggregateValueType] = None
    class_name: str = "seeknal.tasks.duckdb.aggregators.FunctionAggregator"

    def model_post_init(self, __context):
        """Validate and set parameters after initialization."""
        validate_column_name(self.inputCol)
        validate_column_name(self.outputCol)

        self.params = {
            "inputCol": self.inputCol,
            "outputCol": self.outputCol,
            "accumulatorFunction": self.accumulatorFunction,
        }

        if self.defaultAggregateValue is not None:
            self.params["defaultAggregateValue"] = self.defaultAggregateValue
        if self.defaultAggregateValueType is not None:
            self.params["defaultAggregateValueType"] = (
                self.defaultAggregateValueType.value
            )

    def to_sql(self) -> str:
        """Generate SQL aggregation fragment.

        Returns:
            SQL fragment like 'SUM("amount") AS "total"'
        """
        sql_func = self.accumulatorFunction.upper()

        # Handle default values with COALESCE
        if self.defaultAggregateValue:
            return (
                f'COALESCE({sql_func}("{self.inputCol}"), {self.defaultAggregateValue}) '
                f'AS "{self.outputCol}"'
            )

        return f'{sql_func}("{self.inputCol}") AS "{self.outputCol}"'


class ExpressionAggregator(DuckDBAggregatorFunction):
    """Aggregate using custom SQL expression.

    Allows arbitrary SQL expressions for complex aggregations.

    Attributes:
        expression: SQL expression for aggregation
        outputCol: Result column name

    Example:
        >>> agg = ExpressionAggregator(
        ...     expression="SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END)",
        ...     outputCol="active_count"
        ... )
        >>> sql = agg.to_sql()
        >>> # Returns: SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) AS "active_count"
    """

    expression: str
    outputCol: str
    class_name: str = "seeknal.tasks.duckdb.aggregators.ExpressionAggregator"

    def model_post_init(self, __context):
        """Validate and set parameters after initialization."""
        validate_column_name(self.outputCol)

        self.params = {"expression": self.expression, "outputCol": self.outputCol}

    def to_sql(self) -> str:
        """Generate SQL aggregation fragment.

        Returns:
            SQL fragment with expression
        """
        return f'{self.expression} AS "{self.outputCol}"'


class DayTypeAggregator(DuckDBAggregatorFunction):
    """Aggregate with day type filtering (weekday vs weekend).

    Uses DuckDB's FILTER clause to conditionally aggregate based on a
    boolean weekday column.

    Attributes:
        inputCol: Column to aggregate
        outputCol: Result column name
        accumulatorFunction: SQL aggregation function
        weekdayCol: Boolean column indicating weekend (TRUE) vs weekday (FALSE)
        defaultAggregateValue: Optional default value if result is NULL
        defaultAggregateValueType: Optional type for default value

    Example:
        >>> agg = DayTypeAggregator(
        ...     inputCol="transactions",
        ...     outputCol="weekend_sum",
        ...     accumulatorFunction="sum",
        ...     weekdayCol="is_weekend"
        ... )
        >>> sql = agg.to_sql()
        >>> # Returns: SUM("transactions") FILTER (WHERE "is_weekend" = TRUE) AS "weekend_sum"
    """

    inputCol: str
    outputCol: str
    accumulatorFunction: str
    weekdayCol: str
    defaultAggregateValue: Optional[str] = None
    defaultAggregateValueType: Optional[AggregateValueType] = None
    class_name: str = "seeknal.tasks.duckdb.aggregators.DayTypeAggregator"

    def model_post_init(self, __context):
        """Validate and set parameters after initialization."""
        validate_column_name(self.inputCol)
        validate_column_name(self.outputCol)
        validate_column_name(self.weekdayCol)

        self.params = {
            "inputCol": self.inputCol,
            "outputCol": self.outputCol,
            "accumulatorFunction": self.accumulatorFunction,
            "weekdayCol": self.weekdayCol,
        }

        if self.defaultAggregateValue:
            self.params["defaultAggregateValue"] = self.defaultAggregateValue
        if self.defaultAggregateValueType:
            self.params["defaultAggregateValueType"] = (
                self.defaultAggregateValueType.value
            )

    def to_sql(self) -> str:
        """Generate SQL aggregation fragment with FILTER clause.

        Returns:
            SQL fragment with FILTER clause
        """
        # Use FILTER clause for conditional aggregation
        base_agg = f'{self.accumulatorFunction.upper()}("{self.inputCol}")'

        if self.defaultAggregateValue:
            base_agg = (
                f'COALESCE({base_agg}, {self.defaultAggregateValue}) '
                f'FILTER (WHERE "{self.weekdayCol}" = TRUE)'
            )
        else:
            base_agg = f'{base_agg} FILTER (WHERE "{self.weekdayCol}" = TRUE)'

        return f'{base_agg} AS "{self.outputCol}"'
