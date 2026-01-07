"""
Complex DuckDB aggregators.

These aggregators handle time-based windowing and advanced aggregation patterns.
"""

from typing import List, Optional
from dataclasses import dataclass

from .base_aggregator import DuckDBAggregator, DuckDBAggregatorFunction, RenamedCols
from ..transformers import SQL, ColumnRenamed, FilterByExpr
from seeknal.validation import validate_column_name


class LastNDaysAggregator(DuckDBAggregator):
    """Time-based windowing aggregation.

    Creates a rolling window of N days from the latest date in the dataset,
    then applies aggregations within that window.

    Attributes:
        group_by_cols: Columns to group by
        window: Number of days to lookback from latest date
        date_col: Column that contains date
        date_pattern: Date pattern that describes the date value
        aggregators: List of aggregation functions to apply
        pivot_key_col: Optional pivot column
        pivot_value_cols: Optional pivot value columns
        col_by_expression: Optional computed columns
        renamed_cols: Optional column renames

    Example:
        >>> agg = LastNDaysAggregator(
        ...     group_by_cols=["user_id"],
        ...     window=10,
        ...     date_col="date_id",
        ...     date_pattern="yyyyMMdd",
        ...     aggregators=[
        ...         FunctionAggregator(
        ...             inputCol="amount",
        ...             outputCol="amount_sum_10d",
        ...             accumulatorFunction="sum"
        ...         )
        ...     ]
        ... )
    """

    window: int
    date_col: str
    date_pattern: str
    # Inherits group_by_cols, aggregators, pivot_key_col, etc. from DuckDBAggregator

    def model_post_init(self, __context):
        """Build pre and post stages for windowing."""
        from . import FunctionAggregator

        # Pre-stages: Calculate window bounds and filter
        # First, try to parse the date column according to the pattern
        if self.date_pattern == "yyyyMMdd":
            # For yyyyMMdd format, we need to parse it first
            date_cast_expr = f'STRPTIME("{self.date_col}", \'%Y%m%d\')'
        elif self.date_pattern == "yyyy-MM-dd":
            date_cast_expr = f'CAST("{self.date_col}" AS DATE)'
        else:
            # Default: try to cast as DATE
            date_cast_expr = f'CAST("{self.date_col}" AS DATE)'

        self.pre_stages = [
            SQL(statement=f"""
                SELECT *,
                    MAX({date_cast_expr}) OVER () AS _max_date,
                    MAX({date_cast_expr}) OVER () - INTERVAL '{self.window} days' AS _window_start
                FROM __THIS__
            """),
            FilterByExpr(expression=f'{date_cast_expr} >= _window_start'),
        ]

        # Add max_date to group by columns
        if self.group_by_cols is None:
            raise ValueError("group_by_cols must be set")

        new_group_by_cols = self.group_by_cols + ["_max_date"]
        object.__setattr__(self, "group_by_cols", new_group_by_cols)

        # Post-stages: Restore original date column name
        new_post_stages = [
            ColumnRenamed(inputCol="_max_date", outputCol=self.date_col)
        ]

        if self.post_stages is not None:
            new_post_stages.extend(self.post_stages)

        object.__setattr__(self, "post_stages", new_post_stages)
