"""Function-based aggregators."""

from dataclasses import dataclass
from pyspark.sql import DataFrame, functions as F
from ..base import BaseAggregatorPySpark


@dataclass
class AggregationFunction:
    """Aggregation function specification.

    Args:
        column: Column to aggregate (use "*" for count)
        function: Function name (sum, count, avg, min, max)
        alias: Output column name
    """

    column: str
    function: str
    alias: str


class FunctionAggregator(BaseAggregatorPySpark):
    """Aggregate using built-in functions.

    Args:
        group_by_columns: Columns to group by
        aggregations: List of AggregationFunction specs
    """

    def __init__(self, group_by_columns: list, aggregations: list, **kwargs):
        super().__init__(**kwargs, group_by_columns=group_by_columns,
                         aggregations=aggregations)
        self.group_by_columns = group_by_columns
        self.aggregations = aggregations

    def aggregate(self, df: DataFrame) -> DataFrame:
        """Perform aggregation.

        Args:
            df: Input DataFrame

        Returns:
            Aggregated DataFrame
        """
        # Map function names to PySpark functions
        func_map = {
            "sum": F.sum,
            "count": F.count,
            "avg": F.avg,
            "min": F.min,
            "max": F.max,
        }

        # Build aggregations
        agg_exprs = []
        for agg in self.aggregations:
            col = F.lit(1) if agg.column == "*" else F.col(agg.column)
            func = func_map[agg.function]
            agg_exprs.append(func(col).alias(agg.alias))

        # Group and aggregate
        if self.group_by_columns:
            return df.groupBy(*self.group_by_columns).agg(*agg_exprs)
        else:
            return df.agg(*agg_exprs)
