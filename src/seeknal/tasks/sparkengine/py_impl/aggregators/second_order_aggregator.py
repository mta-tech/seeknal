"""Second-order aggregator for time-windowed aggregations.

This matches the API of the DuckDB SecondOrderAggregator for consistency.
"""

import math
from collections import namedtuple
from enum import Enum
from typing import Callable, Dict, List, Optional, Union

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.types import FloatType, StringType


# Native Spark aggregations
all_native_aggregations = {
    "count": "count",
    "mean": "mean",
    "min": "min",
    "max": "max",
    "std": "stddev",
    "sum": "sum",
    "first": "first",
    "last": "last",
}

# UDF aggregations
all_udf_aggregations = {
    "entropy": "entropy_cal_udf",
    "first_value": "first_val_udf",
    "last_value": "last_val_udf",
    "most_frequent": "most_frequent_udf",
}


# UDF functions
def entropy_cal(x: List) -> float:
    """Calculate entropy of a list of values."""
    count = {}
    total = len(x)
    for i in x:
        count[i] = count.get(i, 0) + 1
    for i in count.keys():
        count[i] = -(count[i] / total) * math.log(count[i] / total, 2)
    return sum(count.values())


def first_val(x: List[str]) -> Optional[str]:
    """Get first non-null value from timestamp-prefixed list."""
    for i in x:
        if i.find("_|_") > 0:
            return i[i.find("_|_") + 3:]
    return None


def last_val(x: List[str]) -> Optional[str]:
    """Get last non-null value from timestamp-prefixed list."""
    for i in reversed(x):
        if i.find("_|_") > 0:
            return i[i.find("_|_") + 3:]
    return None


def most_frequent(x: List) -> Optional[str]:
    """Get most frequent value in list."""
    count = {}
    for i in x:
        count[i] = count.get(i, 0) + 1
    if len(count) > 0:
        return max(count, key=count.get)
    return None


# Register UDFs
entropy_cal_udf = F.udf(entropy_cal, FloatType())
first_val_udf = F.udf(first_val, StringType())
last_val_udf = F.udf(last_val, StringType())
most_frequent_udf = F.udf(most_frequent, StringType())


# Type definitions - use namedtuple with defaults like DuckDB version
def _aggregation_spec(
    name,
    features,
    aggregations,
    filterCondition="",
    dayLimitLower1="",
    dayLimitUpper1="",
    dayLimitLower2="",
    dayLimitUpper2="",
):
    """Create AggregationSpec with default values."""
    return namedtuple(
        "AggregationSpec",
        [
            "name",
            "features",
            "aggregations",
            "filterCondition",
            "dayLimitLower1",
            "dayLimitUpper1",
            "dayLimitLower2",
            "dayLimitUpper2",
        ],
    )(
        name,
        features,
        aggregations,
        filterCondition,
        dayLimitLower1,
        dayLimitUpper1,
        dayLimitLower2,
        dayLimitUpper2,
    )


# Create AggregationSpec class with factory function
class AggregationSpec:
    """Specification for an aggregation rule with defaults."""

    def __new__(
        cls,
        name,
        features,
        aggregations,
        filterCondition="",
        dayLimitLower1="",
        dayLimitUpper1="",
        dayLimitLower2="",
        dayLimitUpper2="",
    ):
        return _aggregation_spec(
            name,
            features,
            aggregations,
            filterCondition,
            dayLimitLower1,
            dayLimitUpper1,
            dayLimitLower2,
            dayLimitUpper2,
        )


class SecondOrderAggregator:
    """Perform second-order aggregations with time windows using PySpark.

    This class enables feature engineering by creating second-order features from
    transaction-level or event-level data. It supports various aggregation strategies
    that can be combined to generate a rich feature set.

    Supported Aggregation Types:
        1. **basic**: Simple aggregation over the entire history per ID.
           - Format: `AggregationSpec("basic", "feature_col", "agg_func")`
           - Example: `AggregationSpec("basic", "amount", "sum")` -> Sum of amount for each user.

        2. **basic_days**: Aggregation over a specific time window defined by `days_between`.
           - Format: `AggregationSpec("basic_days", "feature_col", "agg_func", "", lower, upper)`
           - Example: `AggregationSpec("basic_days", "amount", "mean", "", 1, 30)` -> Mean amount in the last 1-30 days.

        3. **ratio**: Ratio of two aggregations over different time windows.
           - Format: `AggregationSpec("ratio", "feature_col", "agg_func", "", lower1, upper1, lower2, upper2)`
           - Example: `AggregationSpec("ratio", "amount", "sum", "", 1, 30, 31, 60)` -> (Sum 1-30 days) / (Sum 31-60 days).

        4. **since**: Aggregation filtered by a custom condition.
           - Format: `AggregationSpec("since", "feature_col", "agg_func", "condition")`
           - Example: `AggregationSpec("since", "flag", "count", "flag == 1")` -> Count of flag=1 events.

    Args:
        spark: SparkSession for executing queries
        idCol: Column name representing the entity ID (e.g., 'user_id', 'msisdn')
        featureDateCol: Column name representing the date of the event/transaction
        featureDateFormat: Format of the feature date column (default: "yyyy-MM-dd")
        applicationDateCol: Column name representing the reference date (e.g., application date)
        applicationDateFormat: Format of the application date column (default: "yyyy-MM-dd")

    The `transform` method applies the aggregation rules to a table and returns a DataFrame.
    """

    def __init__(
        self,
        spark: SparkSession,
        idCol: str,
        featureDateCol: str = "day",
        featureDateFormat: str = "yyyy-MM-dd",
        applicationDateCol: str = "application_date",
        applicationDateFormat: str = "yyyy-MM-dd",
    ):
        self.spark = spark
        self.idCol = idCol
        self.featureDateCol = featureDateCol
        self.featureDateFormat = featureDateFormat
        self.applicationDateCol = applicationDateCol
        self.applicationDateFormat = applicationDateFormat
        self.rules = []

    def setRules(self, rules: List[AggregationSpec]) -> "SecondOrderAggregator":
        """Set aggregation rules.

        Args:
            rules: List of AggregationSpec objects

        Returns:
            self for method chaining
        """
        self.rules = rules
        return self

    def transform(self, table_name: str) -> DataFrame:
        """Transform the input table using the defined aggregation rules.

        Args:
            table_name: Name of the table or view to aggregate

        Returns:
            DataFrame with aggregated features
        """
        if not self.rules:
            raise ValueError("No rules defined for aggregation.")

        # Load the table and add days_between column
        df = self.spark.table(table_name)

        # Add days_between column
        df = df.withColumn(
            "_days_between",
            F.datediff(
                F.to_date(F.col(self.featureDateCol), self.featureDateFormat),
                F.to_date(F.col(self.applicationDateCol), self.applicationDateFormat),
            ),
        )

        # Build aggregations
        all_aggs = []
        group_cols = [self.idCol]

        for rule in self.rules:
            features = [f.strip() for f in rule.features.split(",")]
            aggs = [a.strip() for a in rule.aggregations.split(",")]
            conds = [c.strip() for c in rule.filterCondition.split(",")] if rule.filterCondition else [None] * len(features)

            if rule.name == "basic":
                all_aggs.extend(self._basic_aggregations(features, aggs))
            elif rule.name == "basic_days":
                all_aggs.extend(
                    self._basic_days_aggregations(
                        features, aggs, int(rule.dayLimitLower1), int(rule.dayLimitUpper1), conds
                    )
                )
            elif rule.name == "since":
                all_aggs.extend(self._since_aggregations(features, aggs, conds))
            elif rule.name == "ratio":
                all_aggs.extend(
                    self._ratio_aggregations(
                        features,
                        aggs,
                        int(rule.dayLimitLower1),
                        int(rule.dayLimitUpper1),
                        int(rule.dayLimitLower2),
                        int(rule.dayLimitUpper2),
                        conds,
                    )
                )

        # Perform aggregation
        result = df.groupBy(*group_cols).agg(*all_aggs)

        # Flatten column names (Spark agg creates nested column names)
        for col in result.columns:
            if col != self.idCol:
                # Clean up column name from aggregation nesting
                new_name = col.replace("(", "_").replace(")", "").replace(",", "")
                result = result.withColumnRenamed(col, new_name)

        return result

    def _basic_aggregations(self, features: List[str], aggs: List[str]) -> List[Column]:
        """Generate basic aggregations (no time window)."""
        result = []
        methods = self._get_methods()
        for feat in features:
            for agg in aggs:
                if agg in methods:
                    result.append(methods[agg](F.col(feat)).alias(f"{feat}_{agg.upper()}"))
        return result

    def _basic_days_aggregations(
        self,
        features: List[str],
        aggs: List[str],
        day_lower: int,
        day_upper: int,
        conds: List[Optional[str]],
    ) -> List[Column]:
        """Generate time-windowed aggregations."""
        result = []
        methods = self._get_methods()
        udfs = self._get_udfs()

        for feat, cond in zip(features, conds):
            for agg in aggs:
                # Build filter condition
                if cond:
                    filter_expr = (
                        (F.col("_days_between") >= day_lower)
                        & (F.col("_days_between") <= day_upper)
                        & F.expr(cond)
                    )
                else:
                    filter_expr = (
                        (F.col("_days_between") >= day_lower)
                        & (F.col("_days_between") <= day_upper)
                    )

                # Get filtered column
                filtered_col = F.when(filter_expr & F.col(feat).isNotNull(), F.col(feat))

                # Apply aggregation
                if agg in methods:
                    result.append(
                        methods[agg](filtered_col).alias(f"{feat}_{agg.upper()}_{day_lower}_{day_upper}")
                    )
                elif agg in udfs:
                    result.append(
                        udfs[agg](F.collect_list(filtered_col)).alias(f"{feat}_{agg.upper()}_{day_lower}_{day_upper}")
                    )

        return result

    def _since_aggregations(self, features: List[str], aggs: List[str], conds: List[Optional[str]]) -> List[Column]:
        """Generate days-since aggregations."""
        result = []
        methods = self._get_methods()

        for feat, cond in zip(features, conds):
            for agg in aggs:
                # Build filter condition
                if cond:
                    filter_expr = F.col(feat).isNotNull() & F.expr(cond)
                else:
                    filter_expr = F.col(feat).isNotNull()

                # For "since", get the min days_between value for filtered records
                # This gives the earliest days_since that meets the condition
                if agg == "count":
                    # Count returns count of matching records
                    filtered_col = F.when(filter_expr, F.lit(1))
                    result.append(F.sum(filtered_col).alias(f"SINCE_{agg.upper()}_{feat}"))
                elif agg in ["sum", "mean", "min", "max"]:
                    filtered_col = F.when(filter_expr, F.col("_days_between"))
                    result.append(methods[agg](filtered_col).alias(f"SINCE_{agg.upper()}_{feat}"))
                else:
                    # For other aggregations, use the aggregated value
                    filtered_col = F.when(filter_expr, F.col(feat))
                    result.append(methods[agg](filtered_col).alias(f"SINCE_{agg.upper()}_{feat}"))

        return result

    def _ratio_aggregations(
        self,
        features: List[str],
        aggs: List[str],
        day_lower1: int,
        day_upper1: int,
        day_lower2: int,
        day_upper2: int,
        conds: List[Optional[str]],
    ) -> List[Column]:
        """Generate ratio aggregations (window1 / window2)."""
        result = []
        methods = self._get_methods()

        for feat, cond in zip(features, conds):
            for agg in aggs:
                if agg not in methods:
                    continue

                # Numerator (window 1)
                if cond:
                    filter_expr1 = (
                        (F.col("_days_between") >= day_lower1)
                        & (F.col("_days_between") <= day_upper1)
                        & F.col(feat).isNotNull()
                        & F.expr(cond)
                    )
                else:
                    filter_expr1 = (
                        (F.col("_days_between") >= day_lower1)
                        & (F.col("_days_between") <= day_upper1)
                        & F.col(feat).isNotNull()
                    )

                numerator = F.when(filter_expr1, F.col(feat))

                # Denominator (window 2)
                if cond:
                    filter_expr2 = (
                        (F.col("_days_between") >= day_lower2)
                        & (F.col("_days_between") <= day_upper2)
                        & F.col(feat).isNotNull()
                        & F.expr(cond)
                    )
                else:
                    filter_expr2 = (
                        (F.col("_days_between") >= day_lower2)
                        & (F.col("_days_between") <= day_upper2)
                        & F.col(feat).isNotNull()
                    )

                denominator = F.when(filter_expr2, F.col(feat))

                # Ratio
                result.append(
                    (methods[agg](numerator) / methods[agg](denominator)).alias(
                        f"{feat}_{agg.upper()}{day_lower1}{day_upper1}_{agg.upper()}{day_lower2}{day_upper2}"
                    )
                )

        return result

    def validate(self, table_name: str) -> List[str]:
        """Validate that the input table has the required columns.

        Args:
            table_name: Name of the table to validate

        Returns:
            List of error messages (empty if validation passes)
        """
        errors = []
        try:
            df = self.spark.table(table_name)
            columns = set(df.columns)
        except Exception as e:
            return [f"Could not access table '{table_name}': {str(e)}"]

        # Check required columns
        if self.idCol not in columns:
            errors.append(f"Missing ID column: '{self.idCol}'")

        if self.featureDateCol not in columns:
            errors.append(f"Missing feature date column: '{self.featureDateCol}'")

        if self.applicationDateCol not in columns:
            errors.append(f"Missing application date column: '{self.applicationDateCol}'")

        # Check feature columns from rules
        used_features = set()
        for rule in self.rules:
            for f in rule.features.split(","):
                used_features.add(f.strip())

        missing = used_features - columns
        if missing:
            errors.append(f"Missing feature columns: {missing}")

        return errors

    def builder(self) -> "FeatureBuilder":
        """Returns a FeatureBuilder instance for fluent API.

        Returns:
            FeatureBuilder for chaining aggregation rules
        """
        return FeatureBuilder(self)

    def _get_methods(self) -> Dict[str, Callable]:
        """Get native Spark aggregation methods."""
        return {
            "count": F.count,
            "mean": F.mean,
            "min": F.min,
            "max": F.max,
            "std": F.stddev,
            "sum": F.sum,
            "first": F.first,
            "last": F.last,
        }

    def _get_udfs(self) -> Dict[str, Callable]:
        """Get UDF aggregation methods."""
        return {
            "entropy": entropy_cal_udf,
            "first_value": first_val_udf,
            "last_value": last_val_udf,
            "most_frequent": most_frequent_udf,
        }

    def _map_agg_func(self, agg: str) -> str:
        """Map aggregation function name to Spark equivalent."""
        mapping = {
            "count": "count",
            "sum": "sum",
            "mean": "avg",
            "avg": "avg",
            "min": "min",
            "max": "max",
            "std": "stddev",
            "stddev": "stddev",
        }
        return mapping.get(agg.lower(), agg)


class FeatureBuilder:
    """Fluent builder for SecondOrderAggregator rules.

    Provides a fluent API for defining aggregation rules:

    ```python
    aggregator = SecondOrderAggregator(spark, "user_id", "day", "application_date")
    aggregator.builder() \\
        .feature("amount") \\
        .basic(["sum", "mean"]) \\
        .rolling([(1, 30), (31, 60)], ["sum"]) \\
        .ratio((1, 30), (31, 60), ["sum"]) \\
        .since("flag == 1", ["count"]) \\
        .build()
    ```
    """

    def __init__(self, aggregator: SecondOrderAggregator):
        self.aggregator = aggregator
        self.current_feature = None

    def feature(self, name: str) -> "FeatureBuilder":
        """Set the current feature for aggregation rules.

        Args:
            name: Feature column name

        Returns:
            self for method chaining
        """
        self.current_feature = name
        return self

    def basic(self, aggs: List[str]) -> "FeatureBuilder":
        """Add basic aggregations for the current feature.

        Args:
            aggs: List of aggregation function names (e.g., ["sum", "mean"])

        Returns:
            self for method chaining
        """
        if not self.current_feature:
            raise ValueError("Call .feature() before defining aggregations.")
        for agg in aggs:
            self.aggregator.rules.append(AggregationSpec("basic", self.current_feature, agg))
        return self

    def rolling(self, days: List[tuple], aggs: List[str]) -> "FeatureBuilder":
        """Add rolling window aggregations for the current feature.

        Args:
            days: List of (lower, upper) day window tuples
            aggs: List of aggregation function names

        Returns:
            self for method chaining

        Example:
            .rolling([(1, 30), (31, 60)], ["sum"])  # 1-30 days and 31-60 days
        """
        if not self.current_feature:
            raise ValueError("Call .feature() before defining aggregations.")
        for lower, upper in days:
            for agg in aggs:
                self.aggregator.rules.append(
                    AggregationSpec("basic_days", self.current_feature, agg, "", str(lower), str(upper))
                )
        return self

    def ratio(self, numerator: tuple, denominator: tuple, aggs: List[str]) -> "FeatureBuilder":
        """Add ratio aggregations for the current feature.

        Args:
            numerator: (lower, upper) day window for numerator
            denominator: (lower, upper) day window for denominator
            aggs: List of aggregation function names

        Returns:
            self for method chaining

        Example:
            .ratio((1, 30), (31, 60), ["sum"])  # sum(1-30) / sum(31-60)
        """
        if not self.current_feature:
            raise ValueError("Call .feature() before defining aggregations.")
        l1, u1 = numerator
        l2, u2 = denominator
        for agg in aggs:
            self.aggregator.rules.append(
                AggregationSpec("ratio", self.current_feature, agg, "", str(l1), str(u1), str(l2), str(u2))
            )
        return self

    def since(self, condition: str, aggs: List[str]) -> "FeatureBuilder":
        """Add conditional aggregations for the current feature.

        Args:
            condition: SQL expression for filtering (e.g., "flag == 1")
            aggs: List of aggregation function names

        Returns:
            self for method chaining

        Example:
            .since("flag == 1", ["count"])  # count where flag == 1
        """
        if not self.current_feature:
            raise ValueError("Call .feature() before defining aggregations.")
        for agg in aggs:
            self.aggregator.rules.append(
                AggregationSpec("since", self.current_feature, agg, condition)
            )
        return self

    def build(self) -> SecondOrderAggregator:
        """Return the aggregator with all rules applied.

        Returns:
            The SecondOrderAggregator instance
        """
        return self.aggregator
