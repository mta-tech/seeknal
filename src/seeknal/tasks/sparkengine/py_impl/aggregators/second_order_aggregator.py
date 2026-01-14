"""Second-order aggregator for time-windowed aggregations."""

import math
from collections import namedtuple
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Dict, List, Optional, Union

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.types import FloatType, StringType

from ..base import BaseAggregatorPySpark


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


# Type definitions
@dataclass
class AggregationSpec:
    """Specification for an aggregation rule."""

    name: str
    features: str
    aggregations: str
    filterCondition: str = ""
    dayLimitLower1: str = ""
    dayLimitUpper1: str = ""
    dayLimitLower2: str = ""
    dayLimitUpper2: str = ""


class SecondOrderAggregator(BaseAggregatorPySpark):
    """Perform second-order aggregations with time windows.

    Supports multiple aggregation types:
    - basic: Simple aggregations per ID
    - basic_days: Aggregations with day windows
    - since: Days-since aggregations
    - ratio: Ratio of two time-window aggregations

    Args:
        spark: SparkSession
        rules: List of aggregation specifications
        idCol: ID column name
        applicationDateCol: Application date column name
        featureDateCol: Feature date column name
        applicationDateFormat: Application date format (default: "yyyy-MM-dd")
        featureDateFormat: Feature date format (default: "yyyy-MM-dd")
    """

    def __init__(
        self,
        spark: SparkSession,
        rules: List[AggregationSpec],
        idCol: str,
        applicationDateCol: str,
        featureDateCol: str = "day",
        applicationDateFormat: str = "yyyy-MM-dd",
        featureDateFormat: str = "yyyy-MM-dd",
        **kwargs
    ):
        super().__init__(**kwargs, spark=spark)
        self.spark = spark
        self.rules = rules
        self.idCol = idCol
        self.applicationDateCol = applicationDateCol
        self.featureDateCol = featureDateCol
        self.applicationDateFormat = applicationDateFormat
        self.featureDateFormat = featureDateFormat

    def transform(self, df: DataFrame) -> DataFrame:
        """Apply second-order aggregations.

        Args:
            df: Input DataFrame

        Returns:
            Aggregated DataFrame
        """
        # Add days_between column
        df = df.withColumn(
            "days_between",
            F.datediff(
                F.to_date(F.col(self.applicationDateCol), self.applicationDateFormat),
                F.to_date(F.col(self.featureDateCol), self.featureDateFormat),
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
                all_aggs.extend(self._basic_aggregations(df, features, aggs))
            elif rule.name == "basic_days":
                all_aggs.extend(
                    self._basic_days_aggregations(
                        df, features, aggs, int(rule.dayLimitLower1), int(rule.dayLimitUpper1), conds
                    )
                )
            elif rule.name == "since":
                all_aggs.extend(self._since_aggregations(df, features, aggs, conds))
            elif rule.name == "ratio":
                all_aggs.extend(
                    self._ratio_aggregations(
                        df,
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

        # Flatten column names
        for col in result.columns:
            if col != self.idCol:
                # Extract aggregation name from nested structure
                result = result.withColumnRenamed(col, col.replace("(", "_").replace(")", "").replace(",", ""))

        return result

    def _basic_aggregations(self, df: DataFrame, features: List[str], aggs: List[str]) -> List[Column]:
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
        df: DataFrame,
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
                    filter_expr = (F.col("days_between") >= day_lower) & (F.col("days_between") <= day_upper) & F.expr(cond)
                else:
                    filter_expr = (F.col("days_between") >= day_lower) & (F.col("days_between") <= day_upper)

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

    def _since_aggregations(self, df: DataFrame, features: List[str], aggs: List[str], conds: List[Optional[str]]) -> List[Column]:
        """Generate days-since aggregations."""
        result = []
        methods = self._get_methods()

        for feat, cond in zip(features, conds):
            for agg in aggs:
                if cond:
                    filter_expr = F.col(feat).isNotNull() & F.expr(cond)
                else:
                    filter_expr = F.col(feat).isNotNull()

                # For "since", return the days_between value
                result.append(
                    F.when(filter_expr, F.col("days_between"))
                    .alias(f"{feat}_SINCE")
                )

        return result

    def _ratio_aggregations(
        self,
        df: DataFrame,
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
                        (F.col("days_between") >= day_lower1)
                        & (F.col("days_between") <= day_upper1)
                        & F.col(feat).isNotNull()
                        & F.expr(cond)
                    )
                else:
                    filter_expr1 = (
                        (F.col("days_between") >= day_lower1)
                        & (F.col("days_between") <= day_upper1)
                        & F.col(feat).isNotNull()
                    )

                numerator = F.when(filter_expr1, F.col(feat))

                # Denominator (window 2)
                if cond:
                    filter_expr2 = (
                        (F.col("days_between") >= day_lower2)
                        & (F.col("days_between") <= day_upper2)
                        & F.col(feat).isNotNull()
                        & F.expr(cond)
                    )
                else:
                    filter_expr2 = (
                        (F.col("days_between") >= day_lower2)
                        & (F.col("days_between") <= day_upper2)
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
