"""Advanced PySpark transformers for point-in-time joins and multi-table joins."""

from dataclasses import dataclass
from enum import Enum
from typing import List, Optional, Union
import uuid

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..base import BaseTransformerPySpark


class JoinType(str, Enum):
    """Join types for SQL joins."""

    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    FULL = "full"
    LEFT_OUTER = "left_outer"
    RIGHT_OUTER = "right_outer"
    LEFT_SEMI = "left_semi"
    LEFT_ANTI = "left_anti"
    CROSS = "cross"


@dataclass
class TableJoinDef:
    """Definition for joining a table in a multi-table join.

    Attributes:
        table: Table name or DataFrame to join
        alias: Alias for the table in the join
        joinType: Type of join (inner, left, etc.)
        joinExpression: SQL expression for the join condition
    """

    table: Union[str, DataFrame]
    alias: str
    joinType: Union[str, JoinType]
    joinExpression: str


class JoinTablesByExpr(BaseTransformerPySpark):
    """Join multiple tables by expressions.

    Performs SQL joins with multiple tables using custom join expressions.

    Args:
        spark: SparkSession
        tables: List of TableJoinDef objects defining tables to join
        select_stm: SELECT clause (default: "*")
        alias: Alias for main table (default: "a")
    """

    def __init__(
        self,
        spark: SparkSession,
        tables: List[TableJoinDef],
        select_stm: str = "*",
        alias: str = "a",
        **kwargs
    ):
        super().__init__(**kwargs, spark=spark, tables=tables, select_stm=select_stm, alias=alias)
        self.spark = spark
        self.tables = tables
        self.select_stm = select_stm
        self.alias = alias

    def transform(self, df: DataFrame) -> DataFrame:
        """Join tables by expressions.

        Args:
            df: Input DataFrame

        Returns:
            Result DataFrame with joins applied
        """
        # Register DataFrames as temp views if needed
        tables = []
        for table_def in self.tables:
            if isinstance(table_def.table, DataFrame):
                random_name = "f" + str(uuid.uuid4())[:8]
                table_def.table.createOrReplaceTempView(f"table_{random_name}")
                tables.append(
                    TableJoinDef(
                        table=f"table_{random_name}",
                        alias=table_def.alias,
                        joinType=table_def.joinType,
                        joinExpression=table_def.joinExpression,
                    )
                )
            else:
                tables.append(table_def)

        # Build JOIN clause
        join_stm = ""
        for table_def in tables:
            join_type = table_def.joinType.value if isinstance(table_def.joinType, JoinType) else table_def.joinType
            join_stm += f"{join_type} JOIN {table_def.table} {table_def.alias} ON {table_def.joinExpression} "

        # Build query
        query = f"SELECT {self.select_stm} FROM __THIS__ {self.alias} {join_stm}"

        # Register input as temp view and execute
        df.createOrReplaceTempView("__THIS__")
        return self.spark.sql(query)


class Time(str, Enum):
    """Time direction for point-in-time joins."""

    NOW = "now"
    FUTURE = "future"
    PAST = "past"


class PointInTime(BaseTransformerPySpark):
    """Perform time-aware point-in-time join.

    Joins features with respect to application dates defined in a spine table,
    performing temporal filtering based on time differences.

    Args:
        spark: SparkSession
        spine: Spine table name or DataFrame (application dates)
        app_date: Application date column name in spine
        feature_date: Feature date column name in features (default: "event_time")
        app_date_format: Format of application dates (default: "yyyy-MM-dd")
        feature_date_format: Format of feature dates (default: "yyyy-MM-dd")
        how: Time direction - PAST, NOW, or FUTURE (default: PAST)
        offset: Day offset for time window (default: 0)
        length: Length of time window (optional)
        join_type: SQL join type (default: "INNER JOIN")
        col_id: ID column in features (default: "id")
        spine_col_id: ID column in spine (default: "id")
        broadcast: Use broadcast hint (default: False)
        keep_cols: Additional columns to select (optional)
    """

    def __init__(
        self,
        spark: SparkSession,
        spine: Union[str, DataFrame],
        app_date: str,
        feature_date: str = "event_time",
        app_date_format: str = "yyyy-MM-dd",
        feature_date_format: str = "yyyy-MM-dd",
        how: Time = Time.PAST,
        offset: int = 0,
        length: Optional[int] = None,
        join_type: str = "INNER JOIN",
        col_id: str = "id",
        spine_col_id: str = "id",
        broadcast: bool = False,
        keep_cols: Optional[List[str]] = None,
        **kwargs
    ):
        super().__init__(
            **kwargs,
            spark=spark,
            spine=spine,
            app_date=app_date,
            feature_date=feature_date,
            app_date_format=app_date_format,
            feature_date_format=feature_date_format,
            how=how,
            offset=offset,
            length=length,
            join_type=join_type,
            col_id=col_id,
            spine_col_id=spine_col_id,
            broadcast=broadcast,
            keep_cols=keep_cols,
        )
        self.spark = spark
        self.spine = spine
        self.app_date = app_date
        self.feature_date = feature_date
        self.app_date_format = app_date_format
        self.feature_date_format = feature_date_format
        self.how = how
        self.offset = offset
        self.length = length
        self.join_type = join_type
        self.col_id = col_id
        self.spine_col_id = spine_col_id
        self.broadcast = broadcast
        self.keep_cols = keep_cols or []

    def transform(self, df: DataFrame) -> DataFrame:
        """Perform point-in-time join.

        Args:
            df: Input DataFrame (features)

        Returns:
            Result DataFrame with point-in-time join applied
        """
        # Register spine as temp view if DataFrame
        if isinstance(self.spine, DataFrame):
            random_name = "f" + str(uuid.uuid4())[:8]
            self.spine.createOrReplaceTempView(f"table_{random_name}")
            spine_name = f"table_{random_name}"
        else:
            spine_name = self.spine

        # Build SELECT clause
        selector = ["a.*"] + self.keep_cols
        where_spec = "(1=1)"

        # Build broadcast hint
        broadcast_hint = " /*+ BROADCAST(b) */ " if self.broadcast else ""

        # Build application date specification
        app_date_spec = f"""
            date_format(
                date_sub(
                    from_unixtime(unix_timestamp(b.{self.app_date}, '{self.app_date_format}')),
                    {self.offset}
                ),
                '{self.feature_date_format}'
            )
        """

        # Determine comparator and where clause based on time direction
        if self.how == Time.NOW:
            comparator = "="
        elif self.how == Time.PAST:
            comparator = "<="
            if self.length:
                limit_expr = f"""
                    date_format(
                        date_sub(
                            from_unixtime(unix_timestamp(b.{self.app_date}, '{self.app_date_format}')),
                            {self.offset + self.length - 1}
                        ),
                        '{self.feature_date_format}'
                    )
                """
                where_spec = f"{where_spec} AND (a.{self.feature_date} >= {limit_expr})"
        elif self.how == Time.FUTURE:
            comparator = ">="
            if self.length:
                limit_expr = f"""
                    date_format(
                        date_sub(
                            from_unixtime(unix_timestamp(b.{self.app_date}, '{self.app_date_format}')),
                            {self.offset - (self.length - 1)}
                        ),
                        '{self.feature_date_format}'
                    )
                """
                where_spec = f"{where_spec} AND (a.{self.feature_date} <= {limit_expr})"

        # Build query
        query = f"""
            SELECT {broadcast_hint} {', '.join(selector)}
            FROM __THIS__ a
            {self.join_type}
            {spine_name} b
            ON a.{self.col_id} = b.{self.spine_col_id}
            AND a.{self.feature_date} {comparator} {app_date_spec}
            WHERE {where_spec}
        """

        # Register input as temp view and execute
        df.createOrReplaceTempView("__THIS__")
        return self.spark.sql(query)
