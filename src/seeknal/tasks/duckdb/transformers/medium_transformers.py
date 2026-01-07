"""
Medium complexity DuckDB transformers.

These transformers handle join operations, time-aware joins, and type casting.
"""

from typing import List, Optional, Union
from enum import Enum
from dataclasses import dataclass
import uuid

from seeknal.validation import validate_column_name

from .base_transformer import DuckDBTransformer, DuckDBClassName


class JoinType(str, Enum):
    """SQL join types."""

    INNER = "inner"
    LEFT = "left"
    LEFT_OUTER = "left_outer"
    RIGHT = "right"
    RIGHT_OUTER = "right_outer"
    FULL = "full"
    FULL_OUTER = "full_outer"
    LEFT_SEMI = "left_semi"
    LEFT_ANTI = "left_anti"
    CROSS = "cross"


@dataclass
class TableJoinDef:
    """Definition for a table join operation."""

    table: Union[str, str]  # Changed from pa.Table to str to avoid Pydantic issues
    alias: str
    joinType: Union[str, JoinType]
    joinExpression: str


class JoinTablesByExpr(DuckDBTransformer):
    """Join multiple tables using SQL expressions.

    Supports multiple join types including INNER, LEFT, RIGHT, FULL, and CROSS joins.
    """

    select_stm: str = "*"
    alias: str = "a"
    tables: List[TableJoinDef] = None
    statement: str = ""
    kind: DuckDBClassName = DuckDBClassName.JOIN_BY_EXPR
    class_name: str = "seeknal.tasks.duckdb.transformers.JoinTablesByExpr"
    description: Optional[str] = None
    params: dict = {}

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if self.tables is None:
            raise ValueError("tables must be provided")

        statement = self._construct_stm(self.select_stm, self.alias, self.tables)
        self.statement = statement
        self.params = {
            "select_stm": self.select_stm,
            "alias": self.alias,
            "tables": [t.__dict__ if hasattr(t, "__dict__") else t for t in self.tables],
        }

    @staticmethod
    def _construct_stm(select_stm: str, alias: str, tables: List[TableJoinDef]) -> str:
        """Construct SQL statement for joining tables."""
        join_stm = ""

        for table_def in tables:
            if isinstance(table_def.joinType, JoinType):
                join_type = table_def.joinType.value
            else:
                join_type = table_def.joinType

            join_type_map = {
                "inner": "INNER JOIN",
                "left": "LEFT JOIN",
                "left_outer": "LEFT JOIN",
                "right": "RIGHT JOIN",
                "right_outer": "RIGHT JOIN",
                "full": "FULL OUTER JOIN",
                "full_outer": "FULL OUTER JOIN",
                "left_semi": "WHERE EXISTS",
                "left_anti": "WHERE NOT EXISTS",
                "cross": "CROSS JOIN",
            }

            sql_join = join_type_map.get(join_type, f"{join_type} JOIN")

            if table_def.joinType in [JoinType.LEFT_SEMI, JoinType.LEFT_ANTI]:
                continue
            else:
                join_stm += (
                    f"{sql_join} {table_def.table} {table_def.alias} "
                    f"ON {table_def.joinExpression} "
                )

        query = f"SELECT {select_stm} FROM __THIS__ {alias} {join_stm}"
        return query

    def to_sql(self) -> str:
        """Return the generated SQL statement."""
        return self.statement


class Time(str, Enum):
    """Time direction for point-in-time joins."""

    NOW = "now"
    FUTURE = "future"
    PAST = "past"


class PointInTime(DuckDBTransformer):
    """Perform time-aware join of features with respect to application dates."""

    how: Time = Time.PAST
    offset: int = 0
    length: Optional[int] = None
    feature_date: str = "event_time"
    app_date: str
    feature_date_format: str = "yyyy-MM-dd"
    app_date_format: str = "yyyy-MM-dd"
    broadcast: bool = False
    spine: Union[str, str]  # Changed from pa.Table to str to avoid Pydantic issues
    join_type: str = "INNER JOIN"
    col_id: str = "id"
    spine_col_id: str = "id"
    keep_cols: Optional[List[str]] = None
    statement: str = ""
    kind: DuckDBClassName = DuckDBClassName.POINT_IN_TIME
    class_name: str = "seeknal.tasks.duckdb.transformers.PointInTime"
    description: Optional[str] = None
    params: dict = {}

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        import pyarrow as pa

        spine_name = self.spine
        if isinstance(self.spine, pa.Table):
            random_name = f"spine_{uuid.uuid4().hex[:8]}"
            object.__setattr__(self, "spine", spine_name)
            object.__setattr__(self, "_spine_table", self.spine)

        self.statement = self._construct_statement(self.__dict__)

    @staticmethod
    def _construct_statement(values: dict) -> str:
        """Construct SQL statement for point-in-time join."""
        selector = ["a.*"]
        if values.get("keep_cols"):
            selector.extend(values["keep_cols"])

        spine_name = values["spine"]
        if isinstance(spine_name, str):
            pass
        else:
            spine_name = values.get("_spine_table", spine_name)

        feature_date_col = f'a.{values["feature_date"]}'
        app_date_col = f'b.{values["app_date"]}'

        if values["how"] == Time.NOW:
            comparator = "="
            date_filter = f'{feature_date_col} = CAST({app_date_col} AS DATE)'
        elif values["how"] == Time.PAST:
            comparator = "<="
            date_filter = f'{feature_date_col} <= CAST({app_date_col} AS DATE)'
            
            if values.get("length"):
                date_filter += f"""
                    AND {feature_date_col} >= DATE_SUB(
                        CAST({app_date_col} AS DATE),
                        INTERVAL '{values["offset"] + values["length"] - 1} days'
                    )
                """
        elif values["how"] == Time.FUTURE:
            comparator = ">="
            date_filter = f'{feature_date_col} >= CAST({app_date_col} AS DATE)'
            
            if values.get("length"):
                date_filter += f"""
                    AND {feature_date_col} <= DATE_ADD(
                        CAST({app_date_col} AS DATE),
                        INTERVAL '{values["offset"] + values["length"] - 1} days'
                    )
                """
        else:
            raise ValueError(f"Invalid time direction: {values['how']}")

        if values.get("offset", 0) > 0 and values["how"] != Time.NOW:
            if values["how"] == Time.PAST:
                date_filter += f"""
                    AND {feature_date_col} <= DATE_SUB(
                        CAST({app_date_col} AS DATE),
                        INTERVAL '{values["offset"]} days'
                    )
                """
            elif values["how"] == Time.FUTURE:
                date_filter += f"""
                    AND {feature_date_col} >= DATE_ADD(
                        CAST({app_date_col} AS DATE),
                        INTERVAL '{values["offset"]} days'
                    )
                """

        query = f"""
            SELECT {', '.join(selector)}
            FROM __THIS__ a
            {values["join_type"]}
            {spine_name} b
            ON a.{values["col_id"]} = b.{values["spine_col_id"]}
            AND {date_filter}
            WHERE 1=1
        """

        return query.strip()

    def to_sql(self) -> str:
        """Return the generated SQL statement."""
        return self.statement


class CastColumn(DuckDBTransformer):
    """Cast a column to a different data type."""

    inputCol: str
    outputCol: str
    dataType: str
    kind: DuckDBClassName = DuckDBClassName.CAST_COLUMN
    class_name: str = "seeknal.tasks.duckdb.transformers.CastColumn"
    description: Optional[str] = None
    params: dict = {}

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        validate_column_name(self.inputCol)
        validate_column_name(self.outputCol)
        
        self.params = {
            "inputCol": self.inputCol,
            "outputCol": self.outputCol,
            "dataType": self.dataType,
        }

    def to_sql(self) -> str:
        """Generate SQL for type casting."""
        return f'SELECT *, CAST("{self.inputCol}" AS {self.dataType}) AS "{self.outputCol}" FROM __THIS__'
