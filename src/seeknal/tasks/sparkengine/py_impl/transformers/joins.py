"""Join transformers."""

from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr as F_expr
from ..base import BaseTransformerPySpark


class JoinById(BaseTransformerPySpark):
    """Join two DataFrames by column names.

    Args:
        right_df: Right DataFrame to join
        left_columns: Left join columns
        right_columns: Right join columns
        join_type: Type of join (inner, left, right, full)
    """

    def __init__(
        self,
        right_df: DataFrame,
        left_columns: list,
        right_columns: list,
        join_type: str = "inner",
        **kwargs
    ):
        super().__init__(**kwargs, right_df=right_df, left_columns=left_columns,
                         right_columns=right_columns, join_type=join_type)
        self.right_df = right_df
        self.left_columns = left_columns
        self.right_columns = right_columns
        self.join_type = join_type

    def transform(self, df: DataFrame) -> DataFrame:
        """Join DataFrames by column IDs.

        Args:
            df: Left DataFrame

        Returns:
            Joined DataFrame
        """
        join_expr = [
            col(left) == col(right)
            for left, right in zip(self.left_columns, self.right_columns)
        ]

        if len(join_expr) > 1:
            # Use & operator with parentheses for proper precedence
            join_condition = reduce(lambda x, y: x & y, join_expr)
        else:
            join_condition = join_expr[0]

        return df.join(self.right_df, join_condition, self.join_type)


class JoinByExpr(BaseTransformerPySpark):
    """Join two DataFrames by expression.

    Args:
        right_df: Right DataFrame to join
        expression: Join expression
        join_type: Type of join (inner, left, right, full)
    """

    def __init__(
        self,
        right_df: DataFrame,
        expression: str,
        join_type: str = "inner",
        **kwargs
    ):
        super().__init__(**kwargs, right_df=right_df, expression=expression,
                         join_type=join_type)
        self.right_df = right_df
        self.expression = expression
        self.join_type = join_type

    def transform(self, df: DataFrame) -> DataFrame:
        """Join DataFrames by expression.

        Args:
            df: Left DataFrame

        Returns:
            Joined DataFrame
        """
        return df.join(self.right_df, F_expr(self.expression), self.join_type)
