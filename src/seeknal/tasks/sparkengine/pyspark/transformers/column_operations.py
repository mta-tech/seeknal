"""Column operation transformers."""

from pyspark.sql import DataFrame
from ..base import BaseTransformerPySpark


class ColumnRenamed(BaseTransformerPySpark):
    """Rename a column.

    Args:
        old_name: Existing column name
        new_name: New column name
    """

    def __init__(self, old_name: str, new_name: str, **kwargs):
        super().__init__(**kwargs, old_name=old_name, new_name=new_name)
        self.old_name = old_name
        self.new_name = new_name

    def transform(self, df: DataFrame) -> DataFrame:
        """Rename the specified column.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with renamed column
        """
        return df.withColumnRenamed(self.old_name, self.new_name)


class FilterByExpr(BaseTransformerPySpark):
    """Filter rows by expression.

    Args:
        expression: SQL expression for filtering
    """

    def __init__(self, expression: str, **kwargs):
        super().__init__(**kwargs, expression=expression)
        self.expression = expression

    def transform(self, df: DataFrame) -> DataFrame:
        """Filter DataFrame by expression.

        Args:
            df: Input DataFrame

        Returns:
            Filtered DataFrame
        """
        return df.filter(self.expression)
