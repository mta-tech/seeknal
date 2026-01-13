"""SQL transformer."""

from pyspark.sql import DataFrame, SparkSession
from ..base import BaseTransformerPySpark


class SQL(BaseTransformerPySpark):
    """Execute SQL query on DataFrame.

    Args:
        spark: SparkSession
        query: SQL query to execute
        view_name: Temporary view name for input DataFrame
    """

    def __init__(self, spark: SparkSession, query: str, view_name: str = "input", **kwargs):
        super().__init__(**kwargs, spark=spark, query=query, view_name=view_name)
        self.spark = spark
        self.query = query
        self.view_name = view_name

    def transform(self, df: DataFrame) -> DataFrame:
        """Execute SQL query.

        Args:
            df: Input DataFrame

        Returns:
            Result DataFrame
        """
        df.createOrReplaceTempView(self.view_name)
        return self.spark.sql(self.query)
