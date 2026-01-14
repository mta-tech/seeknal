"""Data loaders."""

from pyspark.sql import DataFrame, SparkSession
from ..base import BaseLoaderPySpark


class ParquetWriter(BaseLoaderPySpark):
    """Write DataFrame to Parquet file.

    Args:
        spark: SparkSession
        path: Output path
        mode: Write mode (overwrite, append, ignore, error)
        partition_by: Optional partitioning columns
    """

    def __init__(
        self,
        spark: SparkSession,
        path: str,
        mode: str = "overwrite",
        partition_by: list = None,
        **kwargs
    ):
        super().__init__(spark, **kwargs, path=path, mode=mode,
                         partition_by=partition_by)
        self.path = path
        self.mode = mode
        self.partition_by = partition_by

    def load(self, df: DataFrame) -> None:
        """Write DataFrame to Parquet.

        Args:
            df: DataFrame to write
        """
        writer = df.write.mode(self.mode)
        if self.partition_by:
            writer = writer.partitionBy(*self.partition_by)
        writer.parquet(self.path)
