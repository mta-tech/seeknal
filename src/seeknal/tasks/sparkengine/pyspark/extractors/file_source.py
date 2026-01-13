"""File-based extractors."""

from pyspark.sql import DataFrame, SparkSession
from ..base import BaseExtractorPySpark


class FileSource(BaseExtractorPySpark):
    """Extract data from files.

    Args:
        spark: SparkSession
        path: File path
        format: File format (parquet, csv, json, orc)
        options: Optional format options
    """

    def __init__(self, spark: SparkSession, path: str, format: str = "parquet",
                 options: dict = None, **kwargs):
        super().__init__(spark, **kwargs, path=path, format=format, options=options)
        self.path = path
        self.format = format
        self.options = options or {}

    def extract(self) -> DataFrame:
        """Extract data from file.

        Returns:
            DataFrame
        """
        reader = getattr(self.spark.read, self.format)
        if self.options:
            return reader(**self.options)(self.path)
        return reader(self.path)


class GenericSource(BaseExtractorPySpark):
    """Generic data source extractor.

    Args:
        spark: SparkSession
        path: Data path
        format: Data format
        options: Optional format options
    """

    def __init__(self, spark: SparkSession, path: str, format: str,
                 options: dict = None, **kwargs):
        super().__init__(spark, **kwargs, path=path, format=format, options=options)
        self.path = path
        self.format = format
        self.options = options or {}

    def extract(self) -> DataFrame:
        """Extract data using generic format reader.

        Returns:
            DataFrame
        """
        reader = self.spark.read.format(self.format)
        if self.options:
            reader = reader.options(**self.options)
        return reader.load(self.path)
