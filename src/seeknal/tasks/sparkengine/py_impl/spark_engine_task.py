"""PySpark-based SparkEngineTask."""

from typing import Dict, Any, Optional, List
from pyspark.sql import DataFrame, SparkSession
from .extractors.file_source import FileSource
from .loaders.parquet_writer import ParquetWriter
from .transformers import (
    FilterByExpr,
    AddColumnByExpr,
    ColumnRenamed,
    JoinById,
    JoinByExpr,
    SQL,
    AddEntropy,
    AddLatLongDistance,
)


class Stage:
    """Pipeline stage.

    Args:
        stage_id: Unique stage identifier
        transformer_class: Transformer class name
        params: Transformer parameters
    """

    def __init__(self, stage_id: str, transformer_class: str, params: Dict[str, Any]):
        self.stage_id = stage_id
        self.transformer_class = transformer_class
        self.params = params

    def get_transformer(self):
        """Get transformer instance.

        Returns:
            Transformer instance
        """
        # Map class names to classes
        transformer_map = {
            "FilterByExpr": FilterByExpr,
            "AddColumnByExpr": AddColumnByExpr,
            "ColumnRenamed": ColumnRenamed,
            "JoinById": JoinById,
            "JoinByExpr": JoinByExpr,
            "SQL": SQL,
            "AddEntropy": AddEntropy,
            "AddLatLongDistance": AddLatLongDistance,
        }

        cls = transformer_map.get(self.transformer_class)
        if cls is None:
            raise ValueError(f"Unknown transformer: {self.transformer_class}")

        return cls(**self.params)


class SparkEngineTask:
    """PySpark-based data pipeline task.

    This replaces the Scala-based SparkEngine with pure PySpark.

    Args:
        spark: SparkSession
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.input_config: Optional[Dict[str, Any]] = None
        self.stages: List[Stage] = []
        self.output_config: Optional[Dict[str, Any]] = None

    def add_input(self, path: str, format: str = "parquet", **options) -> "SparkEngineTask":
        """Add input source.

        Args:
            path: Input path
            format: File format
            **options: Additional options

        Returns:
            Self for chaining
        """
        self.input_config = {"path": path, "format": format, "options": options}
        return self

    def add_stage(
        self,
        stage_id: str,
        transformer_class: str,
        params: Dict[str, Any]
    ) -> "SparkEngineTask":
        """Add transformation stage.

        Args:
            stage_id: Unique stage identifier
            transformer_class: Transformer class name
            params: Transformer parameters

        Returns:
            Self for chaining
        """
        stage = Stage(stage_id, transformer_class, params)
        self.stages.append(stage)
        return self

    def add_output(self, path: str, format: str = "parquet", **options) -> "SparkEngineTask":
        """Add output destination.

        Args:
            path: Output path
            format: File format
            **options: Additional options

        Returns:
            Self for chaining
        """
        self.output_config = {"path": path, "format": format, "options": options}
        return self

    def transform(self) -> DataFrame:
        """Execute transformation pipeline.

        Returns:
            Transformed DataFrame
        """
        # Load input
        if not self.input_config:
            raise ValueError("No input configured")
        extractor = FileSource(
            spark=self.spark,
            path=self.input_config["path"],
            format=self.input_config.get("format", "parquet"),
            options=self.input_config.get("options", {})
        )
        df = extractor.extract()

        # Apply stages
        for stage in self.stages:
            transformer = stage.get_transformer()
            df = transformer.transform(df)

        return df

    def evaluate(self) -> None:
        """Execute pipeline and write output."""
        result = self.transform()

        if self.output_config:
            if self.output_config.get("format") == "parquet":
                loader = ParquetWriter(
                    spark=self.spark,
                    path=self.output_config["path"],
                    mode=self.output_config.get("mode", "overwrite")
                )
                loader.load(result)
            else:
                raise ValueError(f"Unsupported output format: {self.output_config.get('format')}")
