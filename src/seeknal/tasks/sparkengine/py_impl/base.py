"""Base classes for PySpark transformers and aggregators."""

from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession
from typing import Any, Dict, Optional
import yaml


class BaseTransformerPySpark(ABC):
    """Base class for all PySpark transformers.

    All transformers must implement the transform() method.
    """

    def __init__(self, **kwargs):
        """Initialize transformer with configuration.

        Args:
            **kwargs: Transformer-specific configuration
        """
        self.config = kwargs

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """Transform the input DataFrame.

        Args:
            df: Input DataFrame

        Returns:
            Transformed DataFrame
        """
        pass

    def get_config(self) -> Dict[str, Any]:
        """Get transformer configuration.

        Returns:
            Configuration dictionary
        """
        return self.config

    @classmethod
    def from_yaml(cls, yaml_str: str) -> "BaseTransformerPySpark":
        """Create transformer from YAML configuration.

        Args:
            yaml_str: YAML configuration string

        Returns:
            Transformer instance
        """
        config = yaml.safe_load(yaml_str)
        return cls(**config)


class BaseAggregatorPySpark(ABC):
    """Base class for all PySpark aggregators."""

    def __init__(self, **kwargs):
        """Initialize aggregator with configuration.

        Args:
            **kwargs: Aggregator-specific configuration
        """
        self.config = kwargs

    @abstractmethod
    def aggregate(self, df: DataFrame) -> DataFrame:
        """Aggregate the input DataFrame.

        Args:
            df: Input DataFrame

        Returns:
            Aggregated DataFrame
        """
        pass

    def get_config(self) -> Dict[str, Any]:
        """Get aggregator configuration.

        Returns:
            Configuration dictionary
        """
        return self.config


class BaseExtractorPySpark(ABC):
    """Base class for data source extractors."""

    def __init__(self, spark: SparkSession, **kwargs):
        """Initialize extractor.

        Args:
            spark: SparkSession
            **kwargs: Extractor-specific configuration
        """
        self.spark = spark
        self.config = kwargs

    @abstractmethod
    def extract(self) -> DataFrame:
        """Extract data from source.

        Returns:
            DataFrame
        """
        pass


class BaseLoaderPySpark(ABC):
    """Base class for data loaders."""

    def __init__(self, spark: SparkSession, **kwargs):
        """Initialize loader.

        Args:
            spark: SparkSession
            **kwargs: Loader-specific configuration
        """
        self.spark = spark
        self.config = kwargs

    @abstractmethod
    def load(self, df: DataFrame) -> None:
        """Load DataFrame to destination.

        Args:
            df: DataFrame to load
        """
        pass
