"""Tests for loaders."""

import pytest
from pyspark.sql import SparkSession
import tempfile
import os


def test_parquet_writer(spark_session: SparkSession, tmp_path):
    """Test ParquetWriter."""
    from seeknal.tasks.sparkengine.py_impl.loaders.parquet_writer import ParquetWriter
    from tests.sparkengine.data_utils import create_sample_dataframe

    df = create_sample_dataframe(spark_session, 10)
    output_path = os.path.join(tmp_path, "output.parquet")

    loader = ParquetWriter(spark=spark_session, path=output_path)
    loader.load(df)

    # Verify file was written
    assert os.path.exists(output_path)

    # Verify data
    result = spark_session.read.parquet(output_path)
    assert result.count() == 10
