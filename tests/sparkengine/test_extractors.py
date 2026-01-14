"""Tests for extractors."""

import pytest
from pyspark.sql import SparkSession
import tempfile
import os


def test_file_source_parquet(spark_session: SparkSession, tmp_path):
    """Test FileSource with Parquet."""
    from seeknal.tasks.sparkengine.py_impl.extractors.file_source import FileSource

    # Create test file
    from tests.sparkengine.data_utils import create_sample_dataframe
    test_df = create_sample_dataframe(spark_session, 10)
    test_path = os.path.join(tmp_path, "test.parquet")
    test_df.write.parquet(test_path)

    extractor = FileSource(spark=spark_session, path=test_path, format="parquet")
    result = extractor.extract()

    assert result.count() == 10
    assert len(result.columns) == 5


def test_generic_source(spark_session: SparkSession, tmp_path):
    """Test GenericSource."""
    from seeknal.tasks.sparkengine.py_impl.extractors.file_source import GenericSource

    test_df = spark_session.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
    test_path = os.path.join(tmp_path, "test.json")
    test_df.coalesce(1).write.json(test_path)

    extractor = GenericSource(
        spark=spark_session,
        path=test_path,
        format="json",
    )
    result = extractor.extract()

    # Verify data was read
    assert result.count() >= 1
    assert "id" in result.columns
    assert "name" in result.columns
