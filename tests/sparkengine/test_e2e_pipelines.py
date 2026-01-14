"""End-to-end integration tests for PySpark pipelines."""

import pytest
from pyspark.sql import SparkSession
import tempfile
import os
from seeknal.tasks.sparkengine import SparkEngineTask


def test_full_pipeline(spark_session: SparkSession, tmp_path):
    """Test complete pipeline with input, transforms, and output."""
    # Create test data
    data = [(i, f"user_{i}", i * 10) for i in range(100)]
    df = spark_session.createDataFrame(data, ["id", "name", "value"])

    input_path = os.path.join(tmp_path, "input.parquet")
    output_path = os.path.join(tmp_path, "output.parquet")

    df.write.parquet(input_path)

    # Build and run pipeline
    task = SparkEngineTask(spark=spark_session)
    task.add_input(path=input_path, format="parquet")
    task.add_stage(
        stage_id="filter",
        transformer_class="FilterByExpr",
        params={"expression": "id > 50"}
    )
    task.add_stage(
        stage_id="add_doubled",
        transformer_class="AddColumnByExpr",
        params={"column_name": "doubled", "expression": "value * 2"}
    )
    task.add_output(path=output_path, format="parquet")
    task.evaluate()

    # Verify output
    result = spark_session.read.parquet(output_path)
    assert result.count() == 49  # id 51-99
    assert "doubled" in result.columns
    assert result.filter("id <= 50").count() == 0


def test_pipeline_with_join(spark_session: SparkSession, tmp_path):
    """Test pipeline with join operation."""
    # Create test data
    left_data = [(1, "alice"), (2, "bob"), (3, "charlie")]
    left_df = spark_session.createDataFrame(left_data, ["id", "name"])

    right_data = [(1, 100), (2, 200), (3, 300)]
    right_df = spark_session.createDataFrame(right_data, ["user_id", "score"])

    input_path = os.path.join(tmp_path, "input.parquet")
    right_path = os.path.join(tmp_path, "right.parquet")
    output_path = os.path.join(tmp_path, "output.parquet")

    left_df.write.parquet(input_path)
    right_df.write.parquet(right_path)

    # Build pipeline with join
    from pyspark.sql import functions as F
    from seeknal.tasks.sparkengine.py_impl.extractors.file_source import FileSource
    from seeknal.tasks.sparkengine.py_impl.transformers.joins import JoinById
    from seeknal.tasks.sparkengine.py_impl.loaders.parquet_writer import ParquetWriter

    # Load left data
    extractor = FileSource(spark=spark_session, path=input_path, format="parquet")
    df = extractor.extract()

    # Load right data for join
    right_extractor = FileSource(spark=spark_session, path=right_path, format="parquet")
    right_df = right_extractor.extract()

    # Apply join
    transformer = JoinById(
        right_df=right_df,
        left_columns=["id"],
        right_columns=["user_id"],
        join_type="inner"
    )
    result = transformer.transform(df)

    # Write output
    loader = ParquetWriter(spark=spark_session, path=output_path)
    loader.load(result)

    # Verify output
    final = spark_session.read.parquet(output_path)
    assert final.count() == 3
    assert "name" in final.columns
    assert "score" in final.columns
