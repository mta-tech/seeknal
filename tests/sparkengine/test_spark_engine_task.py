"""Tests for SparkEngineTask."""

import pytest
from pyspark.sql import SparkSession
import tempfile
import os
from tests.sparkengine.data_utils import create_sample_dataframe


def test_spark_engine_task_simple_pipeline(spark_session: SparkSession, tmp_path):
    """Test SparkEngineTask with simple pipeline."""
    from seeknal.tasks.sparkengine.pyspark.spark_engine_task import SparkEngineTask

    # Create test data
    df = create_sample_dataframe(spark_session, 100)
    test_path = os.path.join(tmp_path, "input.parquet")
    df.write.parquet(test_path)

    # Build pipeline
    task = SparkEngineTask(spark=spark_session)
    task.add_input(path=test_path, format="parquet")
    task.add_stage(
        stage_id="filter",
        transformer_class="FilterByExpr",
        params={"expression": "id > 50"}
    )
    task.add_stage(
        stage_id="double",
        transformer_class="AddColumnByExpr",
        params={"column_name": "doubled", "expression": "value * 2"}
    )
    result = task.transform()

    # Should have 49 rows (id 51-99, since data has ids 0-99)
    assert result.count() == 49
    assert "doubled" in result.columns


def test_spark_engine_task_with_output(spark_session: SparkSession, tmp_path):
    """Test SparkEngineTask with output."""
    from seeknal.tasks.sparkengine.pyspark.spark_engine_task import SparkEngineTask

    # Create test data
    df = create_sample_dataframe(spark_session, 100)
    input_path = os.path.join(tmp_path, "input.parquet")
    output_path = os.path.join(tmp_path, "output.parquet")
    df.write.parquet(input_path)

    # Build pipeline with output
    task = SparkEngineTask(spark=spark_session)
    task.add_input(path=input_path, format="parquet")
    task.add_stage(
        stage_id="filter",
        transformer_class="FilterByExpr",
        params={"expression": "id > 50"}
    )
    task.add_output(path=output_path, format="parquet")
    task.evaluate()

    # Verify output
    result = spark_session.read.parquet(output_path)
    assert result.count() == 49
