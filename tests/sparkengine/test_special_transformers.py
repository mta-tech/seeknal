"""Tests for special transformers (UDFs)."""

import pytest
from pyspark.sql import SparkSession
from tests.sparkengine.data_utils import create_sample_dataframe


def test_add_entropy(spark_session: SparkSession):
    """Test AddEntropy transformer."""
    from seeknal.tasks.sparkengine.pyspark.transformers.special import AddEntropy

    # Create data with values (probability distributions)
    data = [(1, [0.1, 0.2, 0.7]), (2, [0.5, 0.5, 0.0])]
    schema = ["id", "probs"]
    df = spark_session.createDataFrame(data, schema)

    transformer = AddEntropy(
        input_col="probs",
        output_col="entropy"
    )
    result = transformer.transform(df)

    assert "entropy" in result.columns
    # Entropy should be positive
    assert result.filter("entropy <= 0").count() == 0
    # Entropy of [0.1, 0.2, 0.7] should be higher than [0.5, 0.5, 0.0]
    # (more uncertain distribution = higher entropy)
    row1 = result.filter("id = 1").first()
    row2 = result.filter("id = 2").first()
    assert row1["entropy"] > row2["entropy"]


def test_add_latlong_distance(spark_session: SparkSession):
    """Test AddLatLongDistance transformer."""
    from seeknal.tasks.sparkengine.pyspark.transformers.special import AddLatLongDistance

    data = [
        (1, 40.7128, -74.0060, 34.0522, -118.2437),  # NYC to LA
        (2, 51.5074, -0.1278, 48.8566, 2.3522),      # London to Paris
    ]
    schema = ["id", "lat1", "lon1", "lat2", "lon2"]
    df = spark_session.createDataFrame(data, schema)

    transformer = AddLatLongDistance(
        lat1_col="lat1",
        lon1_col="lon1",
        lat2_col="lat2",
        lon2_col="lon2",
        output_col="distance"
    )
    result = transformer.transform(df)

    assert "distance" in result.columns
    # Distance should be positive
    assert result.filter("distance <= 0").count() == 0
    # NYC to LA is ~3944 km
    row = result.filter("id = 1").first()
    assert 3900 < row["distance"] < 4000
