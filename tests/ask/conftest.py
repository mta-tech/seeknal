"""Conftest for ask tests — overrides the global Spark-dependent autouse fixtures."""

import pytest


@pytest.fixture(autouse=True)
def clean_spark_state_between_tests():
    """No-op override: ask tests don't use Spark."""
    yield
