"""Conftest for UI tests -- overrides the global Spark-dependent autouse fixtures."""

import pytest


@pytest.fixture(autouse=True)
def clean_spark_state_between_tests():
    """No-op override: UI tests don't use Spark."""
    yield
