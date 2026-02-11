"""
Minimal conftest for state backend tests.

This conftest is intentionally minimal to avoid the Spark dependencies
from the root conftest.py. State backend tests don't require Spark.
"""

import logging
import pytest


@pytest.fixture(autouse=True)
def enable_seeknal_logger_propagation():
    """
    Enable log propagation for the seeknal logger during tests.
    """
    seeknal_logger = logging.getLogger("seeknal")
    original_propagate = seeknal_logger.propagate
    seeknal_logger.propagate = True
    yield
    seeknal_logger.propagate = original_propagate
