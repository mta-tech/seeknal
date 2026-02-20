"""Workflow-specific test configuration.

Overrides the root conftest's autouse ``clean_spark_state_between_tests``
fixture so that workflow tests (which never use Spark) skip the slow JVM
initialization entirely.
"""

from __future__ import annotations

from typing import Generator

import pytest  # ty: ignore[unresolved-import]


@pytest.fixture(autouse=True)
def clean_spark_state_between_tests() -> Generator[None, None, None]:
    """No-op override — workflow tests don't use Spark."""
    yield
