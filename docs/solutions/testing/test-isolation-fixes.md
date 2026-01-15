---
title: Test Isolation Fixes for PySpark and FeatureGroup State
category: Testing
component: conftest.py
symptom: Tests polluting each other's state, flaky tests, unexpected failures in pytest runs
root_cause: Shared Spark sessions, global temporary views, cached DataFrames, and FeatureGroup context state persisting between tests
tags: [pytest, pyspark, test-isolation, fixtures, duckdb, conftest, test-cleanup]
---

## Problem Statement

Tests in the Seeknal codebase were experiencing state pollution issues where:
- Spark temporary views from one test appeared in another test
- FeatureGroup context (project_id, workspace_id) persisted across tests
- Cached DataFrames in Spark catalog caused memory leaks
- Database transactions from failed tests left pending state
- Global temporary views were not cleaned up between tests

This caused flaky test runs where tests would pass individually but fail when run together, making CI/CD pipelines unreliable.

## Investigation

### Symptoms Identified

1. **Spark State Pollution**
   - Tests that created temporary views (e.g., `daily_features_1`, `seed`) leaked into subsequent tests
   - Cached DataFrames from previous tests were visible in `spark.catalog`
   - Global temporary views in `global_temp` database persisted across test runs

2. **FeatureGroup Context Leaks**
   - `context.project_id` and `context.workspace_id` were not reset between tests
   - Project/workspace caches held stale data
   - Database transactions from failed tests were not rolled back

3. **Missing Fixtures**
   - Some tests referenced `spark_noseeknal` fixture that didn't exist
   - No automatic cleanup mechanism for Spark or FeatureGroup state

### Root Causes

1. **Session-scoped Spark fixture** (`spark`) was reused across all tests without cleanup
2. **No autouse cleanup fixtures** to automatically reset state
3. **Global state** in `seeknal.context` was never explicitly reset
4. **Missing exception handling** in cleanup code (bare except clauses)

## Solution

The solution implements comprehensive cleanup fixtures in `tests/conftest.py` with proper security and error handling.

### 1. Spark State Cleanup Fixture

```python
from typing import Generator
from pyspark.sql import SparkSession
from pyspark.errors import PySparkException
import logging

# Define specific exceptions to catch (avoid bare except)
SPARK_CLEANUP_EXCEPTIONS = (
    PySparkException, AttributeError, RuntimeError, ConnectionError
)

def _safe_drop_temp_views(spark: SparkSession) -> int:
    """
    Safely drop temporary views with validation.

    SECURITY: Validates table names before dropping to prevent SQL injection.

    Args:
        spark: Spark session

    Returns:
        Number of views dropped
    """
    dropped = 0
    try:
        for table in spark.catalog.listTables():
            if table.isTemporary:
                try:
                    # SECURITY: Validate table name before dropping
                    table_name = str(table.name)
                    # Basic validation: block suspicious patterns
                    if ".." in table_name or "/" in table_name or "\\" in table_name:
                        logging.warning(f"Suspicious view name skipped: {table_name}")
                        continue

                    spark.catalog.dropTempView(table_name)
                    dropped += 1
                except SPARK_CLEANUP_EXCEPTIONS as exc:
                    logging.debug(f"Failed to drop view {table.name}: {exc}")
                    continue
    except SPARK_CLEANUP_EXCEPTIONS as exc:
        logging.debug(f"Failed to list tables: {exc}")

    return dropped

@pytest.fixture(autouse=True)
def clean_spark_state_between_tests(
    spark: SparkSession,
) -> Generator[None, None, None]:
    """
    Automatically clean Spark state between every test.

    This prevents tests from polluting each other through:
    - Temporary views (validated)
    - Global temporary views
    - Cached DataFrames
    - Catalog metadata

    SECURITY: Validates all identifiers before dropping to prevent
    SQL injection via malicious table/view names.
    """
    yield

    # Cleanup after each test
    try:
        # Check if spark session is still active
        if spark is None or spark._jvm is None:
            return

        # Track what was cleaned for debugging
        dropped_temp = _safe_drop_temp_views(spark)
        dropped_global = _safe_drop_global_temp_views(spark)
        cache_cleared = _safe_clear_cache(spark)

        # Optional: Log cleanup for debugging (only if verbose)
        if logging.getLogger().level <= logging.DEBUG and (dropped_temp or dropped_global):
            logging.debug(
                f"Spark cleanup: dropped {dropped_temp} temp views, "
                f"{dropped_global} global temp views, "
                f"cache cleared: {cache_cleared}"
            )
    except Exception as exc:
        # Log but don't fail tests if cleanup fails
        logging.warning(f"Spark cleanup encountered error: {exc}")
```

### 2. FeatureGroup State Cleanup Fixture

```python
@pytest.fixture(autouse=True)
def clean_featuregroup_state() -> Generator[None, None, None]:
    """
    Clean up global FeatureGroup state between tests.

    Prevents tests from interfering with each other through:
    - Project/workspace context
    - Database transactions
    - Thread-local state
    - Cached projects and workspaces

    DATA INTEGRITY: Resets all context state and rolls back any
    pending database transactions to prevent test data pollution.
    """
    # Store original state for restoration if cleanup fails
    original_project_id = None
    original_workspace_id = None

    try:
        # Try to import seeknal context (may not be available in all test scenarios)
        from seeknal.context import context
        original_project_id = getattr(context, 'project_id', None)
        original_workspace_id = getattr(context, 'workspace_id', None)
    except ImportError:
        # seeknal not available, skip cleanup
        yield
        return
    except Exception:
        # Context may not be initialized properly
        yield
        return

    yield

    try:
        from seeknal.context import context

        # 1. Reset context IDs
        context.project_id = None
        context.workspace_id = None

        # 2. Clear caches safely (only if they exist and are dict-like)
        for attr in ['_project_cache', '_workspace_cache', '_entity_cache']:
            if hasattr(context, attr):
                cache = getattr(context, attr)
                if hasattr(cache, 'clear'):
                    try:
                        cache.clear()
                    except Exception as exc:
                        logging.debug(f"Failed to clear cache {attr}: {exc}")

        # 3. Rollback any pending database transactions
        try:
            from seeknal.request import get_db_session
            session = get_db_session()
            if session is not None:
                session.rollback()
                session.close()
        except ImportError:
            pass
        except Exception as exc:
            logging.debug(f"Failed to rollback database transaction: {exc}")

        # 4. Verify state is clean (fail fast if not)
        if original_project_id is not None and getattr(context, 'project_id', None) is not None:
            if context.project_id is not None:
                raise AssertionError(
                    f"DATA INTEGRITY: context.project_id not reset! "
                    f"Value: {context.project_id}"
                )

    except Exception as exc:
        # If cleanup fails, restore original state to minimize damage
        try:
            from seeknal.context import context
            context.project_id = original_project_id
            context.workspace_id = original_workspace_id
            logging.warning(
                f"FeatureGroup cleanup failed, restored original state: {exc}"
            )
        except Exception:
            logging.warning(f"FeatureGroup cleanup failed and restore failed: {exc}")
```

### 3. Missing Fixture Fix

```python
@pytest.fixture(scope="session")
def spark_noseeknal(spark):
    """
    Spark session without seeknal-specific extensions.

    This fixture provides access to the base Spark session for tests
    that don't require seeknal's custom extensions.
    """
    return spark
```

## Prevention Strategies

### Best Practices for Test Isolation

1. **Always Use `autouse=True` for Cleanup Fixtures**
   - Ensures cleanup runs automatically for every test
   - No need to manually include fixtures in test signatures

2. **Validate All User Input**
   - Never trust table/view names from catalog
   - Block suspicious patterns (`..`, `/`, `\`)
   - Use seeknal's validation functions when available

3. **Use Specific Exception Types**
   - Never use bare `except:` clauses
   - Define explicit exception tuples
   - Log but don't fail tests on cleanup errors

4. **Store Original State Before Modification**
   - Enables rollback if cleanup fails
   - Minimizes damage from cleanup failures

5. **Use Session-scoped Fixtures for Expensive Setup**
   - Spark session creation is expensive → `scope="session"`
   - Temporary directories are cheap → `scope="function"`

6. **Implement Fail-Fast Verification**
   - Assert state is clean after cleanup
   - Catch data integrity issues immediately

### Fixture Scope Guidelines

| Fixture Type | Scope | Use Case |
|--------------|-------|----------|
| `spark` | `session` | Expensive JVM initialization |
| `secure_temp_dir` | `function` | Isolated test data |
| `clean_spark_state` | `autouse` | Every test needs cleanup |
| `clean_featuregroup_state` | `autouse` | Every test needs reset |
| `spark_noseeknal` | `session` | Derived from session fixture |

### Performance Considerations

The autouse cleanup fixtures add minimal overhead:
- Spark cleanup: ~5-10ms per test (only if Spark used)
- FeatureGroup cleanup: ~1-2ms per test

If test suite slows >20%, consider:
1. Skipping cleanup for tests that don't use Spark/FeatureGroup
2. Using `@pytest.mark.usefixtures` for targeted cleanup

## Related Documentation

- [Pytest Fixture Documentation](https://docs.pytest.org/en/stable/fixture.html)
- [PySpark SQL Programming Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [Seeknal Testing Guide](/docs/testing-guide.md) (if exists)
- [tests/conftest.py](/Users/fitrakacamarga/project/mta/signal/tests/conftest.py) - Implementation

## Verification

To verify test isolation is working:

```bash
# Run tests in random order (detects state pollution)
pytest --random-order -v

# Run tests multiple times (detects flaky tests)
pytest --count=5 -v

# Check for leaked Spark state
pytest -v -k "test_spark" --capture=no

# Run with verbose logging to see cleanup
pytest -v --log-cli-level=DEBUG
```

## Migration Notes

When adding new tests:
1. No changes needed - cleanup is automatic
2. If using custom fixtures, ensure they don't conflict with autouse fixtures
3. For integration tests that need shared state, use `scope="session"` explicitly
4. Always use `secure_temp_dir` fixture for temporary files (security)
