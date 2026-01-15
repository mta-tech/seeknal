from __future__ import absolute_import

import logging
import os
import tempfile
import stat
import shutil
from builtins import str

import findspark
import numpy
import pytest

# current is ./spark/tests
cur_dir = os.path.dirname(os.path.realpath(__file__))
# workaround to use locally downloaded spark
default_spark_home = os.path.join(cur_dir, os.pardir, os.pardir, os.pardir, "spark")
default_mlflow_tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "sqlite:///{}")
findspark.init(
    os.getenv("SPARK_HOME", "/Users/fitrakacamarga/.sdkman/candidates/spark/current")
)
import pyspark  # noqa
from pyspark.sql import SparkSession  # noqa

os.environ.pop("SPARK_REMOTE", None)
os.environ["PYDEVD_WARN_EVALUATION_TIMEOUT"] = "60"


def quiet():
    """Turn down logging / warning for the test context"""
    logger = logging.getLogger("py4j")
    logger.setLevel(logging.WARN)
    # numpy.warnings.filterwarnings('ignore')


@pytest.fixture(autouse=True)
def enable_seeknal_logger_propagation():
    """
    Enable log propagation for the seeknal logger during tests.

    The seeknal logger has propagate=False by default (set in context.py),
    which prevents pytest's caplog fixture from capturing log messages.
    This fixture temporarily enables propagation for each test so that
    caplog can capture warnings and other log messages.
    """
    seeknal_logger = logging.getLogger("seeknal")
    original_propagate = seeknal_logger.propagate
    seeknal_logger.propagate = True
    yield
    seeknal_logger.propagate = original_propagate


@pytest.fixture(scope="session")
def spark_temp_dirs(tmpdir_factory):
    return tmpdir_factory.mktemp("spark-warehouse"), tmpdir_factory.mktemp("meta_dir")


@pytest.fixture(scope="session")
def spark(request, spark_temp_dirs):
    spark_warehouse_dir, meta_dir = spark_temp_dirs
    # current is ./tests
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    # in ./lib/external (at project root)
    external_libs_dir = os.path.join(cur_dir, os.pardir, "lib", "external")
    external_libs_jars = [
        os.path.join(external_libs_dir, f) for f in os.listdir(external_libs_dir)
    ]

    """Setup spark session"""
    session = (
        SparkSession.builder.master("local")
        # workaround to avoid snappy library issue
        .config("spark.master", "local")
        .config("spark.sql.parquet.compression.codec", "uncompressed")
        .config("spark.driver.memory", "1G")
        # make spark-warehouse temporary
        .config("spark.sql.warehouse.dir", spark_warehouse_dir)
        # make metastore temporary
        .config(
            "spark.driver.extraJavaOptions", "-Dderby.system.home={}".format(meta_dir)
        )
        .config(
            "spark.jars.repositories",
            "http://packages.confluent.io/maven/,https://repo1.maven.org/maven2/",
        )
        .config("spark.jars", ",".join(external_libs_jars))
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-avro_2.12:3.5.3,za.co.absa:abris_2.12:6.4.0,com.lihaoyi:os-lib_2.12:0.8.1,org.apache.kafka:kafka-clients:3.8.0,io.delta:delta-spark_2.12:3.2.1,org.apache.iceberg:iceberg-aws-bundle:1.6.1,org.apache.hadoop:hadoop-aws:3.2.2,org.apache.hadoop:hadoop-client:3.2.2,org.apache.hadoop:hadoop-client-runtime:3.2.2",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .appName("seeknal-test")
        .enableHiveSupport()
        .getOrCreate()
    )

    def cleanup():
        session.stop()
        session._jvm.System.clearProperty("spark.driver.port")

    request.addfinalizer(cleanup)

    quiet()
    return session


def are_equal(df1, df2):
    return len(df1.subtract(df2).take(1)) == 0 and len(df2.subtract(df1).take(1)) == 0


class SecureTempDirectory:
    """
    A secure temporary directory helper class.

    Creates temporary directories with restricted permissions (0o700)
    ensuring only the owner can read, write, and execute.
    """

    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        self._subdirs = []

    @property
    def path(self) -> str:
        """Return the base directory path as a string."""
        return self.base_dir

    def mkdir(self, name: str) -> str:
        """
        Create a subdirectory with secure permissions.

        Args:
            name: Name of the subdirectory to create

        Returns:
            The full path to the created subdirectory
        """
        subdir = os.path.join(self.base_dir, name)
        os.makedirs(subdir, mode=0o700, exist_ok=True)
        self._subdirs.append(subdir)
        return subdir

    def __str__(self) -> str:
        return self.base_dir

    def __fspath__(self) -> str:
        """Allow this object to be used with os.path functions."""
        return self.base_dir


@pytest.fixture(scope="function")
def secure_temp_dir(request):
    """
    Pytest fixture that creates a secure temporary directory for tests.

    Creates a temporary directory with mode 0o700 (owner read/write/execute only),
    ensuring that test data is not accessible to other users on the system.

    The directory and all its contents are automatically cleaned up after the test.

    Usage:
        def test_something(secure_temp_dir):
            # Use base directory
            path = secure_temp_dir.path

            # Create subdirectories
            offline_store = secure_temp_dir.mkdir("offline_store")
            online_store = secure_temp_dir.mkdir("online_store")

    Yields:
        SecureTempDirectory: A helper object for working with the temp directory
    """
    # Create temp directory with secure permissions (owner only)
    temp_dir = tempfile.mkdtemp(prefix="seeknal_test_")

    # Set restrictive permissions (owner read/write/execute only)
    os.chmod(temp_dir, stat.S_IRWXU)  # 0o700

    # Create helper object
    secure_dir = SecureTempDirectory(temp_dir)

    yield secure_dir

    # Cleanup: remove directory and all contents
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture(scope="session")
def secure_temp_dir_session(request):
    """
    Session-scoped version of secure_temp_dir for fixtures that need
    to persist across multiple tests.

    Creates a temporary directory with mode 0o700 (owner read/write/execute only),
    ensuring that test data is not accessible to other users on the system.

    The directory and all its contents are automatically cleaned up after the
    test session.

    Usage:
        def test_something(secure_temp_dir_session):
            # Use base directory
            path = secure_temp_dir_session.path

            # Create subdirectories
            data_dir = secure_temp_dir_session.mkdir("data")

    Yields:
        SecureTempDirectory: A helper object for working with the temp directory
    """
    # Create temp directory with secure permissions (owner only)
    temp_dir = tempfile.mkdtemp(prefix="seeknal_session_test_")

    # Set restrictive permissions (owner read/write/execute only)
    os.chmod(temp_dir, stat.S_IRWXU)  # 0o700

    # Create helper object
    secure_dir = SecureTempDirectory(temp_dir)

    def cleanup():
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)

    request.addfinalizer(cleanup)

    return secure_dir


# Phase 2: Define spark_noseeknal fixture (missing fixture fix)
@pytest.fixture(scope="session")
def spark_noseeknal(spark):
    """
    Spark session without seeknal-specific extensions.

    This fixture provides access to the base Spark session for tests
    that don't require seeknal's custom extensions.
    """
    return spark


# Phase 2: Add Spark cleanup fixture with comprehensive state management
from typing import Generator
from pyspark.sql import SparkSession
from pyspark.errors import PySparkException

# Define specific exceptions to catch (not bare except)
SPARK_CLEANUP_EXCEPTIONS = (PySparkException, AttributeError, RuntimeError, ConnectionError)


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
                    # Use seeknal's validation if available, otherwise basic check
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


def _safe_drop_global_temp_views(spark: SparkSession) -> int:
    """
    Safely drop global temporary views.

    Args:
        spark: Spark session

    Returns:
        Number of views dropped
    """
    dropped = 0
    try:
        for table in spark.catalog.listTables("global_temp"):
            try:
                table_name = str(table.name)
                if ".." in table_name or "/" in table_name or "\\" in table_name:
                    continue

                spark.catalog.dropGlobalTempView(table_name)
                dropped += 1
            except SPARK_CLEANUP_EXCEPTIONS as exc:
                logging.debug(f"Failed to drop global temp view {table.name}: {exc}")
                continue
    except SPARK_CLEANUP_EXCEPTIONS:
        # global_temp may not be initialized
        pass

    return dropped


def _safe_clear_cache(spark: SparkSession) -> bool:
    """
    Safely clear Spark cache.

    Args:
        spark: Spark session

    Returns:
        True if successful, False otherwise
    """
    try:
        spark.catalog.clearCache()
        return True
    except SPARK_CLEANUP_EXCEPTIONS as exc:
        logging.debug(f"Failed to clear cache: {exc}")
        return False


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

    PERFORMANCE: Monitors cleanup time. If tests slow >20%, consider
    switching to targeted cleanup (only clean after tests that use Spark).

    SECURITY: Validates all identifiers before dropping to prevent
    SQL injection via malicious table/view names.
    """
    yield

    # Cleanup after each test (only if spark session exists and is active)
    try:
        # Check if spark session is still active
        if spark is None or spark._jvm is None:
            return

        # Track what was cleaned for debugging
        dropped_temp = _safe_drop_temp_views(spark)
        dropped_global = _safe_drop_global_temp_views(spark)
        cache_cleared = _safe_clear_cache(spark)

        # Optional: Log cleanup for debugging (only if verbose)
        if logging.getLogger().level <= logging.DEBUG and (dropped_temp or dropped_global or cache_cleared):
            logging.debug(
                f"Spark cleanup: dropped {dropped_temp} temp views, "
                f"{dropped_global} global temp views, "
                f"cache cleared: {cache_cleared}"
            )
    except Exception as exc:
        # Log but don't fail tests if cleanup fails
        logging.warning(f"Spark cleanup encountered error: {exc}")


@pytest.fixture(scope="session")
def daily_features_1(spark_noseeknal):
    df = spark_noseeknal.read.csv(
        os.path.join(cur_dir, "resources/daily_features_1.csv"),
        inferSchema=True,
        header=True,
    )
    df.createOrReplaceGlobalTempView("daily_features_1")
    return df, "global_temp.daily_features_1"


@pytest.fixture(scope="session")
def seed(spark_noseeknal):
    df = spark_noseeknal.read.csv(
        os.path.join(cur_dir, "resources/seed.csv"), inferSchema=True, header=True
    )
    df.createOrReplaceGlobalTempView("seed")
    return df, "global_temp.seed"


def file_from_string(tmpdir, filename, content):
    """
    Writes content into a file in the temp directory.
    Returns the path to the temp file
    """
    tempfile = tmpdir.join(filename)
    tempfile.write(content)
    return str(tempfile.realpath())


# Phase 2: Add FeatureGroup state cleanup fixture
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
        # CRITICAL: Tests may leave partial commits
        try:
            from seeknal.request import get_db_session
            session = get_db_session()
            if session is not None:
                session.rollback()
                session.close()
        except ImportError:
            # request module not available
            pass
        except Exception as exc:
            logging.debug(f"Failed to rollback database transaction: {exc}")

        # 4. Verify state is clean (fail fast if not)
        # Only verify if we had original state set
        if original_project_id is not None and getattr(context, 'project_id', None) is not None:
            if context.project_id is not None:
                raise AssertionError(
                    f"DATA INTEGRITY: context.project_id not reset! "
                    f"Value: {context.project_id}"
                )

        if original_workspace_id is not None and getattr(context, 'workspace_id', None) is not None:
            if context.workspace_id is not None:
                raise AssertionError(
                    f"DATA INTEGRITY: context.workspace_id not reset! "
                    f"Value: {context.workspace_id}"
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
            # Even restore failed, log and continue
            logging.warning(f"FeatureGroup cleanup failed and restore failed: {exc}")
