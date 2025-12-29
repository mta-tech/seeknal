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


@pytest.fixture(scope="session")
def spark_temp_dirs(tmpdir_factory):
    return tmpdir_factory.mktemp("spark-warehouse"), tmpdir_factory.mktemp("meta_dir")


@pytest.fixture(scope="session")
def spark(request, spark_temp_dirs):
    spark_warehouse_dir, meta_dir = spark_temp_dirs
    # current is ./tests
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    # in ./lib/external
    external_libs_dir = os.path.join(cur_dir, os.pardir, os.pardir, "lib", "external")
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
