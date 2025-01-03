from __future__ import absolute_import

import logging
import os
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
