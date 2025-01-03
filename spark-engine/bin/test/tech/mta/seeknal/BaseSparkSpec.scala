package tech.mta.seeknal

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec


trait BaseSparkSpec extends AnyWordSpec with DataFrameSuiteBase with Matchers {
  override implicit def reuseContextIfPossible: Boolean = true
  implicit lazy val baseSparkSession: SparkSession = spark
  implicit val baseSparkSessionOption: Option[SparkSession] = None

  // Need to explicitly to set CATALOG_IMPLEMENTATION.key as hive for Spark 2.3
  override def conf: SparkConf = super.conf
    .set(CATALOG_IMPLEMENTATION.key, "hive")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.driver.maxResultSize", "2g")
    .set("spark.sql.session.timeZone", "Asia/Singapore")
    .set("spark.sql.defaultUrlStreamHandlerFactory.enabled", "false")


  // Override assertDataFrameEquals to only check schema based on column name only due to
  // nullable property have different behaviour in different Spark version
  override def assertDataFrameEquals(expected: DataFrame, result: DataFrame) {

    assert(expected.schema.fieldNames === result.schema.fieldNames, "FieldNames Equal")

    try {
      expected.rdd.cache
      result.rdd.cache
      assert(expected.rdd.count === result.rdd.count, "Length not Equal")

      val expectedIndexValue = myZipWithIndex(expected.rdd)
      val resultIndexValue = myZipWithIndex(result.rdd)

      val unequalRDD = expectedIndexValue.join(resultIndexValue).filter { case (idx, (r1, r2)) =>
          !r1.equals(r2)
        }

      assert(unequalRDD.take(maxUnequalRowsToShow).isEmpty)
    } finally {
      expected.rdd.unpersist()
      result.rdd.unpersist()
    }
  }

  override def assertDataFrameApproximateEquals(expected: DataFrame, result: DataFrame, tol: Double) {

    // was: assert(expected.schema, result.schema)
    assert(expected.schema === result.schema)

    try {
      expected.rdd.cache
      result.rdd.cache
      // was: assert("Length not Equal", expected.rdd.count, result.rdd.count)
      assert(expected.rdd.count === result.rdd.count, "Length not Equal")

      val expectedIndexValue = myZipWithIndex(expected.rdd)
      val resultIndexValue = myZipWithIndex(result.rdd)

      val unequalRDD = expectedIndexValue.join(resultIndexValue).filter { case (idx, (r1, r2)) =>
        !(r1.equals(r2) || DataFrameSuiteBase.approxEquals(r1, r2, tol))
      }

      // was: assertEmpty(unequalRDD.take(maxUnequalRowsToShow))
      assert(unequalRDD.take(maxUnequalRowsToShow).isEmpty)
    } finally {
      expected.rdd.unpersist()
      result.rdd.unpersist()
    }
  }

  override def assertTrue(expected: Boolean): Unit =
    assert(expected === true)

  private[seeknal] def myZipWithIndex[U](rdd: RDD[U]) = {
    rdd.zipWithIndex().map{ case (row, idx) => (idx, row) }
  }

}
