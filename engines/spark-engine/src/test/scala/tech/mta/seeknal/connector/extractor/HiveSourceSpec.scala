package tech.mta.seeknal.connector.extractor

import java.io.File
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField}
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, Ignore}
import org.scalatestplus.junit.JUnitRunner
import tech.mta.seeknal.{BaseSparkSpec, DataFrameBuilder}
import tech.mta.seeknal.connector.{IOMaterial, SourceConfig}

@Ignore
@RunWith(classOf[JUnitRunner])
class HiveSourceSpec extends BaseSparkSpec with BeforeAndAfterAll {

  val dbName = "dummy"
  override def beforeAll() {
    super.beforeAll()
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName").collect()

    // sample data
    val inputBuilder = DataFrameBuilder(
      Seq(
        Row("1", "a"),
        Row("2", "b")),
      Seq(
        StructField("id", StringType),
        StructField("column", StringType)))
    .build()
    val tableName = s"$dbName.dummy"
    inputBuilder.write.mode("overwrite").format("parquet").saveAsTable(tableName)
  }

  override def afterAll() {
    // delete data directory
    spark.sql(s"DROP DATABASE IF EXISTS $dbName CASCADE").collect()
    super.afterAll()
  }

  "HiveSource" should {

    val inputConfig = SourceConfig(table = Some("dummy.dummy"))
    "get table name correctly" in {
      val extractor = IOMaterial(spark, inputConfig)
      val hiveSource = new HiveSource(extractor)
      assert(hiveSource.tableName == "dummy.dummy")
    }

    "get read data from hive table" in {
      val expected = DataFrameBuilder(
        Seq(
          Row("1", "a"),
          Row("2", "b")),
        Seq(
          StructField("id", StringType),
          StructField("column", StringType)))
        .build()
      val extractor = IOMaterial(spark, inputConfig)
      val hiveSource = new HiveSource(extractor)
      val output = hiveSource.get()
          .orderBy("id")
      assertDataFrameEquals(expected, output)
    }
  }
}
