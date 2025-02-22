package tech.mta.seeknal.connector.loader

import io.circe.Json
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField}
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, Ignore}
import org.scalatestplus.junit.JUnitRunner
import tech.mta.seeknal.{BaseSparkSpec, DataFrameBuilder}
import tech.mta.seeknal.connector.{IOMaterial, SourceConfig}

@Ignore
@RunWith(classOf[JUnitRunner])
class GenericLoaderSpec extends BaseSparkSpec with BeforeAndAfterAll {

  val outputDir = "build/integ-test/output"
  val dbName = "dummy"

  override def beforeAll() {
    super.beforeAll()
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName").collect()
  }

  override def afterAll() {

    spark.sql(s"DROP DATABASE IF EXISTS $dbName CASCADE").collect()

    super.afterAll()
  }

  // TODO: write more tests
  "Generic loader pipeline" should {

    val inputBuilder = DataFrameBuilder(
      Seq(Row("1", "a"), Row("2", "b")),
      Seq(StructField("id", StringType), StructField("column", StringType)))

    val params = Json
      .obj(
        ("format", Json.fromString("hive"))
      )
      .asObject

    val output = SourceConfig(id = Some("id"),
      source = Some("generic"),
      table = Some(s"$dbName.dummy_table"),
      params = params)

    "return write df correctly" in {
      val loader = IOMaterial(spark, output, Some(inputBuilder.build()))
      val genericLoader = new GenericLoader(loader)

      genericLoader.save()
      assertDataFrameDataEquals(inputBuilder.build(), spark.table("dummy.dummy_table"))
    }
  }
}
