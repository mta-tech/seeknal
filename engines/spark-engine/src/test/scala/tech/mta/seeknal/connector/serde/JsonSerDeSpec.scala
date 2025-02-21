package tech.mta.seeknal.connector.serde

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, lit, struct, to_json}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import za.co.absa.abris.avro.format.SparkAvroConversions
import tech.mta.seeknal.BaseSparkSpec
import tech.mta.seeknal.params.{ColumnDefinition, Config, SchemaFile}

@RunWith(classOf[JUnitRunner])
class JsonSerDeSpec extends BaseSparkSpec {

  private val recordByteSchemaString = """{
        "namespace": "all-types.test",
        "type": "record",
        "name": "record_name",
        "fields":[
        {"name": "int", "type":  ["int", "null"] }
        ]
      }"""

  private def createTestData(avroSchema: String): DataFrame = {
    val testInts = Seq(42, 66, 77, 321, 789) // scalastyle:ignore
    val rows = testInts.map(i => Row.fromSeq(Seq(i)))
    val rdd = spark.sparkContext.parallelize(rows, 2)

    val sparkSchema = SparkAvroConversions.toSqlType(avroSchema)

    spark.createDataFrame(rdd, sparkSchema)
  }

  "JsonSerDeSpec deserialize" should {

    "deserialize data with providing schema" in {
      val testData = createTestData(recordByteSchemaString)
      val dataFrame = testData
        .select(struct(col(testData.columns.head)) as 'integers)
        .withColumn("integers", to_json(col("integers")))

      val schemaFile = SchemaFile(path = "src/test/resources/databases/schema.avsc", store = "file")

      val jsonSerDe = new JsonSerDe()
        .setInputCol("integers")
        .setSchemaFile(schemaFile)

      val result = jsonSerDe
        .transform(dataFrame)
        .select(s"${Config.SERDE_TEMP_COL}.*")

      assertDataFrameEquals(testData, result)
    }

    "deserialize data with colsDefinition" in {
      val testData = createTestData(recordByteSchemaString)
        .withColumn("blah", lit("blaah"))
      val dataFrame = testData
        .select(struct(col(testData.columns.head), col("blah")) as 'integers)
        .withColumn("integers", to_json(col("integers")))

      val colsDefinition = Array(ColumnDefinition("int", "int"), ColumnDefinition("blah", "string"))
      val jsonSerDe = new JsonSerDe()
        .setInputCol("integers")
        .setColsDefinition(colsDefinition)
      val result = jsonSerDe
        .transform(dataFrame)
        .select(s"${Config.SERDE_TEMP_COL}.*")

      assertDataFrameEquals(testData, result)
    }
  }

  "JsonSerDeSpec serialize" should {

    "serialize data" in {
      val testData = createTestData(recordByteSchemaString)
        .withColumn("blah", lit("blaah"))
      val dataFrame = testData.select(struct(col(testData.columns.head), col("blah")) as 'integers)

      val jsonSerDe = new JsonSerDe()
        .setInputCol("integers")
        .setSerialize(true)
        .setOutputCol("integers")

      val result = jsonSerDe.transform(dataFrame)
      result.show(false)

      assert(result.schema.fields.head.dataType.toString == "StringType")
    }
  }
}
