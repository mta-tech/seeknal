package tech.mta.seeknal.connector.serde

import org.apache.avro.Schema
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, struct}
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, Ignore}
import org.scalatestplus.junit.JUnitRunner
import tech.mta.seeknal.BaseSparkSpec
import za.co.absa.abris.avro.format.SparkAvroConversions
import za.co.absa.abris.avro.functions.to_avro
import za.co.absa.abris.avro.read.confluent.SchemaManagerFactory
import za.co.absa.abris.avro.registry.AbrisRegistryClient
import za.co.absa.abris.config.AbrisConfig
import tech.mta.seeknal.params.Config.{SCHEMA_REGISTRY_URL, SERDE_TEMP_COL}
import tech.mta.seeknal.connector.MockSchemaRegistry
import tech.mta.seeknal.params.SchemaFile

@Ignore
@RunWith(classOf[JUnitRunner])
class AvroBytesReadSerDeSpec extends BaseSparkSpec with BeforeAndAfterAll {

  private val dummyUrl = "dummyUrl"
  private val topic = "dummyTopic"
  private val schemaRegistryConfig = Map(SCHEMA_REGISTRY_URL -> dummyUrl)

  private val recordByteSchemaString = """{
        "namespace": "all-types.test",
        "type": "record",
        "name": "record_name",
        "fields":[
        {"name": "int", "type":  ["int", "null"] }
        ]
      }"""

  private val parser = new Schema.Parser()
  private val recordByteSchema = parser.parse(recordByteSchemaString)

  override def beforeAll() {
    super.beforeAll()
    val mockedSchemaRegistryClient = new MockSchemaRegistry()
    mockedSchemaRegistryClient.register(topic, recordByteSchema)

    SchemaManagerFactory
      .addSRClientInstance(schemaRegistryConfig, mockedSchemaRegistryClient.asInstanceOf[AbrisRegistryClient])
  }

  private def createTestData(avroSchema: String): DataFrame = {
    val testInts = Seq(42, 66, 77, 321, 789) // scalastyle:ignore
    val rows = testInts.map(i => Row.fromSeq(Seq(i)))
    val rdd = spark.sparkContext.parallelize(rows, 2)

    val sparkSchema = SparkAvroConversions.toSqlType(avroSchema)

    spark.createDataFrame(rdd, sparkSchema)
  }

  "AvroBytesReaderSerDeSpec deserialization" should {

    "deserialize avro with providing offset and schema" in {
      val allData = createTestData(recordByteSchemaString).orderBy("int")
      val dataFrame = allData.select(struct(col(allData.columns.head)) as 'integers)

      val toCAConfig = AbrisConfig.toConfluentAvro
        .provideAndRegisterSchema(recordByteSchemaString)
        .usingTopicNameStrategy(topic)
        .usingSchemaRegistry(dummyUrl)
      val avroBytes = dataFrame
        .select(to_avro(col("integers"), toCAConfig) as "integers")

      val schemaFile = SchemaFile(path = "src/test/resources/databases/schema.avsc", store = "file")

      val avroSerDe = new AvroBytesReadSerDe()
        .setInputCol("integers")
        .setTrimOffset(6)
        .setSchemaFile(schemaFile)

      val result = avroSerDe
        .transform(avroBytes)
        .select(s"${SERDE_TEMP_COL}.*")
      assertDataFrameEquals(allData, result)
    }
  }
}
