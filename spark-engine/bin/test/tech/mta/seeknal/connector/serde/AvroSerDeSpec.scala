package tech.mta.seeknal.connector.serde

import org.apache.avro.Schema
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, struct}
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatestplus.junit.JUnitRunner
import tech.mta.seeknal.BaseSparkSpec
import za.co.absa.abris.avro.format.SparkAvroConversions
import za.co.absa.abris.avro.functions.to_avro
import za.co.absa.abris.avro.read.confluent.SchemaManagerFactory
import za.co.absa.abris.avro.registry.{ConfluentMockRegistryClient, SchemaSubject}
import za.co.absa.abris.config.AbrisConfig
import tech.mta.seeknal.params.Config.SCHEMA_REGISTRY_URL
import tech.mta.seeknal.params.SchemaFile

@RunWith(classOf[JUnitRunner])
class AvroSerDeSpec extends BaseSparkSpec with BeforeAndAfterAll {

  private val dummyUrl = "mock://dummyUrl"
  private val schemaRegistryConfig = Map(SCHEMA_REGISTRY_URL -> dummyUrl,
    AbrisConfig.REGISTRY_CLIENT_CLASS -> "za.co.absa.abris.avro.registry.ConfluentMockRegistryClient")
  private val topic = "dummyTopic"
  val mockedSchemaRegistryClient = new ConfluentMockRegistryClient()

  private val recordByteSchemaString = """{
        "namespace": "all-types.test",
        "type": "record",
        "name": "record_name",
        "fields":[
        {"name": "int", "type":  ["int", "null"] }
        ]
      }"""

  override def beforeAll() {
    super.beforeAll()
    val parser = new Schema.Parser()
    val schema = parser.parse(recordByteSchemaString)
    SchemaManagerFactory.create(schemaRegistryConfig)
      .register(new SchemaSubject(topic), schema)
  }

  override def afterAll() {

    super.afterAll()
  }

  private def createTestData(avroSchema: String): DataFrame = {
    val testInts = Seq(42, 66, 77, 321, 789) // scalastyle:ignore
    val rows = testInts.map(i => Row.fromSeq(Seq(i)))
    val rdd = spark.sparkContext.parallelize(rows, 2)

    val sparkSchema = SparkAvroConversions.toSqlType(avroSchema)

    spark.createDataFrame(rdd, sparkSchema)
  }

  private def createTestDataTwo(avroSchema: String): DataFrame = {
    val testInts = Seq(42, 66) // scalastyle:ignore
    val rows = testInts.map(i => Row.fromSeq(Seq(i)))
    val rdd = spark.sparkContext.parallelize(rows, 1)

    val sparkSchema = SparkAvroConversions.toSqlType(avroSchema)

    spark.createDataFrame(rdd, sparkSchema)
  }


  "AvroSerDeSpec deserialization" should {

    "deserialize bytes confluentAvro with providing schema" in {
      val allData = createTestData(recordByteSchemaString).orderBy("int")
      val dataFrame = allData.select(struct(col(allData.columns.head)) as 'integers)

      val toCAConfig = AbrisConfig
        .toConfluentAvro
        .provideAndRegisterSchema(recordByteSchemaString)
        .usingTopicNameStrategy(topic)
        .usingSchemaRegistry(dummyUrl)
      val avroBytes = dataFrame
        .select(to_avro(col("integers"), toCAConfig) as "avroBytes")

      val avroSerDe = new AvroSerDe()
        .setInputCol("avroBytes")
        .setTopicName(topic)
        .setSchemaRegistryUrl(dummyUrl)
        .setOutputCol("_parsed")

      val result = avroSerDe.transform(avroBytes).select("_parsed.*").orderBy("int")

      assertDataFrameEquals(allData, result)
    }

    "deserialize bytes confluentAvro with providing schema without schema registry" in {
      val allData = createTestData(recordByteSchemaString).orderBy("int")
      val dataFrame = allData.select(struct(col(allData.columns.head)) as 'integers)

      val toCAConfig = AbrisConfig
        .toConfluentAvro
        .provideAndRegisterSchema(recordByteSchemaString)
        .usingTopicNameStrategy(topic)
        .usingSchemaRegistry(dummyUrl)

      val avroBytes = dataFrame
        .select(to_avro(col("integers"), toCAConfig) as "avroBytes")

      val schemaFile = SchemaFile(path = "src/test/resources/databases/schema.avsc", store = "file")
      val avroSerDe = new AvroSerDe()
        .setInputCol("avroBytes")
        .setSchemaFile(schemaFile)
        .setTopicName(topic)
        .setOutputCol("_parsed")

      val result = avroSerDe.transform(avroBytes).select("_parsed.*").orderBy("int")

      assertDataFrameEquals(allData, result)
    }

    "deserialize bytes from confluentAvro with schema id" in {
      val allData = createTestData(recordByteSchemaString).orderBy("int")
      val dataFrame = allData.select(struct(col(allData.columns.head)) as 'integers)

      val toCAConfig = AbrisConfig
        .toConfluentAvro
        .provideAndRegisterSchema(recordByteSchemaString)
        .usingTopicNameStrategy(topic)
        .usingSchemaRegistry(schemaRegistryConfig)

      val avroBytes = dataFrame
        .select(to_avro(col("integers"), toCAConfig) as "avroBytes")

      val avroSerDe = new AvroSerDe()
        .setInputCol("avroBytes")
        .setSchemaId("1")
        .setStrategyMethod("topicNameStrategy")
        .setSchemaRegistryUrl(dummyUrl)
        .setOutputCol("_parsed")

      val result = avroSerDe.transform(avroBytes).select("_parsed.*").orderBy("int")

      assertDataFrameEquals(allData, result)
    }

    "deserialize bytes from confluentAvro with schema version" in {
      val allData = createTestData(recordByteSchemaString).orderBy("int")
      val dataFrame = allData.select(struct(col(allData.columns.head)) as 'integers)

      val toCAConfig = AbrisConfig
        .toConfluentAvro
        .provideAndRegisterSchema(recordByteSchemaString)
        .usingTopicNameStrategy(topic)
        .usingSchemaRegistry(schemaRegistryConfig)
      val avroBytes = dataFrame
        .select(to_avro(col("integers"), toCAConfig) as "avroBytes")

      val avroSerDe = new AvroSerDe()
        .setInputCol("avroBytes")
        .setSchemaVersion("1")
        .setStrategyMethod("topicNameStrategy")
        .setTopicName(topic)
        .setSchemaRegistryUrl(dummyUrl)
        .setOutputCol("_parsed")

      val result = avroSerDe.transform(avroBytes).select("_parsed.*").orderBy("int")

      assertDataFrameEquals(allData, result)
    }

    "deserialize bytes from confluentAvro with latest schema" in {
      val allData = createTestData(recordByteSchemaString).orderBy("int")

      val dataFrame = allData.select(struct(col(allData.columns.head)) as 'integers)

      val toCAConfig = AbrisConfig
        .toConfluentAvro
        .provideAndRegisterSchema(recordByteSchemaString)
        .usingTopicNameStrategy(topic)
        .usingSchemaRegistry(schemaRegistryConfig)
      val avroBytes = dataFrame
        .select(to_avro(col("integers"), toCAConfig) as "avroBytes")

      val avroSerDe = new AvroSerDe()
        .setInputCol("avroBytes")
        .setStrategyMethod("topicNameStrategy")
        .setTopicName(topic)
        .setSchemaRegistryUrl(dummyUrl)
        .setOutputCol("_parsed")

      val result = avroSerDe.transform(avroBytes).select("_parsed.*").orderBy("int")

      assertDataFrameEquals(allData, result)
    }

    "deserialize bytes from simpleAvro with providing schema" in {
      val allData = createTestData(recordByteSchemaString).orderBy("int")

      val dataFrame = allData.select(struct(col(allData.columns.head)) as 'integers)

        val avroBytes = dataFrame
          .select(to_avro(col("integers"), recordByteSchemaString) as "avroBytes")

      val schemaFile = SchemaFile(path = "src/test/resources/databases/schema.avsc", store = "file")
      val avroSerDe = new AvroSerDe()
        .setAvroType("simpleAvro")
        .setInputCol("avroBytes")
        .setSchemaFile(schemaFile)
        .setOutputCol("_parsed")

      val result = avroSerDe.transform(avroBytes).select("_parsed.*").orderBy("int")

      assertDataFrameEquals(allData, result)
    }

    "deserialize bytes from simpleAvro with schema id" in {

      val allData = createTestData(recordByteSchemaString).orderBy("int")
      val dataFrame = allData.select(struct(col(allData.columns.head)) as 'integers)

      val toCAConfig = AbrisConfig
        .toSimpleAvro
        .provideAndRegisterSchema(recordByteSchemaString)
        .usingTopicNameStrategy(topic)
        .usingSchemaRegistry(dummyUrl)
      val avroBytes = dataFrame
        .select(to_avro(col("integers"), toCAConfig) as "avroBytes")

      val avroSerDe = new AvroSerDe()
        .setAvroType("simpleAvro")
        .setInputCol("avroBytes")
        .setSchemaId("1")
        .setStrategyMethod("topicNameStrategy")
        .setSchemaRegistryUrl(dummyUrl)
        .setOutputCol("_parsed")

      val result = avroSerDe.transform(avroBytes).select("_parsed.*").orderBy("int")

      assertDataFrameEquals(allData, result)
    }

    "deserialize bytes from simpleAvro with schema version" in {
      val allData = createTestData(recordByteSchemaString).orderBy("int")
      val dataFrame = allData.select(struct(col(allData.columns.head)) as 'integers)

      val toCAConfig = AbrisConfig
        .toSimpleAvro
        .provideAndRegisterSchema(recordByteSchemaString)
        .usingTopicNameStrategy(topic)
        .usingSchemaRegistry(dummyUrl)
      val avroBytes = dataFrame
        .select(to_avro(col("integers"), toCAConfig) as "avroBytes")

      val avroSerDe = new AvroSerDe()
        .setAvroType("simpleAvro")
        .setInputCol("avroBytes")
        .setSchemaVersion("1")
        .setStrategyMethod("topicNameStrategy")
        .setTopicName(topic)
        .setSchemaRegistryUrl(dummyUrl)
        .setOutputCol("_parsed")

      val result = avroSerDe.transform(avroBytes).select("_parsed.*").orderBy("int")

      assertDataFrameEquals(allData, result)
    }

    "deserialize bytes from simpleAvro with latest schema" in {
      val allData = createTestData(recordByteSchemaString).orderBy("int")
      val dataFrame = allData.select(struct(col(allData.columns.head)) as 'integers)

      val toCAConfig = AbrisConfig
        .toSimpleAvro
        .provideAndRegisterSchema(recordByteSchemaString)
        .usingTopicNameStrategy(topic)
        .usingSchemaRegistry(dummyUrl)
      val avroBytes = dataFrame
        .select(to_avro(col("integers"), toCAConfig) as "avroBytes")

      val avroSerDe = new AvroSerDe()
        .setAvroType("simpleAvro")
        .setInputCol("avroBytes")
        .setStrategyMethod("topicNameStrategy")
        .setTopicName(topic)
        .setSchemaRegistryUrl(dummyUrl)
        .setOutputCol("_parsed")

      val result = avroSerDe.transform(avroBytes).select("_parsed.*").orderBy("int")

      assertDataFrameEquals(allData, result)
    }

  }

  "AvroSerDeSpec serialization" should {

    "serialize bytes confluentAvro with providing schema" in {
      val allData = createTestData(recordByteSchemaString).orderBy("int")
      val dataFrame = allData.select(struct(col(allData.columns.head)) as 'integers)

      val toCAConfig = AbrisConfig
        .toConfluentAvro
        .provideAndRegisterSchema(recordByteSchemaString)
        .usingTopicNameStrategy(topic)
        .usingSchemaRegistry(dummyUrl)
      val avroBytes = dataFrame
        .select(to_avro(col("integers"), toCAConfig) as "avroBytes")

      val schemaFile = SchemaFile(path = "src/test/resources/databases/schema.avsc", store = "file")
      val avroSerDe = new AvroSerDe()
        .setInputCol("integers")
        .setSchemaFile(schemaFile)
        .setTopicName(topic)
        .setSchemaRegistryUrl(dummyUrl)
        .setSerialize(true)
        .setOutputCol("avroBytes")

      val result = avroSerDe.transform(dataFrame)
          .select("avroBytes")

      assertDataFrameEquals(avroBytes, result)
    }

    "test serialize" in {
      val allData = createTestData(recordByteSchemaString).orderBy("int")
      val dataFrame = allData.select(struct(col(allData.columns.head)) as 'integers)
      val avroSerDe = new AvroSerDe()
        .setInputCol("integers")
        .setTopicName(topic)
        .setSchemaRegistryUrl(dummyUrl)
        .setSerialize(true)
        .setOutputCol("avroBytes")
      val result = avroSerDe.transform(dataFrame)
        .select("avroBytes")
      result.show()
    }

    "serialize bytes confluentAvro with providing schema without schema registry" in {
      val allData = createTestData(recordByteSchemaString).orderBy("int")
      val dataFrame = allData.select(struct(col(allData.columns.head)) as 'integers)

      val toCAConfig = AbrisConfig
        .toConfluentAvro
        .provideAndRegisterSchema(recordByteSchemaString)
        .usingTopicNameStrategy(topic)
        .usingSchemaRegistry(dummyUrl)
      val avroBytes = dataFrame
        .select(to_avro(col("integers"), toCAConfig) as "avroBytes")

      val schemaFile = SchemaFile(path = "src/test/resources/databases/schema.avsc", store = "file")
      val avroSerDe = new AvroSerDe()
        .setInputCol("integers")
        .setSchemaFile(schemaFile)
        .setTopicName(topic)
        .setSerialize(true)
        .setOutputCol("avroBytes")

      val result = avroSerDe.transform(dataFrame)
        .select("avroBytes")

      assertDataFrameEquals(avroBytes, result)
    }

    "serialize bytes confluentAvro with schema id" in {

      val allData = createTestData(recordByteSchemaString).orderBy("int")
      val dataFrame = allData.select(struct(col(allData.columns.head)) as 'integers)

      val toCAConfig = AbrisConfig
        .toConfluentAvro
        .provideAndRegisterSchema(recordByteSchemaString)
        .usingTopicNameStrategy(topic)
        .usingSchemaRegistry(dummyUrl)
      val avroBytes = dataFrame
        .select(to_avro(col("integers"), toCAConfig) as "avroBytes")

      val avroSerDe = new AvroSerDe()
        .setInputCol("integers")
        .setSchemaId("1")
        .setTopicName(topic)
        .setSerialize(true)
        .setSchemaRegistryUrl(dummyUrl)
        .setOutputCol("avroBytes")

      val result = avroSerDe.transform(dataFrame)
        .select("avroBytes")

      assert(!result.head(1).isEmpty)
    }

    "serialize bytes confluentAvro with schema version" in {
      val allData = createTestData(recordByteSchemaString).orderBy("int")
      val dataFrame = allData.select(struct(col(allData.columns.head)) as 'integers)

      val toCAConfig = AbrisConfig
        .toConfluentAvro
        .provideAndRegisterSchema(recordByteSchemaString)
        .usingTopicNameStrategy(topic)
        .usingSchemaRegistry(dummyUrl)
      val avroBytes = dataFrame
        .select(to_avro(col("integers"), toCAConfig) as "avroBytes")

      val avroSerDe = new AvroSerDe()
        .setInputCol("integers")
        .setSchemaVersion("1")
        .setTopicName(topic)
        .setSerialize(true)
        .setSchemaRegistryUrl(dummyUrl)
        .setOutputCol("avroBytes")

      val result = avroSerDe.transform(dataFrame)
        .select("avroBytes")

      assert(!result.head(1).isEmpty)
    }

    "serialize bytes confluentAvro with latest schema" in {
      val allData = createTestData(recordByteSchemaString).orderBy("int")
      val dataFrame = allData.select(struct(col(allData.columns.head)) as 'integers)

      val toCAConfig = AbrisConfig
        .toConfluentAvro
        .provideAndRegisterSchema(recordByteSchemaString)
        .usingTopicNameStrategy(topic)
        .usingSchemaRegistry(dummyUrl)
      val avroBytes = dataFrame
        .select(to_avro(col("integers"), toCAConfig) as "avroBytes")

      val avroSerDe = new AvroSerDe()
        .setInputCol("integers")
        .setTopicName(topic)
        .setSerialize(true)
        .setSchemaRegistryUrl(dummyUrl)
        .setOutputCol("avroBytes")

      val result = avroSerDe.transform(dataFrame)
        .select("avroBytes")

      assert(!result.head(1).isEmpty)
    }

    "serialize bytes simpleAvro with latest schema" in {
      val allData = createTestData(recordByteSchemaString).orderBy("int")
      val dataFrame = allData.select(struct(col(allData.columns.head)) as 'integers)

      val avroSerDe = new AvroSerDe()
        .setAvroType("simpleAvro")
        .setInputCol("integers")
        .setTopicName(topic)
        .setSerialize(true)
        .setSchemaRegistryUrl(dummyUrl)
        .setOutputCol("avroBytes")

      val result = avroSerDe.transform(dataFrame)
        .select("avroBytes")

      assert(!result.head(1).isEmpty)
    }

    "serialize bytes confluentAvro with inferFromCol" in {
      val allData = createTestData(recordByteSchemaString).orderBy("int")
                    .withColumn("int_two", col("int"))
      val dataFrame = allData.select(struct(col("int"), col("int_two")) as 'integers)
      val newTopic = topic + "two"

      val avroSer = new AvroSerDe()
        .setInputCol("integers")
        .setTopicName(newTopic)
        .setInferFromCol(true)
        .setSerialize(true)
        .setSchemaRegistryUrl(dummyUrl)
        .setOutputCol("avroBytes")

      val result = avroSer.transform(dataFrame)
        .select("avroBytes")
      assert(!result.head(1).isEmpty)
    }

    "serialize bytes simpleAvro with providing schema" in {
      val allData = createTestData(recordByteSchemaString).orderBy("int")
      val dataFrame = allData.select(struct(col(allData.columns.head)) as 'integers)

      val avroBytes = dataFrame
        .select(to_avro(col("integers"), recordByteSchemaString) as "avroBytes")

      val schemaFile = SchemaFile(path = "src/test/resources/databases/schema.avsc", store = "file")
      val avroSerDe = new AvroSerDe()
        .setAvroType("simpleAvro")
        .setInputCol("integers")
        .setSchemaFile(schemaFile)
        .setTopicName(topic)
        .setSchemaRegistryUrl(dummyUrl)
        .setSerialize(true)
        .setOutputCol("avroBytes")

      val result = avroSerDe.transform(dataFrame)
        .select("avroBytes")

      assertDataFrameEquals(avroBytes, result)
    }

    "serialize bytes simpleAvro with schema id" in {
      val allData = createTestData(recordByteSchemaString).orderBy("int")
      val dataFrame = allData.select(struct(col(allData.columns.head)) as 'integers)

      val toCAConfig = AbrisConfig
        .toSimpleAvro
        .provideAndRegisterSchema(recordByteSchemaString)
        .usingTopicNameStrategy(topic)
        .usingSchemaRegistry(dummyUrl)
      val avroBytes = dataFrame
        .select(to_avro(col("integers"), toCAConfig) as "avroBytes")

      val avroSerDe = new AvroSerDe()
        .setAvroType("simpleAvro")
        .setInputCol("integers")
        .setSchemaId("1")
        .setSerialize(true)
        .setSchemaRegistryUrl(dummyUrl)
        .setOutputCol("avroBytes")

      val result = avroSerDe.transform(dataFrame)
        .select("avroBytes")

      assertDataFrameEquals(avroBytes, result)
    }

    "serialize bytes simpleAvro with schema version" in {
      val allData = createTestData(recordByteSchemaString).orderBy("int")
      val dataFrame = allData.select(struct(col(allData.columns.head)) as 'integers)

      val toCAConfig = AbrisConfig
        .toSimpleAvro
        .provideAndRegisterSchema(recordByteSchemaString)
        .usingTopicNameStrategy(topic)
        .usingSchemaRegistry(dummyUrl)
      val avroBytes = dataFrame
        .select(to_avro(col("integers"), toCAConfig) as "avroBytes")

      val avroSerDe = new AvroSerDe()
        .setAvroType("simpleAvro")
        .setInputCol("integers")
        .setSchemaVersion("1")
        .setTopicName(topic)
        .setSerialize(true)
        .setSchemaRegistryUrl(dummyUrl)
        .setOutputCol("avroBytes")

      val result = avroSerDe.transform(dataFrame)
        .select("avroBytes")

      assertDataFrameEquals(avroBytes, result)
    }

    "throw exception if avroType is not the one currently supported" in {
      val allData = createTestData(recordByteSchemaString).orderBy("int")

      val avroSerDe = new AvroSerDe()
        .setAvroType("blah")
        .setInputCol("integers")
        .setTopicName(topic)
        .setSerialize(true)
        .setSchemaRegistryUrl(dummyUrl)

      an [NotImplementedError] should be thrownBy avroSerDe.transform(allData)
    }
  }
}
