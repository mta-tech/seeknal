package tech.mta.seeknal.connector

import java.io.File
import io.circe.Json
import org.apache.avro.Schema
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, lit, struct}
import org.apache.spark.sql.types.{StringType, StructField}
import org.mockito.ArgumentCaptor
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.BeforeAndAfterAll
import org.scalatestplus.mockito.MockitoSugar
import za.co.absa.abris.avro.format.SparkAvroConversions
import za.co.absa.abris.avro.functions.to_avro
import za.co.absa.abris.avro.read.confluent.SchemaManagerFactory
import za.co.absa.abris.config.AbrisConfig
import tech.mta.seeknal.params.Config.SCHEMA_REGISTRY_URL
import za.co.absa.abris.avro.registry.AbrisRegistryClient
import org.scalatest.matchers.should.Matchers._
import tech.mta.seeknal.{BaseSparkSpec, DataFrameBuilder}
import tech.mta.seeknal.params.SerDeConfig
import tech.mta.seeknal.pipeline.DbConfig

class BatchIOSpec extends BaseSparkSpec with BeforeAndAfterAll with MockitoSugar {

  val fileDir = "build/integ-test/output"
  val avroFileDir = "src/test/resources/databases/confluentAvro.parquet"
  val parquetFileDir = "src/test/resources/databases/file.parquet"
  private val dummyUrl = "dummyUrl"
  private val schemaRegistryConfig = Map(SCHEMA_REGISTRY_URL -> dummyUrl)
  private val topic = "dummyTopic"

  private val recordByteSchemaString = """{
        "namespace": "all-types.test",
        "type": "record",
        "name": "record_name",
        "fields":[
        {"name": "int", "type":  ["int", "null"] }
        ]
      }"""

  override def beforeAll(): Unit = {
    super.beforeAll()
    val mockedSchemaRegistryClient = new MockSchemaRegistry()
    val parser = new Schema.Parser()
    val recordByteSchema = parser.parse(recordByteSchemaString)
    mockedSchemaRegistryClient.register(topic, recordByteSchema)

    SchemaManagerFactory
      .addSRClientInstance(schemaRegistryConfig, mockedSchemaRegistryClient.asInstanceOf[AbrisRegistryClient])
    /**
      * File connector setup
      */
    val dir = new File(fileDir)
    if (!dir.isDirectory) {
      dir.mkdir()
    }
  }

  override def afterAll(): Unit = {
    /**
      * Clean up for File connector
      */
    val dir = new File(fileDir)
    if (dir.isDirectory) {
      FileUtils.deleteDirectory(new File(fileDir))
    }

    super.afterAll()
  }

  private def createTestData(avroSchema: String): DataFrame = {
    val testInts = Seq(42, 66, 77, 321, 789) // scalastyle:ignore
    val rows = testInts.map(i => Row.fromSeq(Seq(i)))
    val rdd = spark.sparkContext.parallelize(rows, 2)

    val sparkSchema = SparkAvroConversions.toSqlType(avroSchema)

    spark.createDataFrame(rdd, sparkSchema)
  }

  "PipelineIO" should {

    val mockSession: SparkSession = mock[SparkSession]
    when(mockSession.table("table_name")).thenReturn(null)

    "returns requested hive table as default" in {

      val config = SourceConfig(
        Some("id"),
        table = Some("table_name"),
        db = Some(DbConfig("feateng", Some("/hive/feateng"))),
        path = Some("/some/path")
      )

      val setting = IOMaterial(mockSession, config)
      PipelineIO.get(setting) shouldBe null

      val configTwo = config.copy(source = Some("hive"))
      val settingTwo = IOMaterial(mockSession, configTwo)
      PipelineIO.get(settingTwo) shouldBe null

      val argument: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
      verify(mockSession, times(2)).table(argument.capture)
      argument.getAllValues.get(0) shouldBe "feateng.table_name"
      argument.getAllValues.get(1) shouldBe "feateng.table_name"
    }

    "read with limit" in {
      val inputParams = Json.obj(
        ("path", Json.fromString("src/test/resources/databases/db.csv"))
      ).asObject
      val config = SourceConfig(source = Some("file"), params = inputParams, limit = Some(5))
      val setting = IOMaterial(spark, config)
      val data = PipelineIO.get(setting)

      assert(setting.limit(data).count() === 5)
      assert(data.count() === 5)
    }

    "write with limit" in {
      val dummy = DataFrameBuilder(
        Seq(
          Row("1", "a"),
          Row("2", "b"),
          Row("3", "c"),
          Row("4", "d")),
        Seq(
          StructField("id", StringType),
          StructField("column", StringType)))
        .build()

      val outputParams = Json.obj(
        ("path", Json.fromString(fileDir)),
        ("format", Json.fromString("csv")),
        ("numPartition", Json.fromBigInt(1))
      ).asObject
      val config = SourceConfig(source = Some("file"), params = outputParams, limit = Some(3))
      val setting = IOMaterial(spark, config, Some(dummy))

      PipelineIO.save(setting)
      val writtenData = spark.read.csv(fileDir)
      assert(writtenData.count() === 3)

      // testing overriding size
      val configTwo = SourceConfig(source = Some("file"), params = outputParams, limit = Some(2))
      val settingTwo = IOMaterial(spark, configTwo, Some(dummy))

      val resizedData = settingTwo.limit(dummy)
      settingTwo.setResult(resizedData)

      assert(settingTwo.result.get.count() === 2)
    }

    "deserialize data" in {

      val expected = createTestData(recordByteSchemaString)
        .withColumn("abc", lit("blaaaah"))
        .orderBy("int")
        .select("abc", "int")

      val serdeParams = Json.obj(
        ("inputCol", Json.fromString("avroBytes")),
        ("schemaId", Json.fromString("1")),
        ("strategyMethod", Json.fromString("topicNameStrategy")),
        ("schemaRegistryUrl", Json.fromString("dummyUrl"))
      ).asObject

      val serde = SerDeConfig(
        className = "ai.eureka.featureengine.connector.serde.AvroSerDe",
        params = serdeParams,
        keepCols = Some(Seq("abc"))
      )

      val params = Json.obj(
        ("path", Json.fromString(avroFileDir)),
        ("format", Json.fromString("parquet"))
      ).asObject

      val config = SourceConfig(
        source = Some("file"),
        params = params,
        serde = Some(Seq(serde))
      )

      val setting = IOMaterial(spark, config)
      val result = PipelineIO.get(setting)

      assertDataFrameEquals(expected, result)
    }

    "serialize data" in {

      val serdeParams = Json.obj(
        ("inputCol", Json.fromString("integers")),
        ("schemaId", Json.fromString("1")),
        ("strategyMethod", Json.fromString("topicNameStrategy")),
        ("schemaRegistryUrl", Json.fromString("dummyUrl")),
        ("outputCol", Json.fromString("integers"))
      ).asObject

      val serde = SerDeConfig(
        className = "ai.eureka.featureengine.connector.serde.AvroSerDe",
        params = serdeParams
      )

      val params = Json.obj(
        ("path", Json.fromString(fileDir + "/serialized")),
        ("format", Json.fromString("parquet"))
      ).asObject

      val config = SourceConfig(
        source = Some("file"),
        params = params,
        serde = Some(Seq(serde))
      )

      val data = spark.read.parquet(parquetFileDir)
        .select(struct(col("int")) as "integers")

      val toCAConfig = AbrisConfig
        .toConfluentAvro
        .downloadSchemaById(1)
        .usingSchemaRegistry("dummyUrl")
      val expected = data
        .select(to_avro(col("integers"), toCAConfig) as "integers")

      val setting = IOMaterial(spark, config, Some(data))
      PipelineIO.save(setting)

      val res = spark.read.parquet(fileDir + "/serialized")
      assertDataFrameEquals(expected, res)
    }

    "debug streaming by set source to 'console'" in {
      val streamParams = Json.obj(
        ("path", Json.fromString("src/test/resources/databases/files/")),
        ("format", Json.fromString("json")),
        ("multiLine", Json.fromBoolean(true))
      ).asObject
      val inputConfig = SourceConfig(source = Some("file"), params = streamParams)
      val sourceSetting = IOMaterial(spark, inputConfig)
      val dataFrame = PipelineIO.getStream(sourceSetting)

      val outputConfig = SourceConfig(source = Some("console"))
      val sinkSetting = IOMaterial(spark, outputConfig, Some(dataFrame))

      val streamQuery = PipelineIO.saveStream(sinkSetting)

      streamQuery.processAllAvailable()
    }

    "read with repartition" in {
      val inputParams = Json.obj(
        ("path", Json.fromString("src/test/resources/databases/db.csv"))
      ).asObject
      val config = SourceConfig(source = Some("file"), params = inputParams, repartition = Some(5))
      val setting = IOMaterial(spark, config)
      val data = PipelineIO.get(setting)

      assert(data.rdd.getNumPartitions === 5)
    }

    "write with repartition" in {
      val dummy = DataFrameBuilder(
        Seq(
          Row("1", "a"),
          Row("2", "b"),
          Row("3", "c"),
          Row("4", "d")),
        Seq(
          StructField("id", StringType),
          StructField("column", StringType)))
        .build()

      val outputParams = Json.obj(
        ("path", Json.fromString(fileDir)),
        ("format", Json.fromString("csv"))
      ).asObject
      val config = SourceConfig(source = Some("file"), params = outputParams, repartition = Some(3))
      val setting = IOMaterial(spark, config, Some(dummy))

      PipelineIO.save(setting)
      val writtenData = spark.read.csv(fileDir)
      assert(writtenData.rdd.getNumPartitions === 3)
    }
  }
}
