package tech.mta.seeknal.connector.loader

import java.io.File

import io.circe.Json
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.asc
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, StructField}
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, Ignore}
import org.scalatestplus.junit.JUnitRunner
import tech.mta.seeknal.{BaseSparkSpec, DataFrameBuilder}
import tech.mta.seeknal.connector.{IOMaterial, SourceConfig}
import tech.mta.seeknal.connector.extractor.FileSource

@Ignore
@RunWith(classOf[JUnitRunner])
class FileLoaderSpec extends BaseSparkSpec with BeforeAndAfterAll {

  val outputDir = "build/integ-test/output"
  val format = "json"
  val outputDirStream = "build/integ-test/outputStream"

  override def beforeAll() {

    super.beforeAll()

    val dir = new File(outputDir)
    // attempt to create the directory
    if (!dir.isDirectory) {
      val successful = dir.mkdir()
    }

  }

  override def afterAll() {
    val dir = new File(outputDir)
    val dirTwo = new File(outputDirStream)
    if (dir.isDirectory) {
      FileUtils.deleteDirectory(new File(outputDir))
    }
    if (dirTwo.isDirectory) {
      FileUtils.deleteDirectory(new File(outputDirStream))
    }
    super.afterAll()
  }

  // TODO: write more tests
  "file loader pipeline" should {

    val filePath = outputDir + "/" + "test.json"

    val inputBuilder = DataFrameBuilder(Seq(Row("1", "a"), Row("2", "b")),
                                        Seq(StructField("id", StringType), StructField("column", StringType))
    )

    val params = Json
      .obj(("path", Json.fromString(filePath)),
           ("format", Json.fromString(format)),
           ("multiLine", Json.fromBoolean(true)),
           ("numPartition", Json.fromBigInt(10))
      )
      .asObject

    val output =
      SourceConfig(id = Some("id"), source = Some("file"), table = Some("table"), path = Some("path"), params = params)

    "return parameters correctly" in {
      val loader = IOMaterial(spark, output, Some(inputBuilder.build()))
      val fileLoader = new FileLoader(loader)
      assert(
        fileLoader
          .fetchConfigParams("numPartition", params.get.toMap)
          .get == "10"
      )
      assert(fileLoader.getFilePath == filePath)
      assert(fileLoader.getFormat == format)
      assert(fileLoader.getMode == "overwrite")

      val options = fileLoader.getOptionsParams
      assert(options("path") == filePath)
      assert(options("multiLine") == "true")
      assert(options("numPartition") == "10")
    }

    "save the df to files" in {
      val loader = IOMaterial(spark, output, Some(inputBuilder.build()))
      val fileLoader = new FileLoader(loader)
      fileLoader.save()
      val outputDF = spark.read
        .json(filePath)
        .select("id", "column")
        .orderBy(asc("id"))

      assertDataFrameEquals(inputBuilder.build(), outputDF)
    }

    "save in partition" in {
      val filePath = outputDir + "/" + "test_partition.json"
      val params = Json
        .obj(("path", Json.fromString(filePath)),
             ("format", Json.fromString(format)),
             ("multiLine", Json.fromBoolean(true))
        )
        .asObject
      val sourceConfig =
        SourceConfig(id = Some("id"), source = Some("file"), params = params, partitions = Some(Seq("column")))

      val loader = IOMaterial(spark, sourceConfig, Some(inputBuilder.build()))
      val fileLoader = new FileLoader(loader)
      fileLoader.save()
      val outputDF = spark.read
        .json(filePath)
      assert(outputDF.rdd.partitions.size == 2)

    }

    "use relative path" in {
      val params = Json
        .obj(("relPath", Json.fromString(s"${filePath}/../../another/test.json")), ("store", Json.fromString("file")))
        .asObject

      val output = SourceConfig(id = Some("id"),
                                source = Some("file"),
                                table = Some("table"),
                                path = Some("path"),
                                params = params
      )

      val loader = IOMaterial(spark, output, Some(inputBuilder.build()))
      val fileLoader = new FileLoader(loader)
      val relPath = s"file://" + (os.pwd / os.RelPath("build/integ-test/another/test.json")).toString()
      assert(fileLoader.getFilePath == relPath)
    }

    "save the df to files in streaming" in {

      val expected = DataFrameBuilder(Seq(Row("20", "CLERK", "7369", "SMITH", "12/17/1980", "7902", "800"),
                                          Row("30", "SALESMAN", "7499", "ALLEN", "2/20/1981", "7698", "1600")
                                      ),
                                      Seq(StructField("deptno", StringType),
                                          StructField("designation", StringType),
                                          StructField("empno", StringType),
                                          StructField("ename", StringType),
                                          StructField("hire_date", StringType),
                                          StructField("manager", StringType),
                                          StructField("sal", StringType)
                                      )
      ).build()

      val sourceStreamParams = Json
        .obj(("path", Json.fromString("src/test/resources/databases/files/")),
             ("format", Json.fromString(format)),
             ("multiLine", Json.fromBoolean(true))
        )
        .asObject

      val sinkStreamParams = Json
        .obj(("path", Json.fromString(outputDirStream)), ("format", Json.fromString(format)))
        .asObject

      val inputConfig = SourceConfig(source = Some("file"), params = sourceStreamParams)
      val outputConfig = SourceConfig(source = Some("file"), params = sinkStreamParams)

      val extractor = IOMaterial(spark, inputConfig)
      val fileExtractor = new FileSource(extractor)
      val inputStream = fileExtractor.getStream()

      val loader = IOMaterial(spark, outputConfig, Some(inputStream))
      val fileLoader = new FileLoader(loader)
      val streamQuery = fileLoader
        .saveStream()
        .trigger(Trigger.Once())
        .start()

      streamQuery.awaitTermination()

      val result = spark.read.json(outputDirStream)
      assertDataFrameEquals(expected, result)
    }
  }
}
