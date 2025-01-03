package tech.mta.seeknal.connector.extractor

import java.io.File

import scala.util.Random

import io.circe.Json
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField}
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, Ignore}
import org.scalatestplus.junit.JUnitRunner
import tech.mta.seeknal.{BaseSparkSpec, DataFrameBuilder}
import tech.mta.seeknal.connector.{IOMaterial, SourceConfig}
import tech.mta.seeknal.params.ColumnDefinition

@RunWith(classOf[JUnitRunner])
class FileSourceSpec extends BaseSparkSpec with BeforeAndAfterAll {

  val random = Math.abs(Random.nextInt(100))
  val checkPoint = s"build/checkpoint/$random"

  override def afterAll(): Unit = {

    /** Clean up for File connector
      */
    val dir = new File(checkPoint)
    if (dir.isDirectory) {
      FileUtils.deleteDirectory(new File(checkPoint))
    }
  }

  // TODO: write more tests
  "FileSourceSpec" should {

    val params = Json
      .obj(("path", Json.fromString("src/test/resources/databases/multi_line.json")),
           ("format", Json.fromString("json")),
           ("multiLine", Json.fromBoolean(true)),
           ("numPartition", Json.fromBigInt(10))
      )
      .asObject

    val streamParams = Json
      .obj(("path", Json.fromString("src/test/resources/databases/files/")),
           ("format", Json.fromString("json")),
           ("multiLine", Json.fromBoolean(true))
      )
      .asObject

    val inputConfig = SourceConfig(source = Some("file"), params = params)

    "fetchConfigParams gives correct value" in {
      val extractor = IOMaterial(spark, inputConfig)
      val fileSource = new FileSource(extractor)
      assert(fileSource.getFormat == "json")
      assert(fileSource.getFilePath == "src/test/resources/databases/multi_line.json")
      assert(fileSource.fetchConfigParams("numPartition", params.get.toMap).get == "10")
    }

    "read file data from absolute path" in {
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

      val extractor = IOMaterial(spark, inputConfig)
      val fileSource = new FileSource(extractor)
      val output = fileSource
        .get()
        .select(expected.columns.head, expected.columns.tail: _*)

      val columnCount = output.columns.length
      val rowCount = output.count()
      assert(columnCount == 7)
      assert(rowCount == 2)

      assertDataFrameEquals(expected, output)
    }

    "read file data from relative path" in {
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

      val newParams = Json
        .obj(("relPath", Json.fromString("src/test/resources/databases/multi_line.json")),
             ("store", Json.fromString("file")),
             ("format", Json.fromString("json")),
             ("multiLine", Json.fromBoolean(true)),
             ("numPartition", Json.fromBigInt(10))
        )
        .asObject

      val inputConfig = SourceConfig(source = Some("file"), params = newParams)
      val extractor = IOMaterial(spark, inputConfig)
      val fileSource = new FileSource(extractor)
      val output = fileSource
        .get()
        .select(expected.columns.head, expected.columns.tail: _*)

      val columnCount = output.columns.length
      val rowCount = output.count()
      assert(columnCount == 7)
      assert(rowCount == 2)

      assertDataFrameEquals(expected, output)
    }

    "read file with streaming" in {
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

      val inputConfig = SourceConfig(source = Some("file"), params = streamParams)
      val extractor = IOMaterial(spark, inputConfig)
      val fileSource = new FileSource(extractor)
      val output = fileSource.getStream()

      val streamQuery = output.writeStream
        .queryName("file_test_data")
        .format("memory")
        .start()

      streamQuery
        .processAllAvailable()

      val result = spark.sql("select * from file_test_data")
      assertDataFrameEquals(expected, result)
    }

    "read file with streaming with schema specified" in {
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

      val streamParams = Json
        .obj(("path", Json.fromString("src/test/resources/databases/files/")),
             ("format", Json.fromString("json")),
             ("multiLine", Json.fromBoolean(true)),
             ("schemaInference", Json.fromBoolean(false))
        )
        .asObject

      val colsDefinition = Seq(ColumnDefinition("deptno", "string"),
                               ColumnDefinition("designation", "string"),
                               ColumnDefinition("empno", "string"),
                               ColumnDefinition("ename", "string"),
                               ColumnDefinition("hire_date", "string"),
                               ColumnDefinition("manager", "string"),
                               ColumnDefinition("sal", "string")
      )
      val inputConfig = SourceConfig(source = Some("file"), params = streamParams, schema = Some(colsDefinition))
      val extractor = IOMaterial(spark, inputConfig)
      val fileSource = new FileSource(extractor)
      val output = fileSource.getStream()

      val streamQuery = output.writeStream
        .queryName("file_test_data_two")
        .format("memory")
        .start()

      streamQuery
        .processAllAvailable()

      val result = spark.sql("select * from file_test_data_two")
      assertDataFrameEquals(expected, result)
    }
  }
}
