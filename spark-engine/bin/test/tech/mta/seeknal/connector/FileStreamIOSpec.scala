package tech.mta.seeknal.connector

import java.io.File

import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfterAll
import tech.mta.seeknal.BaseSparkSpec
import tech.mta.seeknal.pipeline.SparkPipeline

class FileStreamIOSpec extends BaseSparkSpec with BeforeAndAfterAll {

  val fileDir = "build/integ-test/outputStream"
  val fileDirTwo = "build/integ-test/outputStreamTwo"

  override def beforeAll(): Unit = {
    super.beforeAll()

    /** File connector setup
      */
    val dir = new File(fileDir)
    if (!dir.isDirectory) {
      dir.mkdir()
    }
  }

  override def afterAll(): Unit = {

    /** Clean up for File connector
      */
    val dir = new File(fileDir)
    val dirTwo = new File(fileDirTwo)
    if (dir.isDirectory) {
      FileUtils.deleteDirectory(new File(fileDir))
    }
    if (dirTwo.isDirectory) {
      FileUtils.deleteDirectory(new File(fileDirTwo))
    }

    super.afterAll()
  }

  def runSparkPipeline(configFile: String): Unit = {
    SparkPipeline("pipeline-testing", Seq(configFile), spark).run()
  }

  "FileStreamIO" should {

    "read and write with json format files" in {
      val configPath = "src/test/resources/conf/file_stream.yml"
      runSparkPipeline(configPath)
      val result = spark.read.json(fileDir)
      val expected = spark.read
        .option("multiLine", true)
        .json("src/test/resources/databases/files")

      assertDataFrameEquals(expected, result)
    }

    "read and write with json format files and schema specified" in {
      val configPath = "src/test/resources/conf/file_stream_schema.yml"
      runSparkPipeline(configPath)
      val result = spark.read.json(fileDirTwo)
      val expected = spark.read
        .option("multiLine", true)
        .json("src/test/resources/databases/files")

      assertDataFrameEquals(expected, result)
    }
  }
}
