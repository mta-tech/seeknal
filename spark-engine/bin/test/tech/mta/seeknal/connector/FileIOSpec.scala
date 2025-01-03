package tech.mta.seeknal.connector

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.scalatest.BeforeAndAfterAll
import tech.mta.seeknal.BaseSparkSpec
import tech.mta.seeknal.pipeline.SparkPipeline

class FileIOSpec extends BaseSparkSpec with BeforeAndAfterAll {

  val fileDir = "build/integ-test/output"

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
    if (dir.isDirectory) {
      FileUtils.deleteDirectory(new File(fileDir))
    }

    super.afterAll()
  }

  def runSparkPipeline(configFile: String): Unit = {
    SparkPipeline("pipeline-testing", Seq(configFile), spark).run()
  }

  "FileIO" should {

    "read and write with csv format files" in {
      val configPath = "src/test/resources/conf/file_csv_test.yml"
      runSparkPipeline(configPath)

      val input = spark.read.csv("src/test/resources/databases/db.csv")
      val expected = spark.read.csv(new Path(fileDir, "csv_output").toString)
      assertDataFrameEquals(input, expected)
    }

    "read and write with tsv format files" in {
      val configPath = "src/test/resources/conf/file_tsv_test.yml"
      runSparkPipeline(configPath)

      val input = spark.read
        .option("delimiter", "\t")
        .csv("src/test/resources/databases/db.tsv")
      val expected = spark.read
        .option("delimiter", "\t")
        .csv(new Path(fileDir, "tsv-to-csv-output").toString)
      assertDataFrameEquals(input, expected)
    }

    "read and write with json format files" in {
      val configPath = "src/test/resources/conf/file_single_line_json.yml"
      runSparkPipeline(configPath)

      val input =
        spark.read.json("src/test/resources/databases/single_line.json")
      val expected = spark.read.json(new Path(fileDir, "json_output").toString)
      assertDataFrameEquals(input, expected)

      val configPathTwo = "src/test/resources/conf/file_multi_line_json.yml"
      runSparkPipeline(configPathTwo)

      val inputTwo = spark.read
        .option("multiLine", value = true)
        .json("src/test/resources/databases/multi_line.json")
      val expectedTwo = spark.read
        .json(new Path(fileDir, "multi-line-json-output").toString)
      assertDataFrameEquals(inputTwo, expectedTwo)
    }

    "read and write with parquet files" in {
      val configPath = "src/test/resources/conf/file_parquet_test.yml"
      runSparkPipeline(configPath)

      val input = spark.read.parquet("src/test/resources/databases/db.parquet")
      val expected =
        spark.read.parquet(new Path(fileDir, "parquet-to-csv-output").toString)
      assertDataFrameEquals(input, expected)
    }

    "read and write with limit" in {
      val configPath = "src/test/resources/conf/file_limit_test.yml"
      runSparkPipeline(configPath)

      val expected = spark.read.csv(new Path(fileDir, "file_limit_test").toString)
      assert(expected.count() === 4)
    }
  }
}
