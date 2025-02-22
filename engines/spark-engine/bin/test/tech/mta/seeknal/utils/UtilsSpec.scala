package tech.mta.seeknal.utils

import org.apache.hadoop.fs.{FileSystem, Path}
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatestplus.junit.JUnitRunner
import org.scalatest.matchers.should.Matchers._
import tech.mta.seeknal.BaseSparkSpec

@RunWith(classOf[JUnitRunner])
class UtilsSpec extends BaseSparkSpec with BeforeAndAfterAll {

  val dbName = "utilTest"
  val outputDir = new Path("build/utilSpec")
  val tableName = s"$dbName.input"

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName").collect()
  }

  override def afterAll(): Unit = {
    spark.sql(s"DROP DATABASE IF EXISTS $dbName CASCADE").collect()
    val fs = getFs
    if (fs.exists(outputDir)) {
      fs.delete(outputDir, true)
    }
    super.afterAll()
  }

  def getFs: FileSystem = {
    outputDir.getFileSystem(baseSparkSession.sparkContext.hadoopConfiguration)
  }

  "formatDate" should {
    "reformat date pattern to new one" in {
      val res = Utils.formatDate("20180715", "yyyyMMdd", "yyyy-MM-dd")
      assertTrue(res === "2018-07-15")
    }
  }

  "parseRangeOfDates" should {
    "parse 'startDate endDate' to seq of dates from startDate to endDate" in {
      val rangeDates = "20190101 20190105"
      val inputPattern = "yyyyMMdd"

      val seqDates = Utils.parseRangeDate(rangeDates, inputPattern)
      assertTrue(Seq("20190101", "20190102", "20190103", "20190104", "20190105") === seqDates)

      val feDayRangeDates = "2019-01-01 2019-01-05"
      val feDayInputPattern = "yyyy-MM-dd"

      val feDaySeqDates =
        Utils.parseRangeDate(feDayRangeDates, feDayInputPattern)
      assertTrue(
        Seq("2019-01-01", "2019-01-02", "2019-01-03", "2019-01-04", "2019-01-05")
          === feDaySeqDates
      )

      val abitraryDate = "2019070112"
      val abitrarySeqDate = Utils.parseRangeDate(abitraryDate, "yyyyMMddHH")
      assertTrue(Seq("2019070112") === abitrarySeqDate)
    }
  }

  "trimDatePattern" should {
    "trim specified datePattern from length inputted date string" in {
      val trimmedDatePattern = Utils.trimDatePattern("2019-05", "yyyyMMdd")
      assertTrue(trimmedDatePattern === "yyyyMM")
    }
  }

  "replaceStringParams" should {
    "use list of inline params to replace string of '{}' in config yaml if any" in {

      val inlineParams: Map[String, Either[String, Map[Boolean, Seq[String]]]] =
        Map("table" -> Left("abc"),
            "date" -> Left("20190501"),
            "applications" -> Right(Map(true -> Seq("fb", "tiktok", "tw", "ig"))),
            "hits" -> Right(Map(false -> Seq("1", "10")))
        )

      val yamlString =
        """---
          |pipeline:
          |  input:
          |    id: traffic_day
          |
          |  output:
          |    id: feateng_random_day
          |  stages:
          |    - className: org.apache.spark.ml.feature.SQLTransformer
          |      params:
          |        statement: CREATE OR REPLACE VIEW {table} as SELECT msisdn as id, "dpi" as cat,
          |        date_id as day FROM feateng_integtest.db_dpi_hourly
          |        where date_id = "{date}" and application in ({applications})
          |        and hit in ({hits})
          |    - className: org.apache.spark.ml.feature.SQLTransformer
          |      params:
          |        statement: SELECT * FROM {table}
          |
          |feature:
          |  name: random
          |  frequency: day
          |            """.stripMargin

      val expectedYamlString =
        """---
          |pipeline:
          |  input:
          |    id: traffic_day
          |
          |  output:
          |    id: feateng_random_day
          |  stages:
          |    - className: org.apache.spark.ml.feature.SQLTransformer
          |      params:
          |        statement: CREATE OR REPLACE VIEW abc as SELECT msisdn as id, "dpi" as cat,
          |        date_id as day FROM feateng_integtest.db_dpi_hourly
          |        where date_id = "20190501" and application in ('fb','tiktok','tw','ig')
          |        and hit in (1,10)
          |    - className: org.apache.spark.ml.feature.SQLTransformer
          |      params:
          |        statement: SELECT * FROM abc
          |
          |feature:
          |  name: random
          |  frequency: day
          |            """.stripMargin

      val replacedConfig = Utils.replaceStringParams(inlineParams, yamlString)

      replacedConfig shouldBe expectedYamlString
    }
  }
}
