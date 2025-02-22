package tech.mta.seeknal

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{AnalysisException, DataFrame, Row}
import org.apache.spark.sql.functions.{asc, col}
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, Ignore}
import org.scalatest.matchers.should.Matchers._
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SparkEngineSpec extends BaseSparkSpec with BeforeAndAfterAll {
  // these are specified in the yaml config file
  // TODO: create single source of truth for these
  val dbName = "integtest"
  val outputDir = "build/integtest-output"
  val commonConfigPath = "conf/common.yml"

  override def beforeAll() {
    super.beforeAll()

    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName").collect()

    // sample CDR data
    val cdrInput = DataFrameBuilder(
      Seq(Row("1234", "2345", "20180611", "Voice", "Outgoing", "9999", "OUTBOUND", 2, 2, null, null, null, null, null),
        Row("1234", "2345", "20180617", "Voice", "Outgoing", "9998", "OUTBOUND", 4, 4, null, null, null, null, null),
        Row("1234", "4567", "20180709", "Voice", "Outgoing", "9997", "LOCAL", 8, 8, null, null, null, null, null),
        Row("1234", "5678", "20180715", "Voice", "Outgoing", "9999", "LOCAL", 16, 16, null, null, null, null, null),
        Row("1234", "5678", "20180715", "Data", null, "9999", "OUTBOUND", null, 16, 10.0, 15.0, 25.0, "domain1", 7L),
        Row("1234", "2345", "20180715", "Voice", "Outgoing", "9998", "LOCAL", 16, 16, null, null, null, null, null),
        Row("1234", "2345", "20180715", "Data", null, "9998", "OUTBOUND", null, 16, 5.0, 30.0, 35.0, "domain1", 2L),
        Row("1234", "2345", "20180715", "Sms", "Outgoing", "9999", "OUTBOUND", 20, 20, null, null, null, null, null),
        Row("1234", "2345", "20180715", "Data", null, "9999", "LOCAL", null, 20, 10.0, 10.0, 20.0, "domain2", 3L),
        Row("5555",
          "1234",
          "20180715",
          "Voice",
          "Incoming",
          "9999",
          "OUTBOUND",
          12,
          12,
          null,
          null,
          null,
          null,
          null
        ),
        Row("5555",
          "1234",
          "20180715",
          "Voice",
          "Outgoing",
          "9999",
          "OUTBOUND",
          13,
          13,
          null,
          null,
          null,
          null,
          null
        ),
        Row("5555", "1234", "20180715", "Data", null, "9999", "OUTBOUND", null, 13, 20.0, 10.0, 30.0, "domain1", 1L),
        Row("5555", "3333", "20180715", "Sms", "Incoming", "9999", "LOCAL", 55, 55, null, null, null, null, null),
        Row("5555", "3333", "20180715", "Data", null, "9999", "LOCAL", null, 55, 6.0, 14.0, 20.0, "domain2", 5L),
        Row("5555", "4444", "20180715", "Sms", "Outgoing", "9999", "LOCAL", 19, 19, null, null, null, null, null),
        Row("5555", "4444", "20180715", "Data", null, "9999", "LOCAL", null, 19, 1.0, 2.0, 3.0, "domain1", 6L),
        // Adding user 5556 to test what happens if there's user with no call and SMS
        // currently all users without call / sms will be excluded from grouping
        // so there's no way to test the case for 0/0 ratio
        Row("5556", "4444", "20180715", "Sms", "Incoming", "9999", "LOCAL", 0, 0, null, null, null, null, null),
        Row("1234", "2345", "20180716", "Sms", "Incoming", "9999", "LOCAL", 24, 24, null, null, null, null, null),
        Row("1234", "2345", "20180716", "Voice", "Outgoing", "9998", "LOCAL", 40, 40, null, null, null, null, null),
        Row("1234", "2345", "20180716", "Data", null, "9998", "OUTBOUND", null, 40, 0.0, 3.0, 3.0, "domain1", 3L),
        Row("1234",
          "5555",
          "20180716",
          "Voice",
          "Incoming",
          "9997",
          "OUTBOUND",
          15,
          15,
          null,
          null,
          null,
          null,
          null
        ),
        Row("5555", "1234", "20180716", "Voice", "Incoming", "9999", "LOCAL", 12, 12, null, null, null, null, null),
        Row("5555", "1234", "20180716", "Data", null, "9998", "OUTBOUND", null, 13, 30.0, 0.0, 30.0, "domain1", 3L),
        Row("5555", "2345", "20180716", "Sms", "Incoming", "9997", "OUTBOUND", 30, 30, null, null, null, null, null),
        Row("5555", "2345", "20180716", "Sms", "Outgoing", "9999", "LOCAL", 12, 12, null, null, null, null, null),
        Row("5555", "2345", "20180817", "Sms", "Incoming", "9995", "LOCAL", 1, 1, null, null, null, null, null)
      ),
      Seq(StructField("msisdn", StringType),
        StructField("b_party", StringType),
        StructField("date_id", StringType),
        StructField("service_type", StringType),
        StructField("direction_type", StringType),
        StructField("site_id", StringType),
        StructField("roaming_position", StringType),
        StructField("duration", IntegerType),
        StructField("hit", IntegerType),
        StructField("downlink_volume", DoubleType),
        StructField("uplink_volume", DoubleType),
        StructField("volume", DoubleType),
        StructField("url_domain", StringType),
        StructField("total_hits", LongType)
      )
    ).build()
    val cdrTable = s"$dbName.db_traffic_hourly"
    cdrInput
      .write
      .mode("overwrite")
      .format("parquet")
      .saveAsTable(cdrTable)
  }

  override def afterAll() {
    // delete data directory
    val dir = new File(outputDir)
    if (dir.isDirectory) {
      spark.sql(s"DROP DATABASE IF EXISTS $dbName CASCADE").collect()

      FileUtils.deleteDirectory(new File(outputDir))
    }
    // FileUtils.deleteDirectory(new File("metastore_db"))
    super.afterAll()
  }

  def runFeatureEngine(configFile: String,
                       filter: Option[Map[String, String]] = None,
                       date: Seq[String] = Seq[String](),
                       datePattern: String = "yyyyMMdd",
                       maxResults: Int = 0
                      ): Unit = {
    val featureEngine = SparkEngine(Seq(JobSpec(Some(configFile), None)),
      None,
      maxResults,
      CommonObject(Some(commonConfigPath), None),
      date,
      datePattern
    )
    featureEngine.run()
  }

  def runFeatureEngineShow(configFile: String,
                           filter: Option[Map[String, String]] = None,
                           date: Seq[String] = Seq[String](),
                           datePattern: String = "yyyyMMdd",
                           maxResults: Int = 0
                          ): JobResult = {
    val featureEngine = SparkEngine(Seq(JobSpec(Some(configFile), None)),
      None,
      maxResults,
      CommonObject(Some(commonConfigPath), None),
      date,
      datePattern
    )
    featureEngine.transform()
  }

  def runPipeline(configFile: String,
                  filter: Option[Map[String, String]] = None,
                  date: Seq[String] = Seq[String](),
                  datePattern: String = "yyyyMMdd",
                  maxResults: Int = 0
                 ): Unit = {
    val sparkengine =
      SparkEngine(Seq(JobSpec(Some(configFile), None)), None, maxResults, CommonObject(), date, datePattern)
    sparkengine.run()
  }

  "spark engine" should {

    "run transformation with dataframe provided" in {
      val df = spark.read.table("integtest.db_traffic_hourly")
      val configStr =
        """
          |---
          |pipeline:
          |  stages:
          |    - className: tech.mta.seeknal.transformers.SQL
          |      params:
          |         statement: >-
          |           SELECT msisdn, b_party FROM __THIS__
          |
          |""".stripMargin
      val jobSpec = JobSpec(None, Some(configStr))
      val feat = SparkEngine(Seq(jobSpec), None, 0, CommonObject(Some(commonConfigPath), None),
        Seq("20180715"), "yyyyMMdd")
      val res = feat.transform(Some(df), "date_id")
      res.get().head.show()
    }
  }

}
