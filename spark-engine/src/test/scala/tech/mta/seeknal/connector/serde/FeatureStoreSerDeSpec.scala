package tech.mta.seeknal.connector.serde

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions.{col, lit}
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, Ignore}
import org.scalatestplus.junit.JUnitRunner
import tech.mta.seeknal.BaseSparkSpec
import tech.mta.seeknal.params.{FeatureGroup, FillNull}

@RunWith(classOf[JUnitRunner])
class FeatureStoreSerDeSpec extends BaseSparkSpec with BeforeAndAfterAll {

  val dbName = "feature_store"
  val commDayDir = "src/test/resources/databases/comm_day/"
  val outputDir = "build/integ-test"

  override def beforeAll() {
    super.beforeAll()

    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName").collect()

  }

  override def afterAll() {
    // delete data directory

    spark.sql(s"DROP DATABASE IF EXISTS $dbName CASCADE").collect()
    val dir = new File(outputDir)
    if (dir.isDirectory) {
      FileUtils.deleteDirectory(new File(outputDir))
    }

    new File("metastore_db").delete()
    super.afterAll()
  }

  // TODO: write more tests
  "FeatureStoreSerDe" should {

    "deserialize from offline store" in {
      val schemaValue =
        """
          |{"type":"record","name":"features","fields":[{"name":"comm_count_call_in","type":["long","null"]},{"name":"comm_count_call_out","type":["long","null"]},{"name":"comm_count_call_inout","type":["long","null"]},{"name":"comm_count_sms_in","type":["long","null"]},{"name":"comm_count_sms_out","type":["long","null"]},{"name":"comm_count_sms_inout","type":["long","null"]},{"name":"comm_count_callsms_in","type":["long","null"]},{"name":"comm_count_callsms_out","type":["long","null"]},{"name":"comm_count_callsms_inout","type":["long","null"]},{"name":"comm_roamingcount_call_in","type":["long","null"]},{"name":"comm_roamingcount_call_out","type":["long","null"]},{"name":"comm_roamingcount_call_inout","type":["long","null"]},{"name":"comm_roamingcount_sms_in","type":["long","null"]},{"name":"comm_roamingcount_sms_out","type":["long","null"]},{"name":"comm_roamingcount_sms_inout","type":["long","null"]},{"name":"comm_roamingcount_callsms_in","type":["long","null"]},{"name":"comm_roamingcount_callsms_out","type":["long","null"]},{"name":"comm_roamingcount_callsms_inout","type":["long","null"]},{"name":"comm_duration_call_in","type":["double","null"]},{"name":"comm_duration_call_out","type":["double","null"]},{"name":"comm_duration_call_inout","type":["double","null"]},{"name":"comm_callratio_callsms_in","type":["double","null"]},{"name":"comm_smsratio_callsms_in","type":["double","null"]},{"name":"comm_callratio_callsms_out","type":["double","null"]},{"name":"comm_smsratio_callsms_out","type":["double","null"]},{"name":"comm_callratio_callsms_inout","type":["double","null"]},{"name":"comm_smsratio_callsms_inout","type":["double","null"]},{"name":"comm_inratio_inout_call","type":["double","null"]},{"name":"comm_outratio_inout_call","type":["double","null"]},{"name":"comm_inratio_inout_sms","type":["double","null"]},{"name":"comm_outratio_inout_sms","type":["double","null"]},{"name":"comm_inratio_inout_callsms","type":["double","null"]},{"name":"comm_outratio_inout_callsms","type":["double","null"]}]}
          |""".stripMargin

      val schemaValueFinance =
        """
          |{"type":"record","name":"features","fields":[{"name":"finance_pre_spend_data","type":["double","null"], "default": 0.0},{"name":"finance_pre_spend_sms","type":["double","null"], "default": 0.0},{"name":"finance_pre_spend_call","type":["double","null"], "default": 0.0},{"name":"finance_pre_spend_vas","type":["double","null"], "default": 0.0},{"name":"finance_pre_spend_total","type":["double","null"], "default": 0.0}]}
          |""".stripMargin

      val commDay = spark.read.parquet("src/test/resources/databases/default_subscriber/")
        .drop("name").withColumn("name", lit("comm_day"))
      val communicationDay = spark.read.parquet("src/test/resources/databases/default_subscriber/")
        .drop("name").withColumn("name", lit("communication_day"))
      val financeDay = spark.read.parquet("src/test/resources/databases/default_subscriber_finance/")
      val offlineStore = commDay.unionByName(financeDay).unionByName(communicationDay)

      val deserialize = new FeatureStoreSerDe()
        .setProject("default")
        .setEntity("subscriber")
        .setKeyCols(Array("msisdn"))
        .setFeatureGroups(Array(
          FeatureGroup(
            name = "comm_day",
            schemaValueString = Some(schemaValue)
          ),
          FeatureGroup(
            name = "finance_day",
            schemaValueString = Some(schemaValueFinance)
          ),
          FeatureGroup(
            name = "communication_day",
            schemaValueString = Some(schemaValue)
          ),
        ))
        .setStartEventTime("2019-03-01")
        .setEndEventTime("2019-03-03")
        .setDropEventTime(false)
        .setFillNull(Array(
          FillNull(
            value = "0.0",
            dataType = "double"
          )
        ))
      val res = deserialize.transform(offlineStore)
      assert(res.head(1).isEmpty === false)
    }

    "serialize finance features" in {
      val financeDay = spark.read.parquet("src/test/resources/databases/spend_day")
      val fsSerDe = new FeatureStoreSerDe()
      .setEventTimeCol("day")
      .setDatePattern("yyyy-MM-dd")
      .setEntity("subscriber")
      .setProject("default")
      .setFeatureGroup("finance_day")
      .setKeyCols(Array("msisdn"))
      .setInferFromCol(true)
      .setSerialize(true)

      val res = fsSerDe.transform(financeDay)
      assert(res.head(1).isEmpty === false)
    }

    "serialize comm_day features" in {
      val commDay = spark.read.parquet("src/test/resources/databases/comm_day")
      val fsSerDe = new FeatureStoreSerDe()
        .setEventTimeCol("day")
        .setDatePattern("yyyy-MM-dd")
        .setEntity("subscriber")
        .setProject("default")
        .setFeatureGroup("comm_day")
        .setKeyCols(Array("msisdn"))
        .setInferFromCol(true)
        .setSerialize(true)

      val res = fsSerDe.transform(commDay)
      assert(res.head(1).isEmpty === false)
    }
  }
}
