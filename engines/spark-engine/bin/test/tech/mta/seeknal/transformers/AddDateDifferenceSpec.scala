package tech.mta.seeknal.transformers

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import tech.mta.seeknal.{BaseSparkSpec, DataFrameBuilder}

@RunWith(classOf[JUnitRunner])
class AddDateDifferenceSpec extends BaseSparkSpec {

  "AddDateDifference" should {
    "add new column with difference in days" in {
      val inputDf = DataFrameBuilder(Seq(Row("2018/06/01 23:50", "2018/06/02 01:00"),
                                         Row("2018/07/21 05:23", "2018/07/21 15:23"),
                                         Row(null, "2018/07/21 15:23"),
                                         Row("2018/07/21 05:23", null),
                                         Row(null, null)
                                     ),
                                     Seq(StructField("startDate", StringType), StructField("endDate", StringType))
      )
        .build()

      val expectedOutputDf = DataFrameBuilder(Seq(Row("2018/06/01 23:50", "2018/06/02 01:00", 1L),
                                                  Row("2018/07/21 05:23", "2018/07/21 15:23", 0L),
                                                  Row(null, "2018/07/21 15:23", null),
                                                  Row("2018/07/21 05:23", null, null),
                                                  Row(null, null, null)
                                              ),
                                              Seq(StructField("startDate", StringType),
                                                  StructField("endDate", StringType),
                                                  StructField("diff", LongType)
                                              )
      )
        .build()

      val transformer = new AddDateDifference()
        .setStartDate("startDate")
        .setEndDate("endDate")
        .setInputPattern("yyyy/MM/dd HH:mm")
        .setOutputCol("diff")

      val outputDf = transformer.transform(inputDf)

      assertDataFrameEquals(outputDf, expectedOutputDf)
    }
  }
}
