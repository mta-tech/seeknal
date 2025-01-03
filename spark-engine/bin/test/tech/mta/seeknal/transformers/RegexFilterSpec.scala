package tech.mta.seeknal.transformers

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import tech.mta.seeknal.{BaseSparkSpec, DataFrameBuilder}

@RunWith(classOf[JUnitRunner])
class RegexFilterSpec extends BaseSparkSpec {

  "RegexFilter" should {
    val filter = new RegexFilter()
      .setInputCol("day")
      .setRegex("wa.*lk$")

    "filter out non-matching rows" in {
      val inputDf = DataFrameBuilder(Seq(
                                       // accepted
                                       Row("walk"),
                                       Row("wahahalk"),
                                       Row("abcd xwalk"),
                                       // filtered out
                                       Row("walkz"),
                                       Row("wal"),
                                       Row("alk")
                                     ),
                                     Seq(StructField("day", StringType))
      )
        .build()

      val expectedOutputDf =
        DataFrameBuilder(Seq(Row("walk"), Row("wahahalk"), Row("abcd xwalk")), Seq(StructField("day", StringType)))
          .build()

      val outputDf = filter.transform(inputDf)
      assertDataFrameEquals(outputDf, expectedOutputDf)
    }
  }
}
