package tech.mta.seeknal.transformers

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import tech.mta.seeknal.{BaseSparkSpec, DataFrameBuilder}

@RunWith(classOf[JUnitRunner])
class AddDateSpec extends BaseSparkSpec {

  "AddDate" should {
    val pattern = "yyyy-MM-dd"

    "add new column with formatted month info" in {
      val inputDf =
        DataFrameBuilder(Seq(Row("2018-06-01"), Row("2018-06-30"), Row("2018-07-01"), Row("2018-07-21"), Row(null)),
                         Seq(StructField("day", StringType))
        )
          .build()

      val expectedOutputDf = DataFrameBuilder(
        Seq(Row("2018-06-01", "2018-06"),
            Row("2018-06-30", "2018-06"),
            Row("2018-07-01", "2018-07"),
            Row("2018-07-21", "2018-07"),
            Row(null, null)
        ),
        Seq(StructField("day", StringType), StructField("month", StringType))
      )
        .build()

      val transformer = new AddDate()
        .setInputCol("day")
        .setInputPattern(pattern)
        .setOutputCol("month")
        .setOutputPattern("yyyy-MM")

      val outputDf = transformer.transform(inputDf)
      assertDataFrameEquals(outputDf, expectedOutputDf)
    }

    "replace column with formatted month info" in {
      val inputDf =
        DataFrameBuilder(Seq(Row("2018-06-01"), Row("2018-06-30"), Row("2018-07-01"), Row("2018-07-21"), Row(null)),
                         Seq(StructField("day", StringType))
        )
          .build()

      val expectedOutputDf =
        DataFrameBuilder(Seq(Row("2018-06"), Row("2018-06"), Row("2018-07"), Row("2018-07"), Row(null)),
                         Seq(StructField("month", StringType))
        )
          .build()

      val transformer = new AddDate()
        .setInputCol("day")
        .setInputPattern(pattern)
        .setOutputCol("month")
        .setOutputPattern("yyyy-MM")
        .setRemoveInputCol(true)

      val outputDf = transformer.transform(inputDf)
      assertDataFrameEquals(outputDf, expectedOutputDf)
    }

    "parse month without date" in {
      val inputDf =
        DataFrameBuilder(Seq(Row("2018-06"), Row("2018-07"), Row(null)), Seq(StructField("month", StringType)))
          .build()

      val expectedOutputDf =
        DataFrameBuilder(Seq(Row("2018-06", "2018-06-01"), Row("2018-07", "2018-07-01"), Row(null, null)),
                         Seq(StructField("month", StringType), StructField("new_month", StringType))
        )
          .build()

      val transformer = new AddDate()
        .setInputCol("month")
        .setInputPattern("yyyy-MM")
        .setOutputCol("new_month")
        .setOutputPattern("yyyy-MM-01")

      val outputDf = transformer.transform(inputDf)
      assertDataFrameEquals(outputDf, expectedOutputDf)
    }

    "parse year without month and date" in {
      val inputDf = DataFrameBuilder(Seq(Row("2018")), Seq(StructField("year", StringType)))
        .build()

      val expectedOutputDf = DataFrameBuilder(Seq(Row("2018", "2018-01-01")),
                                              Seq(StructField("year", StringType), StructField("new_year", StringType))
      )
        .build()

      val transformer = new AddDate()
        .setInputCol("year")
        .setInputPattern("yyyy")
        .setOutputCol("new_year")
        .setOutputPattern("yyyy-01-01")

      val outputDf = transformer.transform(inputDf)
      assertDataFrameEquals(outputDf, expectedOutputDf)
    }

    "replace column irrespective of the case of month" in {
      val inputDf =
        DataFrameBuilder(Seq(Row("11-MAR-15 01.56.21.000000000 PM"), Row("11-Jun-15 01.56.21.000000000 AM")),
                         Seq(StructField("date", StringType))
        )
          .build()

      val expectedOutputDf =
        DataFrameBuilder(Seq(Row("2015-03-11"), Row("2015-06-11")), Seq(StructField("day", StringType)))
          .build()

      val transformer = new AddDate()
        .setInputCol("date")
        .setInputPattern("dd-MMM-yy hh.mm.ss.000000000 a")
        .setOutputCol("day")
        .setOutputPattern("yyyy-MM-dd")
        .setRemoveInputCol(true)

      val outputDf = transformer.transform(inputDf)
      assertDataFrameEquals(outputDf, expectedOutputDf)
    }

  }
}
