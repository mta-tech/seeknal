package tech.mta.seeknal.transformers

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField}
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import tech.mta.seeknal.{BaseSparkSpec, DataFrameBuilder}

@RunWith(classOf[JUnitRunner])
class AddWeekSpec extends BaseSparkSpec {

  "AddWeek" should {
    val pattern = "yyyy-MM-dd"
    val newPattern = "yyyy-MM-dd"
    val formatter: DateTimeFormatter = DateTimeFormat.forPattern(pattern)

    "add new column with default week configuration" in {
      // 30 July 2018 = Monday
      val startDate = new DateTime(2018, 7, 30, 0, 0, 0, 0)

      // create input DataFrame from Monday to next Tuesday
      val inputData = for (i <- 0 until 9) yield {
        val currentDate = startDate.plusDays(i)
        Row(formatter.print(currentDate))
      }
      val inputDf = DataFrameBuilder(inputData, Seq(StructField("day", StringType)))
        .build()

      val expectedOutputDf = DataFrameBuilder(Seq(Row("2018-07-30", "2018-07-30"), // mon
                                                  Row("2018-07-31", "2018-07-30"), // tue
                                                  Row("2018-08-01", "2018-07-30"), // wed
                                                  Row("2018-08-02", "2018-07-30"), // thu
                                                  Row("2018-08-03", "2018-07-30"), // fri
                                                  Row("2018-08-04", "2018-07-30"), // sat
                                                  Row("2018-08-05", "2018-07-30"), // sun
                                                  Row("2018-08-06", "2018-08-06"), // mon
                                                  Row("2018-08-07", "2018-08-06")
                                              ), // tue
                                              Seq(StructField("day", StringType), StructField("week", StringType))
      )
        .build()

      val transformer = new AddWeek()
        .setInputCol("day")
        .setInputPattern(pattern)
        .setOutputCol("week")
        .setOutputPattern(newPattern)

      val outputDf = transformer.transform(inputDf)
      assertDataFrameEquals(outputDf, expectedOutputDf)
    }

    "add new column with custom week configuration" in {
      // 30 July 2018 = Monday
      val startDate = new DateTime(2018, 7, 30, 0, 0, 0, 0)

      // create input DataFrame from Monday to next Tuesday
      val inputData = for (i <- 0 until 9) yield {
        val currentDate = startDate.plusDays(i)
        Row(formatter.print(currentDate))
      }
      val inputDf = DataFrameBuilder(inputData, Seq(StructField("day", StringType)))
        .build()

      val expectedOutputDf = DataFrameBuilder(Seq(Row("2018-07-30", "2018-07-24"), // mon
                                                  Row("2018-07-31", "2018-07-31"), // tue
                                                  Row("2018-08-01", "2018-07-31"), // wed
                                                  Row("2018-08-02", "2018-07-31"), // thu
                                                  Row("2018-08-03", "2018-07-31"), // fri
                                                  Row("2018-08-04", "2018-07-31"), // sat
                                                  Row("2018-08-05", "2018-07-31"), // sun
                                                  Row("2018-08-06", "2018-07-31"), // mon
                                                  Row("2018-08-07", "2018-08-07")
                                              ), // tue
                                              Seq(StructField("day", StringType), StructField("week", StringType))
      )
        .build()

      val transformer = new AddWeek()
        .setInputCol("day")
        .setInputPattern(pattern)
        .setOutputCol("week")
        .setOutputPattern(newPattern)
        .setFirstDayOfWeek("TUESDAY")

      val outputDf = transformer.transform(inputDf)
      assertDataFrameEquals(outputDf, expectedOutputDf)
    }
  }
}
