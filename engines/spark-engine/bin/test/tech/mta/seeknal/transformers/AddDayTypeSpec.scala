package tech.mta.seeknal.transformers

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{BooleanType, StringType, StructField}
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import tech.mta.seeknal.{BaseSparkSpec, DataFrameBuilder}

@RunWith(classOf[JUnitRunner])
class AddDayTypeSpec extends BaseSparkSpec {

  "AddDayType" should {
    val pattern = "yyyy-MM-dd"
    val formatter: DateTimeFormatter = DateTimeFormat.forPattern(pattern)
    // 11 June 2018 = Monday
    val startDate = new DateTime(2018, 6, 11, 0, 0, 0, 0)

    // create input DataFrame from Monday to Sunday
    val inputData = for (i <- 0 until 7) yield {
      val currentDate = startDate.plusDays(i)
      Row(formatter.print(currentDate))
    }

    "add new column with weekday info" in {
      val inputDf = DataFrameBuilder(inputData, Seq(StructField("day", StringType)))
        .build()

      val expectedOutputDf = DataFrameBuilder(Seq(Row("2018-06-11", true), // monday
                                                  Row("2018-06-12", true),
                                                  Row("2018-06-13", true),
                                                  Row("2018-06-14", true),
                                                  Row("2018-06-15", true),
                                                  Row("2018-06-16", false), // saturday
                                                  Row("2018-06-17", false)
                                              ), // sunday
                                              Seq(StructField("day", StringType), StructField("weekday", BooleanType))
      )
        .build()

      val transformer = new AddDayType()
        .setDateCol("day")
        .setInputDatePattern(pattern)
        .setOutputCol("weekday")

      val outputDf = transformer.transform(inputDf)
      assertDataFrameEquals(outputDf, expectedOutputDf)
    }

    "add new column with weekday info based on custom weekends configuration" in {
      val inputDf = DataFrameBuilder(inputData, Seq(StructField("day", StringType)))
        .build()

      val expectedOutputDf = DataFrameBuilder(Seq(Row("2018-06-11", false), // monday
                                                  Row("2018-06-12", false), // tuesday
                                                  Row("2018-06-13", true),
                                                  Row("2018-06-14", true),
                                                  Row("2018-06-15", true),
                                                  Row("2018-06-16", true),
                                                  Row("2018-06-17", true)
                                              ), // sunday
                                              Seq(StructField("day", StringType), StructField("weekday", BooleanType))
      )
        .build()

      val transformer = new AddDayType()
        .setDateCol("day")
        .setInputDatePattern(pattern)
        .setOutputCol("weekday")
        .setWeekends(Array("MONDAY", "TUESDAY"))

      val outputDf = transformer.transform(inputDf)
      assertDataFrameEquals(outputDf, expectedOutputDf)
    }
  }
}
