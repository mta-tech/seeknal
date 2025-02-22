package tech.mta.seeknal.transformers

import java.time.{Duration, LocalDateTime}
import java.time.format.DateTimeFormatterBuilder
import java.time.temporal.ChronoField

import org.apache.spark.ml.param.Param
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, lit, udf, when}
import tech.mta.seeknal.params.{HasInputPattern, HasOutputCol}

/** Returns difference in days between endDate and startDate
  */
class AddDateDifference extends BaseTransformer with HasInputPattern with HasOutputCol {

  final val startDate: Param[String] = new Param[String](this, "startDate", "The start date column")
  final def getStartDate: String = $(startDate)
  final def setStartDate(value: String): this.type = set(startDate, value)

  final val endDate: Param[String] = new Param[String](this, "endDate", "The end date column")
  final def getEndDate: String = $(endDate)
  final def setEndDate(value: String): this.type = set(endDate, value)

  setDefault(inputPattern, "yyyyMMdd")
  setDefault(outputCol, "difference")

  override def transform(df: Dataset[_]): DataFrame = {
    df.withColumn(getOutputCol,
                  when(col(getStartDate).isNull || col(getEndDate).isNull, lit(null))
                    .otherwise(udf(getDateDifference).apply(col(getStartDate), col(getEndDate)))
    )
  }

  private def getDateDifference: (String, String) => Long =
    (startDateString, endDateString) => {
      val parser = new DateTimeFormatterBuilder()
        .appendPattern(getInputPattern)
        .parseDefaulting(ChronoField.HOUR_OF_DAY, 1)
        .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 1)
        .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 1)
        .parseDefaulting(ChronoField.MILLI_OF_SECOND, 1)
        .toFormatter()
      // make sure end date always later than start date if they fall in the same day
      // so that we're only calculating different based on date portion
      val startDate = LocalDateTime.from(parser.parse(startDateString)).withHour(0)
      val endDate = LocalDateTime.from(parser.parse(endDateString)).withHour(1)
      Duration.between(startDate, endDate).toDays
    }
}
