package tech.mta.seeknal.transformers

import java.time.{DayOfWeek, LocalDate}
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, udf}
import tech.mta.seeknal.params.{HasDateCol, HasOutputCol}

/** Indicates if the input date is a weekday
  */
class AddDayType extends BaseTransformer with HasDateCol with HasOutputCol {

  setDefault(outputCol, "is_weekday")

  override def transform(df: Dataset[_]): DataFrame = {
    df.withColumn(getOutputCol, udf(isWeekday).apply(col(getDateCol)))
  }

  /** Parse a date string and determine if it falls on weekday or weekend
    */
  private def isWeekday: String => Boolean = dateString => {
    val parser = DateTimeFormatter.ofPattern(getInputDatePattern)
    val dateTime = LocalDate.parse(dateString, parser)
    val dayOfWeek = dateTime.getDayOfWeek

    !getWeekends.map(DayOfWeek.valueOf).contains(dayOfWeek)
  }
}
