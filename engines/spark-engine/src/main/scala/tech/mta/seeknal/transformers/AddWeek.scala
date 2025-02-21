package tech.mta.seeknal.transformers

import java.time.{DayOfWeek, LocalDate}
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, udf}
import tech.mta.seeknal.params.{DayOfWeekParam, HasInputCol, HasInputPattern, HasOutputCol, HasOutputPattern}

/** Add a column containing the date of the first day of the week based on a given date
  */
class AddWeek extends BaseTransformer with HasInputCol with HasInputPattern with HasOutputCol with HasOutputPattern {

  final val firstDayOfWeek: DayOfWeekParam =
    new DayOfWeekParam(this, "firstDayOfWeek", "First day of the week (in capital letters)")
  final def getFirstDayOfWeek: String = $(firstDayOfWeek)
  final def setFirstDayOfWeek(value: String): this.type = set(firstDayOfWeek, value)

  setDefault(inputPattern, "yyyyMMdd")
  setDefault(outputPattern, "yyyy-MM-dd")
  setDefault(outputCol, "week")
  setDefault(firstDayOfWeek, "MONDAY")

  override def transform(df: Dataset[_]): DataFrame = {
    df.withColumn(getOutputCol, udf(getWeek).apply(col(getInputCol)))
  }

  /** Parse a date string and print the first date of the week
    */
  private def getWeek: String => String = dateString => {
    val parser = DateTimeFormatter.ofPattern(getInputPattern)
    val dateTime = LocalDate.parse(dateString, parser)
    val firstDayOfWeek = DayOfWeek.valueOf(getFirstDayOfWeek)
    val dayOfWeek = dateTime.getDayOfWeek.getValue

    // make sure that distance is always positive
    var distanceToStartOfWeek = (dayOfWeek + 7 - firstDayOfWeek.getValue) % 7

    // get and print the start of the week
    val startOfWeek = dateTime.minusDays(distanceToStartOfWeek)
    val formatter = DateTimeFormatter.ofPattern(getOutputPattern)
    formatter.format(startOfWeek)
  }
}
