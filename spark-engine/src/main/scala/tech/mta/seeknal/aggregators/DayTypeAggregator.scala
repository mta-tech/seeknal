package tech.mta.seeknal.aggregators

import org.apache.spark.ml.param.Param
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, when}

/** Aggregate value by day type
  *
  * Will produce three aggregates from one input column: weekday, weekend and all days Input column will be aggregated
  * based on a configured aggregate function in `accumulatorFunction`
  */
class DayTypeAggregator extends FunctionAggregator {

  val weekdayCol: Param[String] =
    new Param[String](this, "weekdayCol", "column containing weekday information (true / false)")
  def getWeekdayCol: String = $(weekdayCol)
  def setWeekdayCol(value: String): this.type = set(weekdayCol, value)
  setDefault(weekdayCol, "is_weekday")

  override def columns: Seq[(String, Column)] = {
    val isWeekday: Column = col(getWeekdayCol) === true
    // different conditions to aggregate input column: all day, weekday, weekend
    Seq((s"${getOutputCol}_alldays", col(getInputCol)),
        (s"${getOutputCol}_wkday", when(isWeekday, col(getInputCol))),
        (s"${getOutputCol}_wkend", when(!isWeekday, col(getInputCol)))
    )
  }
}
