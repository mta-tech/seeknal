package tech.mta.seeknal.transformers

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, lit, udf, when}
import tech.mta.seeknal.params.{HasInputCol, HasInputPattern, HasOutputCol, HasOutputPattern}
import tech.mta.seeknal.utils.Utils.formatDate

/** Reformat date column into new format Let's say the month is represented as yyyy-MM-01 Then 2018-03-04 -> 2018-03 ->
  * 2018-03-01
  */
class AddDate extends BaseTransformer with HasInputCol with HasInputPattern with HasOutputCol with HasOutputPattern {

  setDefault(inputPattern, "yyyyMMdd")
  setDefault(outputPattern, "yyyy-MM-dd")

  override def transform(df: Dataset[_]): DataFrame = {
    val dfWithDate = df.withColumn(getOutputCol,
                                   when(col(getInputCol).isNull, lit(null))
                                     .otherwise(udf(getDate).apply(col(getInputCol)))
    )
    if (getRemoveInputCol) {
      dfWithDate.drop(getInputCol)
    } else {
      dfWithDate
    }
  }

  /** Parse a date string and reformat it again as the first day of the month
    */
  private def getDate: String => String = dateString => {
    formatDate(dateString, getInputPattern, getOutputPattern)
  }
}
