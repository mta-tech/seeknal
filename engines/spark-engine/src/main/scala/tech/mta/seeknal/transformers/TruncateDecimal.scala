package tech.mta.seeknal.transformers

import org.apache.spark.ml.param.{BooleanParam, IntParam}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, udf}
import tech.mta.seeknal.params.{HasInputCol, HasOutputCol}

/** Truncate number of decimal digits Return as double
  */
class TruncateDecimal extends BaseTransformer with HasInputCol with HasOutputCol {

  final val decimalOffset: IntParam = new IntParam(this, "decimalOffset", "The number of digits on right side of dot)")
  final def getDecimalOffset: Int = $(decimalOffset)
  final def setDecimalOffset(value: Int): this.type = set(decimalOffset, value)
  setDefault(decimalOffset, 2)

  final val roundUp: BooleanParam =
    new BooleanParam(this, "booleanParam", "Apply rounding up when truncate the decimal")
  final def getRoundUp: Boolean = $(roundUp)
  final def setRoundUp(value: Boolean): this.type = set(roundUp, value)
  setDefault(roundUp, false)

  def truncateDecimalFun: (Double) => Double = (input) => {

    var round = BigDecimal.RoundingMode.DOWN

    if (getRoundUp) {
      round = BigDecimal.RoundingMode.UP
    }

    BigDecimal(input).setScale(getDecimalOffset, round).toDouble
  }

  override def transform(df: Dataset[_]): DataFrame = {

    df.withColumn(getOutputCol, udf(truncateDecimalFun).apply(col(getInputCol)))

  }
}
