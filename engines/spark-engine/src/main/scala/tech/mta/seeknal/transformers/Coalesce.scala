package tech.mta.seeknal.transformers

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{coalesce, col}
import tech.mta.seeknal.params.{HasInputCols, HasOutputCol}

/** Coalesce columns
  */
class Coalesce extends BaseTransformer with HasInputCols with HasOutputCol {

  override def transform(df: Dataset[_]): DataFrame = {
    df.withColumn(getOutputCol, coalesce(getInputCols.map(col): _*))
  }
}
