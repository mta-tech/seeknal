package tech.mta.seeknal.transformers

import org.apache.spark.sql.{DataFrame, Dataset}
import tech.mta.seeknal.params.{HasInputCol, HasOutputCol}

/** Rename column
  */
class ColumnRenamed extends BaseTransformer with HasInputCol with HasOutputCol {

  override def transform(df: Dataset[_]): DataFrame = {
    df.withColumnRenamed(getInputCol, getOutputCol)
  }
}
