package tech.mta.seeknal.transformers

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.col
import tech.mta.seeknal.params.{HasInputCol, HasOutputCol}

/** Add new column and rename it
  */
class AddColumnRenamed extends BaseTransformer with HasInputCol with HasOutputCol {

  override def transform(df: Dataset[_]): DataFrame = {
    df.withColumn(getOutputCol, col(getInputCol))
  }
}
