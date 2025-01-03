package tech.mta.seeknal.transformers

import org.apache.spark.sql.{DataFrame, Dataset}
import tech.mta.seeknal.params.HasInputCols

class SelectColumns extends BaseTransformer with HasInputCols {

  override def transform(df: Dataset[_]): DataFrame = {
    df.select(getInputCols.head, getInputCols.tail: _*)
  }
}
