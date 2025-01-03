package tech.mta.seeknal.transformers

import org.apache.spark.sql.{DataFrame, Dataset}
import tech.mta.seeknal.params.HasInputCols

/** Drop columns
  */
class DropCols extends BaseTransformer with HasInputCols {

  override def transform(dataset: Dataset[_]): DataFrame = {
    dataset.drop(getInputCols: _*)
  }
}
