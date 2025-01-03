package tech.mta.seeknal.transformers

import org.apache.spark.ml.param.Param
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.col
import tech.mta.seeknal.params.HasInputCol

/** Filter dataset with list of value
  */
class FilterByIsIn extends BaseTransformer with HasInputCol {

  final val values: Param[List[String]] = new Param[List[String]](this, "valueCol", "Value being choose for filter")
  final def getValues: List[String] = $(values)
  final def setValues(value: List[String]): this.type = set(values, value)

  override def transform(dataset: Dataset[_]): DataFrame = {

    dataset.filter(col(getInputCol).isin(getValues: _*)).toDF
  }
}
