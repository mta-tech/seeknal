package tech.mta.seeknal.transformers

import org.apache.spark.ml.param.Param
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.col
import tech.mta.seeknal.params.HasInputCol

/** Filter by distinct values in a column
  */
class FilterByColumnValue extends BaseTransformer with HasInputCol {

  final val valueCol: Param[String] =
    new Param[String](this, "valueCol", "Column from which distinct values are extracted")
  final def getValueCol: String = $(valueCol)
  final def setValueCol(value: String): this.type = set(valueCol, value)

  override def transform(df: Dataset[_]): DataFrame = {
    val values = df.select(getValueCol).distinct().collect()

    df.filter(col(getInputCol).isin(values.map(x => x(0)): _*)).toDF
  }
}
