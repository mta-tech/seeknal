package tech.mta.seeknal.transformers

import org.apache.spark.ml.param.Param
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.expr

/** Filter by expression
  */
class FilterByExpr extends BaseTransformer {

  final val expression: Param[String] = new Param[String](this, "expression", "Expression to be applied to the filter")
  final def getExpression: String = $(expression)
  final def setExpression(value: String): this.type = set(expression, value)

  override def transform(df: Dataset[_]): DataFrame = {
    df.filter(expr(getExpression)).toDF
  }
}
