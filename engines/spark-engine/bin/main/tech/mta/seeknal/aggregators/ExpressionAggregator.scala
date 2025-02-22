package tech.mta.seeknal.aggregators

import org.apache.spark.ml.param.Param
import org.apache.spark.sql.functions.expr
import tech.mta.seeknal.params.HasOutputCol

/** Aggregate value with expression
  */
class ExpressionAggregator extends BaseAggregator with HasOutputCol {

  final val expression: Param[String] =
    new Param[String](this, "expression", "spark sql expression used to construct aggregation in outputCol")
  final def getExpression: String = $(expression)
  final def setExpression(value: String): this.type = set(expression, value)

  override def columns: Seq[ColumnDef] = {
    Seq(applyAlias((getOutputCol, expr(getExpression))))
  }
}
