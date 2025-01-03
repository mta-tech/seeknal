package tech.mta.seeknal.transformers

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.expr
import tech.mta.seeknal.params.{HasColsByExpression, HasOutputCol}

/** Add new column by expression
  */
class AddColumnByExpr extends BaseTransformer with HasOutputCol with HasColsByExpression {

  override def transform(df: Dataset[_]): DataFrame = {
    df.withColumn(getOutputCol, expr(getExpression))
  }
}
