package tech.mta.seeknal.transformers

import org.apache.spark.ml.param.Param
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.expr
import tech.mta.seeknal.params.HasInputCols

/** Join with another table based on a set of columns. All columns specified in inputCols must be present in both sides
  */
class JoinById extends BaseTransformer with HasInputCols {

  final val targetTable: Param[String] = new Param[String](this, "targetTable", "The table to join to")
  final def getTargetTable: String = $(targetTable)
  final def setTargetTable(value: String): this.type = set(targetTable, value)

  final val filterExpression: Param[String] =
    new Param[String](this, "filterExpression", "Filter expression to be applied for the target table before joining")
  final def getFilterExpression: String = $(filterExpression)
  final def setFilterExpression(value: String): this.type = set(filterExpression, value)

  final val joinType: Param[String] = new Param[String](this, "joinType", "The join joinType, eg. inner, cross, etc.")
  final def getJoinType: String = $(joinType)
  final def setJoinType(value: String): this.type = set(joinType, value)
  setDefault(joinType, "left_outer")

  override def transform(df: Dataset[_]): DataFrame = {
    val table = if (isDefined(filterExpression)) {
      df.sparkSession.table(getTargetTable).filter(expr(getFilterExpression))
    } else {
      df.sparkSession.table(getTargetTable)
    }

    df.join(table, getInputCols, getJoinType)
  }
}
