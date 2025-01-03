package tech.mta.seeknal.transformers

import org.apache.spark.ml.param.{BooleanParam, Param}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{broadcast, expr}
import tech.mta.seeknal.params.HasInputCols

/** Join with another table based on expression. Source table have alias "a" and target table have alias "b"
  */
class JoinByExpr extends BaseTransformer with HasInputCols {

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

  final val joinExpression: Param[String] =
    new Param[String](this, "joinExpression", "Join expression to be applied for joining")
  final def getJoinExpression: String = $(joinExpression)
  final def setJoinExpression(value: String): this.type = set(joinExpression, value)

  final val doBroadcast: BooleanParam =
    new BooleanParam(this, "doBroadcast", "Broadcast applied to target table before joining")
  final def getDoBroadcast: Boolean = $(doBroadcast)
  final def setDoBroadcast(value: Boolean): this.type = set(doBroadcast, value)
  setDefault(doBroadcast, false)

  override def transform(df: Dataset[_]): DataFrame = {

    var table = if (isDefined(filterExpression)) {
      df.sparkSession.table(getTargetTable).filter(expr(getFilterExpression))
    } else {
      df.sparkSession.table(getTargetTable)
    }

    if (getDoBroadcast) {
      table = broadcast(table)
    }

    df.as("a").join(table.as("b"), expr(getJoinExpression), getJoinType)

  }
}
