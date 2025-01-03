package tech.mta.seeknal.transformers

import org.apache.spark.ml.param.{Param, StringArrayParam}
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, expr}
import tech.mta.seeknal.params.{HasColsByExpression, HasInputCols, HasRenamedCols}

/** Group data by a set of columns then perform some aggregations
  *
  * @param _aggregators
  *   the list of aggregators, must not be empty
  */
class GroupByColumns(_aggregators: Seq[Column])
    extends GroupingTransformer
    with HasColsByExpression
    with HasInputCols
    with HasRenamedCols {

  final val pivotKeyCol: Param[String] =
    new Param[String](this, "pivotKeyCol", "Pivot key column")
  final def getPivotKeyCol: String = $ { pivotKeyCol }
  final def setPivotKeyCol(value: String): this.type = set(pivotKeyCol, value)

  final val pivotValueCols: StringArrayParam =
    new StringArrayParam(this, "pivotValueCols", "Pivot value columns")
  final def getPivotValueCols: Array[String] = $ { pivotValueCols }

  final def setPivotValueCols(value: Array[String]): this.type =
    set(pivotValueCols, value)

  val aggregators: Seq[Column] = _aggregators

  def this() = this(Seq())

  override def withAggregators(aggregators: Seq[Column]): GroupByColumns = {
    val instance = new GroupByColumns(aggregators)
    copyValues(instance, extractParamMap())
  }

  override def transform(df: Dataset[_]): DataFrame = {
    var aggregatedExpr = df.groupBy(getInputCols.map(col): _*)
    if (isDefined(pivotKeyCol)) {
      if (isDefined(pivotValueCols)) {
        aggregatedExpr = aggregatedExpr.pivot(getPivotKeyCol, getPivotValueCols)
      } else {
        aggregatedExpr = aggregatedExpr.pivot(getPivotKeyCol)
      }
    }
    val aggregatedData = aggregatedExpr.agg(aggregators.head, aggregators.drop(1): _*)

    val dataWithColsByExpression = if (isDefined(colsByExpression)) {
      getColsByExpression.foldLeft(aggregatedData) { (data, col) =>
        data.withColumn(col.newName, expr(col.expression))
      }
    } else {
      aggregatedData
    }

    if (isDefined(renamedCols)) {
      getRenamedCols.foldLeft(dataWithColsByExpression) { (data, col) =>
        data.withColumnRenamed(col.name, col.newName)
      }
    } else {
      dataWithColsByExpression
    }
  }
}
