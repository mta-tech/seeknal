package tech.mta.seeknal.utils

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.functions.{coalesce, lit}

object FunctionUtils {

  /** Apply aggregate function to multiple columns
    *
    * @param spark
    *   the SparkSession
    * @param functionName
    *   the name of aggregate function to be applied
    * @param inputs
    *   the input columns to be aggregated
    * @param defaultValue
    *   the default value of the aggregation
    */
  def applyAggregateFunction(spark: SparkSession,
                             functionName: String,
                             inputs: Seq[Column],
                             defaultValue: Option[Any]
  ): Column = {
    val function = FunctionIdentifier.apply(functionName)
    val registry = spark.sessionState.functionRegistry
    val aggregateExpression = registry
      .lookupFunction(function, inputs.map(_.expr))
      .asInstanceOf[AggregateFunction]
      // TODO: add option to configure distinct aggregation, currently using the default (false)
      .toAggregateExpression(isDistinct = false)

    val col = new Column(aggregateExpression)
    if (defaultValue.isDefined) {
      coalesce(col, lit(defaultValue.get))
    } else {
      col
    }
  }
}
