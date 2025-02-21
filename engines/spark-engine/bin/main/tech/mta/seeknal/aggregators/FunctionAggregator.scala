package tech.mta.seeknal.aggregators

import org.apache.spark.ml.param.Param
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.col
import tech.mta.seeknal.utils.FunctionUtils
import tech.mta.seeknal.params.{HasInputCol, HasOutputCol}

/** Aggregate value accumulator function
  *
  * Input column will be aggregated based on a configured aggregate function in `accumulatorFunction`
  */
class FunctionAggregator extends BaseAggregator with HasInputCol with HasOutputCol {
  val accumulatorFunction: Param[String] = new Param[String](this, "accumulatorFunction", "Accumulator function name")
  def getAccumulatorFunction: String = $(accumulatorFunction)
  def setAccumulatorFunction(value: String): this.type = set(accumulatorFunction, value)
  setDefault(accumulatorFunction, "sum")

  val defaultAggregateValue: Param[String] = new Param[String](
    this,
    "defaultAggregateValue",
    "Default value of the accumulator function, in case it returns null"
  )
  def getDefaultAggregateValue: String = $(defaultAggregateValue)
  def setDefaultAggregateValue(value: String): this.type = set(defaultAggregateValue, value)

  val defaultAggregateValueType: Param[String] =
    new Param[String](this, "defaultAggregateValueType", "Data type of the default defaultAggregateValue")
  def getDefaultAggregateValueType: String = $(defaultAggregateValueType)
  def setDefaultAggregateValueType(value: String): this.type = set(defaultAggregateValueType, value)

  override def getAll()(implicit spark: Option[SparkSession]): Seq[Column] = {
    createOutputs(getAccumulatorFunction)
  }

  override def columns: Seq[ColumnDef] = Seq((getOutputCol, col(getInputCol)))

  protected def createOutputs(accumulator: String)(implicit spark: Option[SparkSession]): Seq[Column] = {

    val defaultVal = if (isDefined(defaultAggregateValue) && isDefined(defaultAggregateValueType)) {
      Some(FunctionAggregator.convertStringToObject(getDefaultAggregateValue, getDefaultAggregateValueType))
    } else {
      None
    }

    // apply aggregate function and generate outputs
    columns.map { case (outputColName, column) =>
      FunctionUtils
        .applyAggregateFunction(spark.get, accumulator, Seq(column), defaultVal)
        .alias(outputColName)
    }
  }
}

object FunctionAggregator {

  /** Convert a string into an object
    *
    * @param str
    *   the string representation of the object
    * @param objectType
    *   the object type, must be either string, int, long, double or float
    */
  def convertStringToObject(str: String, objectType: String): Any = {
    objectType match {
      case "string" => str
      case "int" => str.toInt
      case "long" => str.toLong
      case "double" => str.toDouble
      case "float" => str.toFloat
      case _ => throw new IllegalArgumentException("Invalid object type")
    }
  }
}
