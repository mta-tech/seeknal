package tech.mta.seeknal.aggregators

import org.apache.spark.ml.param.{ParamMap, Params, StringArrayParam}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{Column, SparkSession}

abstract class BaseAggregator extends Params {
  val uid: String = Identifiable.randomUID("BaseAggregator")
  override def copy(extra: ParamMap): BaseAggregator = defaultCopy(extra)

  final val features: StringArrayParam =
    new StringArrayParam(this, "features", "Features to compute, empty means all")
  final def getFeatures: Array[String] = $(features)
  final def setFeatures(value: Array[String]): this.type = set(features, value)

  type ColumnDef = (String, Column)
  def columns: Seq[ColumnDef]

  /** Apply column name alias to column definition
    */
  def applyAlias(columnDef: ColumnDef): ColumnDef =
    (columnDef._1, columnDef._2.alias(columnDef._1))

  /** Get all aggregators or aggregators specified in the feature list
    *
    * @param spark
    *   SparkSession used to retrieve the aggregation function (eg. sum, count, etc.)
    */
  def getAll()(implicit spark: Option[SparkSession]): Seq[Column] = {
    // returns all columns if no features are specified
    // else returns features with matching name
    columns
      .filter(columnDef => !isDefined(features) || getFeatures.contains(columnDef._1))
      .map(columnDef => columnDef._2)
  }
}
