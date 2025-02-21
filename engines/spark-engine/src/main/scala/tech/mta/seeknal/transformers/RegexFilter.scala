package tech.mta.seeknal.transformers

import org.apache.spark.ml.param.Param
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.col
import tech.mta.seeknal.params.HasInputCol

/** Filter data based on a given regex
  */
class RegexFilter extends BaseTransformer with HasInputCol {
  final val regex: Param[String] = new Param(this, "regex", "Regex to match the inputCol")
  final def getRegex: String = $(regex)
  final def setRegex(value: String): this.type = set(regex, value)

  override def transform(df: Dataset[_]): DataFrame = {
    df.filter(col(getInputCol).rlike(getRegex)).toDF
  }
}

object RegexFilter {

  def fromString(colName: String, expression: String): RegexFilter = expression match {
    case e if e.nonEmpty =>
      new RegexFilter()
        .setInputCol(colName)
        .setRegex(s"^$e")
    case _ => null
  }
}
