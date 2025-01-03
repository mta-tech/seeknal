package tech.mta.seeknal.transformers

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.expr
import tech.mta.seeknal.params.{HasInputCol, HasOutputCol, HasValueMappings}

/** Maps values to another value Only support string value
  */
class ColumnValueRenamed extends BaseTransformer with HasInputCol with HasOutputCol with HasValueMappings {

  override def transform(df: Dataset[_]): DataFrame = {
    val rules = getValueMappings.map { mapping =>
      s"when $getInputCol = '${mapping.fromValue}' then '${mapping.toValue}'"
    }
    val expression = expr(s"case ${rules.mkString(" ")} else $getInputCol end")
    df.withColumn(getOutputCol, expression)
  }
}
