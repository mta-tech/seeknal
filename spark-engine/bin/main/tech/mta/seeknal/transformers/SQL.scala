package tech.mta.seeknal.transformers

import org.apache.spark.ml.param.Param
import org.apache.spark.sql.{DataFrame, Dataset}

/** Extending Spark SQLTransformera
  */

class SQL extends BaseTransformer {

  final val statement: Param[String] =
    new Param[String](this, "statement", "SQL statement")
  final def setStatement(value: String): this.type = set(statement, value)
  final def getStatement: String = $(statement)

  override def transform(dataset: Dataset[_]): DataFrame = {

    val sqlTransformer = new org.apache.spark.ml.feature.SQLTransformer()
      .setStatement($ { statement })

    sqlTransformer.transform(dataset)
  }
}
