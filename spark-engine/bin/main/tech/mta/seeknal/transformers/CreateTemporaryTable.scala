package tech.mta.seeknal.transformers

import org.apache.spark.ml.param.Param
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset}

/** Create Temporary Table with SQL statement
  */
class CreateTemporaryTable extends BaseTransformer {

  final val statement: Param[String] =
    new Param[String](this, "statement", "SQL statement")
  final def setStatement(value: String): this.type = set(statement, value)
  final def getStatement: String = $(statement)

  final val tempTableName: Param[String] =
    new Param[String](this, "tempTableName", "Temporary table name resulting from a statement")
  final def setTempTableName(value: String): this.type = set(tempTableName, value)
  final def getTempTableName: String = $ { tempTableName }

  private val tableIdentifier: String = "__THIS__"

  override def transform(dataset: Dataset[_]): DataFrame = {

    val tableName = Identifiable.randomUID(uid)
    dataset.createOrReplaceTempView(tableName)
    val realStatement = $(statement).replace(tableIdentifier, tableName)
    val input = dataset.sparkSession.sql(realStatement)
    input.createOrReplaceTempView($ { tempTableName })
    dataset.sparkSession.sessionState.catalog.dropTempView(tableName)
    dataset.toDF()
  }
}
