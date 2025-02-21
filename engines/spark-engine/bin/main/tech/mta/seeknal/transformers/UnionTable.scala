package tech.mta.seeknal.transformers

import org.apache.spark.ml.param.{BooleanParam, Param, StringArrayParam}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, expr, max}

/** Union table
  */
class UnionTable extends BaseTransformer {

  final val tableName: Param[String] = new Param[String](this, "tableName", "Table to union")
  final def getTableName: String = $(tableName)
  final def setTableName(value: String): this.type = set(tableName, value)

  final val filterExpression: Param[String] =
    new Param[String](this, "filterExpression", "Filter rule to apply before union table if any")
  final def getFilterExpression: String = $(filterExpression)
  final def setFilterExpression(value: String): this.type = set(filterExpression, value)

  final val latestData: BooleanParam = new BooleanParam(
    this,
    "latestData",
    "Use latest data available in target table, corresponds to the largest data in the column"
  )
  final def useLatestData: Boolean = $(latestData)
  final def setUseLatestData(value: Boolean): this.type = set(latestData, value)
  setDefault(latestData, false)

  final val partitionCols: StringArrayParam = new StringArrayParam(
    this,
    "partitionCols",
    "Choose columns that's used to identify latest data." +
      "Use the table's partition columns by default"
  )
  final def getPartitionCols: Array[String] = $(partitionCols)
  final def setPartitionCols(value: Array[String]): this.type = set(partitionCols, value)

  override def transform(df: Dataset[_]): DataFrame = {
    val spark = df.sparkSession
    var table = spark.table(getTableName)

    if (isDefined(filterExpression)) {
      table = table.filter(expr(getFilterExpression))
    }

    if (useLatestData) {
      val cols = if (!isDefined(partitionCols)) {
        import spark.implicits._
        spark.sql(s"show partitions $getTableName").as[String].first.split('/').map(_.split("=").head)
      } else {
        getPartitionCols
      }

      val aggrMax = cols.map(x => max(x).alias(s"max_$x"))
      val latest = table.agg(aggrMax.head, aggrMax.tail: _*).collect()

      var k = 0
      for (x <- cols) {
        table = table.filter(col(x) === latest(k)(0))
        k += 1
      }
    }

    table.union(df.select(table.columns.map(col): _*))
  }
}
