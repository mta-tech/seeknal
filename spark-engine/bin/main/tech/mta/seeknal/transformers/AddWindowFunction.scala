package tech.mta.seeknal.transformers

import org.apache.spark.ml.param.{BooleanParam, Param, StringArrayParam}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import tech.mta.seeknal.params.{HasColsByExpression, HasInputCol, HasOutputCol}

/** Add new column by Window Function
  */
class AddWindowFunction extends BaseTransformer with HasInputCol with HasOutputCol with HasColsByExpression {

  final val offset: Param[String] =
    new Param[String](this, "offset", "Offset supplied to the window function (ntile, lag, lead)")
  final def getOffset: String = $(offset)
  final def setOffset(value: String): this.type = set(offset, value)

  final val windowFunction: Param[String] = new Param[String](
    this,
    "windowFunction",
    "The name of window function (avg, sum, count, max, min, rank, row_number, dense_rank" +
      "percent_rank, ntile, cume_dist, lag, lead"
  )
  final def getWindowFunction: String = $(windowFunction)
  final def setWindowFunction(value: String): this.type = set(windowFunction, value)

  final val partitionCols: StringArrayParam =
    new StringArrayParam(this, "partitionCols", "List of partition columns")
  final def getPartitionCols: Array[String] = $(partitionCols)
  final def setPartitionCols(value: Array[String]): this.type = set(partitionCols, value)

  final val orderCols: StringArrayParam =
    new StringArrayParam(this, "orderCols", "Columns to order the partition window")
  final def getOrderCols: Array[String] = $(orderCols)
  final def setOrderCols(value: Array[String]): this.type = set(orderCols, value)

  // TODO: include this in orderCols directly to support different ordering for different column
  final val ascending: BooleanParam =
    new BooleanParam(this, "ascending", "If set to true, use ascending order for orderCols")
  final def isAscending: Boolean = $(ascending)
  final def setAscending(value: Boolean): this.type = set(ascending, value)

  setDefault(orderCols, Array.empty[String])
  setDefault(ascending, true)

  final val FLAG_COL = "_expression"
  final val ROW_N_COL = "_ROW_N"
  final val PARTITION_COL = "_id"

  /** get last distinct value based on expression
    *
    * @param dataFrame
    *   @param partitionBy
    * @return
    */
  def getLastDistinctByExpression(dataFrame: Dataset[_], partitionBy: WindowSpec): DataFrame = {

    val input = dataFrame
      .toDF()
      .withColumn(FLAG_COL, when(expr(getExpression), 1).otherwise(0))
      .withColumn(ROW_N_COL, row_number() over partitionBy)
      .withColumn(PARTITION_COL, concat_ws(",", getPartitionCols.map(col): _*))

    val inputColDataType = input.schema.fields
      .map(x => x.name -> x.dataType)
      .toMap
      .get(getInputCol)
    val vocabCols = Array(PARTITION_COL, getInputCol, ROW_N_COL, FLAG_COL)
    val vocab = input
      .withColumn(getInputCol, col(getInputCol).cast("string"))
      .select(vocabCols.head, vocabCols.tail: _*)

    import dataFrame.sparkSession.implicits._

    val lastDistinctVocab = vocab.rdd
      .map(x => {
        x.getAs[String](PARTITION_COL) ->
          List((x.getAs[Int](ROW_N_COL), x.getAs[Int](FLAG_COL), x.getAs[String](getInputCol)))
      })
      .reduceByKey((x, y) => x ++ y)
      .mapValues(x => {
        var row_n = x.head._1
        var value = x.head._3

        x.map(y => {
          if (y._2 != 0) {
            row_n = y._1
            value = y._3
            (y._1, y._2, row_n, value)
          } else {
            (y._1, y._2, row_n, value)
          }
        })
      })
      .flatMapValues(x => x)
      .map(x => {
        (x._1, x._2._1, x._2._4)
      })
      .toDF(PARTITION_COL, ROW_N_COL, getOutputCol)
      .drop(FLAG_COL)

    input
      .join(lastDistinctVocab, Seq(PARTITION_COL, ROW_N_COL), "inner")
      .withColumn(getOutputCol, col(getOutputCol).cast(inputColDataType.get))
      .drop(PARTITION_COL, ROW_N_COL, FLAG_COL)
  }

  override def transform(df: Dataset[_]): DataFrame = {
    val ordering = getOrderCols.map(x => if (isAscending) asc(x) else desc(x))
    val partitionBy = Window
      .partitionBy(getPartitionCols.map(col): _*)
      .orderBy(ordering: _*)

    getWindowFunction match {
      case "avg" => df.withColumn(getOutputCol, avg(col(getInputCol)) over partitionBy)
      case "sum" => df.withColumn(getOutputCol, sum(col(getInputCol)) over partitionBy)
      case "count" => df.withColumn(getOutputCol, count(col(getInputCol)) over partitionBy)
      case "max" => df.withColumn(getOutputCol, max(col(getInputCol)) over partitionBy)
      case "min" => df.withColumn(getOutputCol, min(col(getInputCol)) over partitionBy)
      case "rank" => df.withColumn(getOutputCol, rank over partitionBy)
      case "row_number" => df.withColumn(getOutputCol, row_number over partitionBy)
      case "dense_rank" => df.withColumn(getOutputCol, dense_rank over partitionBy)
      case "percent_rank" => df.withColumn(getOutputCol, percent_rank over partitionBy)
      case "ntile" => df.withColumn(getOutputCol, ntile(getOffset.toInt) over partitionBy)
      case "cume_dist" => df.withColumn(getOutputCol, cume_dist over partitionBy)
      case "lag" => df.withColumn(getOutputCol, lag(getInputCol, getOffset.toInt) over partitionBy)
      case "lead" =>
        df.withColumn(getOutputCol,
                      lead(getInputCol, getOffset.toInt)
                        over partitionBy
        )
      case "last_distinct" => getLastDistinctByExpression(df, partitionBy)
      case _ => throw new IllegalArgumentException("Invalid window function")
    }
  }
}
