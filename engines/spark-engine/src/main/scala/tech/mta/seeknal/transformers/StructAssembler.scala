package tech.mta.seeknal.transformers

import org.apache.spark.ml.param.StringArrayParam
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, struct}
import tech.mta.seeknal.params.{HasInputCols, HasOutputCol}

/**
  * Combines a given list of columns or excluded columns into a single struct column.
  * It is useful for combining different columns in order to serializing the columns.
  */
class StructAssembler extends BaseTransformer
  with HasInputCols
  with HasOutputCol {

  final val excludeCols: StringArrayParam = new StringArrayParam(
    this,
    "excludeCols",
    "column that excluded to corresponding operation"
  )
  final def getExcludeCols: Array[String] = ${ excludeCols }
  final def setExcludeCols(value: Array[String]): this.type = set(excludeCols, value)

  override def transform(dataset: Dataset[_]): DataFrame = {

    // if excludeCols is defined, it will take all columns except the one
    // specified in excludeCols in order assemble struct column
    val targetCols: Array[String] = if (isDefined(excludeCols)) {
      val cols = dataset.columns.toBuffer
      cols --= getExcludeCols
      cols.toArray
    } else if (isDefined(inputCols)) {
      getInputCols
    } else {
      dataset.columns
    }

    // if set true, it will remove original column and keeping struct column
    val keepCols: Option[Array[String]] = if (getRemoveInputCols) {
      if (isDefined(excludeCols)) {
        Some(Array(getOutputCol) ++ getExcludeCols)
      } else {
        val cols = dataset.columns.toBuffer
        cols --= targetCols
        Some(cols.toArray[String] ++ Array(getOutputCol))
      }
    } else {
      None
    }

    val result = dataset
        .withColumn(getOutputCol, struct(targetCols.map(col): _*))

    keepCols match {
      case Some(cols) =>
        result.select(cols.head, cols.tail: _*)
      case None =>
        result
    }
  }
}
