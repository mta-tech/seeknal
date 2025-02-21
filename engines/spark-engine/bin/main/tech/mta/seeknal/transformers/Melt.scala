package tech.mta.seeknal.transformers

import org.apache.spark.ml.param.{Param, StringArrayParam}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{array, col, explode, lit, struct}
import tech.mta.seeknal.params.HasInputCols

/** Taken from: https://stackoverflow.com/questions/41670103/how-to-melt-spark-dataframe Convert
  * [[org.apache.spark.sql.DataFrame]] from wide to long format.
  *
  * melt is (kind of) the inverse of pivot Note: currently it is not support melting columns that have different
  * datatype.
  */
class Melt extends BaseTransformer with HasInputCols {

  final val keyCols: StringArrayParam =
    new StringArrayParam(this, "keyCols", "the columns to preserve")
  final def getKeyCols: Array[String] = $(keyCols)
  final def setKeyCols(value: Array[String]): this.type = set(keyCols, value)

  final val outputKeyCol: Param[String] =
    new Param[String](this, "outputKeyCol", "the name for the column holding the melted column names")
  final def getOutputKeyCol: String = $ { outputKeyCol }
  final def setOutputKeyCol(value: String): this.type = set(outputKeyCol, value)

  final val outputValueCol: Param[String] =
    new Param[String](this, "outputValueCol", "the name for the column holding the values of the melted columns")
  final def getOutputValueCol: String = $ { outputValueCol }

  final def setOutputValueCol(value: String): this.type =
    set(outputValueCol, value)

  override def transform(dataset: Dataset[_]): DataFrame = {

    // Create array<struct<variable: str, value: ...>>
    val varsAndVals = array((for (c <- $ { inputCols }) yield {
      struct(lit(c).alias($ { outputKeyCol }),
             col(c).alias($ {
               outputValueCol
             })
      )
    }): _*)

    // Add to the DataFrame and explode
    val tmp = dataset.withColumn("_vars_and_vals", explode(varsAndVals))
    val cols = $ { keyCols }.map(col) ++ {
      for (x <- List($ { outputKeyCol }, $ { outputValueCol })) yield {
        col("_vars_and_vals")(x).alias(x)
      }
    }

    tmp.select(cols: _*)
  }
}
