package tech.mta.seeknal.transformers

import java.lang.Math.log10

import org.apache.spark.ml.param.{Param, StringArrayParam}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, collect_list, expr, sum, udf, when}

class AddEntropy extends BaseTransformer {

  final val idCol: Param[String] = new Param[String](this, "idCol", "The column which stores the `id`")
  final def getIdCol: String = $(idCol)
  final def setIdCol(value: String): this.type = set(idCol, value)

  final val frequencyCol: Param[String] =
    new Param[String](this, "frequencyCol", "The column which stores the `frequency`")
  final def getFrequencyCol: String = $(frequencyCol)
  final def setFrequencyCol(value: String): this.type = set(frequencyCol, value)

  final val groupByCols: StringArrayParam =
    new StringArrayParam(this, "groupByCols", "The column names to group by besides id and frequency columns.")
  final def getGroupByCols: Array[String] = $(groupByCols)
  final def setGroupByCols(value: Array[String]): this.type = set(groupByCols, value)

  final val entityCol: Param[String] =
    new Param[String](this, "entityCol", "Column name which stores the entity for entropy calculation.")
  final def getEntityCol: String = $(entityCol)
  final def setEntityCol(value: String): this.type = set(entityCol, value)

  final val entropyCol: Param[String] = new Param[String](this, "entropyCol", "The column which stores `entropy`")
  final def getEntropyCol: String = $(entropyCol)
  final def setEntropyCol(value: String): this.type = set(entropyCol, value)

  final val filterExpression: Param[String] =
    new Param[String](this, "filterExpression", "Expression to filter the input.")
  final def getFilterExpression: String = $(filterExpression)
  final def setFilterExpression(value: String): this.type = set(filterExpression, value)

  setDefault(frequencyCol, "day")

  override def transform(df: Dataset[_]): DataFrame = {

    // columns to group by
    val allGroupByColNames = getGroupByCols :+ getIdCol :+ getFrequencyCol
    val idFrequencyGroupBy = Seq(getIdCol, getFrequencyCol)
    var df1 = df

    // filter by filter expression
    if (isDefined(filterExpression)) {
      df1 = df.filter(expr(getFilterExpression))
    }

    // group by all columns and sum entity
    val df2 = df1
      .groupBy(allGroupByColNames.map(col): _*)
      .agg(sum(getEntityCol) as "entity_per_group")

    // group by id and frequency, and collect "entity_per_group" as list
    val df3 = df2
      .groupBy(idFrequencyGroupBy.map(col): _*)
      .agg(collect_list("entity_per_group") as "entity_list_per_id")

    // udf to compute entropy per id
    val df4 = df3.withColumn(getEntropyCol,
                             udf(calculateEntropy)
                               .apply(col("entity_list_per_id"))
    )

    // rename columns in the aggregated df
    val tmpIdCol = getIdCol.concat("_tmp")
    val tmpFrequencyCol = getFrequencyCol.concat("_tmp")

    // keep required columns only
    val colsToSelect = Array(getIdCol, getFrequencyCol, getEntropyCol)
    val df5 = df4
      .select(colsToSelect.map(col): _*)
      .withColumnRenamed(getIdCol, tmpIdCol)
      .withColumnRenamed(getFrequencyCol, tmpFrequencyCol)

    // join the aggregated df with original df
    var df6 = df.join(df5,
                      df.col(getIdCol) === df5.col(tmpIdCol) &&
                        df.col(getFrequencyCol) === df5.col(tmpFrequencyCol),
                      "left"
    )

    // update the entropy based on filter expression
    if (isDefined(filterExpression)) {
      df6 = df6.withColumn(getEntropyCol,
                           when(expr(getFilterExpression), col(getEntropyCol))
                             .otherwise(0.0)
      )
    }

    // drop tmp id and frequency columns and return
    df6.drop(col(tmpIdCol)).drop(col(tmpFrequencyCol))
  }

  private def calculateEntropy: Seq[Long] => Double = entities => {
    var entropy = 0d
    val sumEntities = entities.foldLeft(0d)(_ + _)

    if (sumEntities > 0) {
      val log2 = (x: Double) => log10(x) / log10(2.0)

      entities.foreach(e => {
        val proportion = e / sumEntities
        entropy = entropy + (proportion * log2(proportion))
      })

      entropy = if (entropy == 0d) entropy else entropy * -1d
    }

    entropy
  }
}
