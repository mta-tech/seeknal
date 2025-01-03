package tech.mta.seeknal.transformers

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.StringType
import tech.mta.seeknal.params.{HasInputCols, HasNullFilter}

/** Outputs distinct combinations of columns. Records with empty string or null values are ignored.
  */
class DistinctAttributes extends GroupingTransformer with HasInputCols with HasNullFilter {

  override def transform(df: Dataset[_]): DataFrame = {
    // differentiate string and non-string, since we are filtering empty string
    // and any non-string compared with string will always return false
    val stringCols = df.schema.fields.filter(_.dataType == StringType).map(_.name)
    val stringInputCols = getInputCols.intersect(stringCols)
    val nonStringInputCols = getInputCols.diff(stringInputCols)
    if (getNullFilter) {
      val filteredStringResult = stringInputCols.foldLeft(df) { (data, colName) =>
        data.filter(col(colName).isNotNull && !col(colName).equalTo(lit("")))
      }
      val filteredNonStringResult = nonStringInputCols.foldLeft(filteredStringResult) { (data, colName) =>
        data.filter(col(colName).isNotNull)
      }
      filteredNonStringResult.select(getInputCols.map(col): _*).distinct()
    } else {
      df.select(getInputCols.map(col): _*).distinct()
    }
  }
}
