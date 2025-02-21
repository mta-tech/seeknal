package tech.mta.seeknal.transformers

import java.util.Locale

import org.apache.spark.ml.param.Param
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DecimalType, DoubleType, FloatType, IntegerType, LongType, StringType}
import tech.mta.seeknal.params.{HasInputCol, HasOutputCol}

/** Cast column from one data type to another
  */
class CastColumn extends BaseTransformer with HasInputCol with HasOutputCol {

  final val dataType: Param[String] = new Param[String](this, "dataType", "The data type to cast to")
  final def getDataType: String = $(dataType)
  final def setDataType(value: String): this.type = set(dataType, value)

  final val precision: Param[Int] =
    new Param[Int](this, "precision", "Maximum number of digits. only used when casting column to decimal")
  final def getPrecision: Int = $(precision)
  final def setPrecision(value: Int): this.type = set(precision, value)

  final val scale: Param[Int] =
    new Param[Int](this, "scale", "Number of digits on right side of dot. only used when casting column to decimal")
  final def getScale: Int = $(scale)
  final def setScale(value: Int): this.type = set(scale, value)

  setDefault(precision, 10)
  setDefault(scale, 2)

  override def transform(df: Dataset[_]): DataFrame = {
    val dataType = getDataType.toLowerCase(Locale.ROOT)

    dataType match {
      case "int" => df.withColumn(getOutputCol, col(getInputCol).cast(IntegerType))
      case "double" => df.withColumn(getOutputCol, col(getInputCol).cast(DoubleType))
      case "bigint" => df.withColumn(getOutputCol, col(getInputCol).cast(LongType))
      case "float" => df.withColumn(getOutputCol, col(getInputCol).cast(FloatType))
      case "decimal" =>
        df
          .withColumn(getOutputCol,
                      col(getInputCol)
                        .cast(DecimalType(getPrecision, getScale))
          )
      case "string" => df.withColumn(getOutputCol, col(getInputCol).cast(StringType))
      case e => throw new IllegalArgumentException(s"Invalid datatype: $e")
    }
  }
}
