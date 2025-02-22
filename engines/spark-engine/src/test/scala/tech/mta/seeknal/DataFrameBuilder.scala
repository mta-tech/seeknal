package tech.mta.seeknal

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StructField, StructType}

case class DataFrameBuilder(input: Seq[Row] = Seq(), schema: Seq[StructField] = Seq()) {

  def build()(implicit spark: SparkSession): DataFrame = {
    spark.createDataFrame(spark.sparkContext.parallelize(input), StructType(schema))
  }
}
