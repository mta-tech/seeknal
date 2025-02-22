package tech.mta.seeknal.connector.serde

import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.SparkSession
import tech.mta.seeknal.params.{HasInputCol, HasOutputCol, HasSerDeConfig}
import tech.mta.seeknal.transformers.BaseTransformer
import tech.mta.seeknal.utils.FileUtils

abstract class BaseSerDe(override val uid: String)
    extends BaseTransformer
    with HasSerDeConfig
    with HasInputCol
    with HasOutputCol {

  def this() = this(Identifiable.randomUID("SerDeTransformer"))

  def schemaReader(spark: SparkSession): String = {

    getSchemaFile.store match {
      case "hdfs" =>
        FileUtils.readFile(getSchemaFile.path, spark)
      case "file" =>
        FileUtils.readFile(getSchemaFile.path)
      case _ =>
        throw new IllegalArgumentException(s"This schemaFile.store: `${getSchemaFile.store}` is not supported")
    }
  }
}
