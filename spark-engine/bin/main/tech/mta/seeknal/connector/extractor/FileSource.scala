package tech.mta.seeknal.connector.extractor

import org.apache.log4j.{Level, Logger, LogManager}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import tech.mta.seeknal.connector.{BaseFile, IOMaterial}

/** Extractor class for read data from file
  *
  * @param extractor
  */
class FileSource(extractor: IOMaterial) extends BaseFile(extractor: IOMaterial) {

  require(extractor.sourceConfig.params.isDefined, "params should contains configuration parameters ")

  val logger: Logger = LogManager.getLogger(this.getClass)
  logger.setLevel(Level.INFO)

  /** get data from file
    *
    * @return
    */
  override def get(): DataFrame = {
    val sourcePath = getFilePath
    logger.info(s"Reading data from '$sourcePath'")
    // Drop "path" from options since compiler complains if path is specified in options and load
    val data = extractor.spark.read
      .format(getFormat)
      .options(getOptionsParams.-("path"))
      .load(sourcePath)
    logger.info("Data is successfully read from file")
    data
  }

  /** get data from file in streaming
    *
    * @return
    */
  override def getStream(): DataFrame = {
    val schemaInference = fetchConfigParams("schemaInference", getConfigParamMap())
      .getOrElse("true")
      .toBoolean
    removeOptions += "schemaInference"
    val sourcePath = getFilePath
    logger.info(s"Reading data from '$sourcePath' continuously")
    if (schemaInference) {
      extractor.spark.sql("set spark.sql.streaming.schemaInference=true")
      extractor.spark.readStream
        .format(getFormat)
        .options(getOptionsParams.-("path"))
        .load(sourcePath)
    } else {
      val colsDefinition = extractor.sourceConfig.schema.get
      var schema = new StructType()
      colsDefinition.foreach(x =>
        schema = schema
          .add(x.name, x.dataType)
      )
      extractor.spark.readStream
        .format(getFormat)
        .options(getOptionsParams.-("path"))
        .schema(schema)
        .load(sourcePath)
    }
  }
}
