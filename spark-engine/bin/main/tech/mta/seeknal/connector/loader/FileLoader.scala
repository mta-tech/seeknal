package tech.mta.seeknal.connector.loader

import org.apache.log4j.{Level, Logger, LogManager}
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.DataStreamWriter
import tech.mta.seeknal.connector.{BaseFile, IOMaterial}

/** Loader class for write result into file(s) Filesystem that currently support is same as that Spark natively
  * supported, otherwise will inform here.
  *
  * @param loader
  *   IOMaterial object
  */
class FileLoader(loader: IOMaterial) extends BaseFile(loader: IOMaterial) {

  require(loader.sourceConfig.params.isDefined, "params should contains configuration parameters")

  val logger: Logger = LogManager.getLogger(this.getClass)
  logger.setLevel(Level.INFO)

  setDefaultMode("overwrite")
  setDefaultStore("hdfs")

  /** write result to files
    */
  override def save(): Unit = {
    val targetPath = getFilePath
    logger.info(s"Writing data into '$targetPath'")
    if (loader.sourceConfig.partitions.isDefined) {
      loader.result.get.write
        .mode(getMode)
        .format(getFormat)
        .partitionBy(loader.sourceConfig.partitions.get: _*)
        .options(getOptionsParams.-("path"))
        .save(targetPath)
    } else {
      loader.result.get.write
        .mode(getMode)
        .format(getFormat)
        .options(getOptionsParams.-("path"))
        .save(targetPath)
    }

    logger.info("Data is successfully saved")
  }

  /** write result to file in streaming
    */
  override def saveStream(): DataStreamWriter[Row] = {
    val targetPath = getFilePath
    logger.info(s"Writing data into '$targetPath' continuously")

    // Path required here in options params
    var streamWriter = loader.result.get.writeStream
      .format(getFormat)
      .outputMode(getOutputMode)
      .options(getOptionsParams)

    if (loader.sourceConfig.partitions.isDefined) {
      streamWriter = streamWriter
        .partitionBy(loader.sourceConfig.partitions.get: _*)
    }
    streamWriter
  }
}
