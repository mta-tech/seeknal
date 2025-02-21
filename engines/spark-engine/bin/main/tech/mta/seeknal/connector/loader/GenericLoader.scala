package tech.mta.seeknal.connector.loader

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.DataStreamWriter
import tech.mta.seeknal.connector.{BaseIO, IOMaterial}

class GenericLoader(loader: IOMaterial)
  extends BaseIO(loader: IOMaterial) {

  require(loader.sourceConfig.params.isDefined,
    "params should contains configuration parameters")

  val logger: Logger = LogManager.getLogger(this.getClass)
  logger.setLevel(Level.INFO)

  setDefaultMode("overwrite")

  /**
   * write result a table
   */
  override def save(): Unit = {
    val writer = if (loader.sourceConfig.partitions.isDefined) {
      loader.result.get.write
        .mode(getMode)
        .format(getFormat)
        .partitionBy(loader.sourceConfig.partitions.get: _*)
        .options(getOptionsParams)
    } else {
      loader.result.get.write
        .mode(getMode)
        .format(getFormat)
        .options(getOptionsParams)
    }

    if (getTableName != "id") {
      writer.saveAsTable(getTableName)
    } else {
      if (getFilePath != "") {
        writer.save(getFilePath)
      } else {
        writer.save()
      }
    }
  }

  /**
   * write result in streaming
   */
  override def saveStream(): DataStreamWriter[Row] = {
    throw new NotImplementedError()
  }
}
