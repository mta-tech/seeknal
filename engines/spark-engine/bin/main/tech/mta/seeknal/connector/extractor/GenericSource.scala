package tech.mta.seeknal.connector.extractor

import org.apache.log4j.{Level, Logger, LogManager}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import tech.mta.seeknal.connector.{BaseFile, IOMaterial}

class GenericSource(extractor: IOMaterial) extends BaseFile(extractor: IOMaterial) {

  require(extractor.sourceConfig.params.isDefined, "params should contains configuration parameters ")

  val logger: Logger = LogManager.getLogger(this.getClass)
  logger.setLevel(Level.INFO)

  /** get data from generic connector
   *
   * @return
   */
  override def get(): DataFrame = {
    val data = extractor.spark.read
      .format(getFormat)
      .options(getOptionsParams)
      .load()
      data
  }

  /** get data from file in streaming
   *
   * @return
   */
  override def getStream(): DataFrame = {
    throw new NotImplementedError()
  }
}
