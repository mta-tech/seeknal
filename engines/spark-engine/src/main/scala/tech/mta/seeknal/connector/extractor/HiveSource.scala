package tech.mta.seeknal.connector.extractor

import org.apache.spark.sql.DataFrame
import tech.mta.seeknal.connector.{BaseIO, IOMaterial}

/** Extractor class for read data from hive table
  *
  * @param extractor
  */
class HiveSource(extractor: IOMaterial) extends BaseIO(extractor: IOMaterial) {

  final val tableName = extractor.sourceConfig.getTableName

  /** get data from hive table
    * @return
    */
  override def get(): DataFrame = {
    extractor.spark.table(tableName)
  }

  /** Streaming is not implemented
    * @return
    */
  override def getStream(): DataFrame = {
    throw new NotImplementedError("Extractor method is not available for streaming data source")
  }
}
