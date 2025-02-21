package tech.mta.seeknal.connector.loader

import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.DataStreamWriter
import tech.mta.seeknal.connector.{BaseIO, IOMaterial}

/** loader for create a temporary table within SparkSession
  *
  * @param loader
  */
class TemporaryTableLoader(loader: IOMaterial) extends BaseIO(loader: IOMaterial) {

  override def save(): Unit = {
    // unpersisting the possibly cached dataset.
    loader.result.get.sparkSession.catalog.dropTempView(loader.sourceConfig.table.get)
    // persist result dataframe into temporary view
    loader.result.get.createOrReplaceTempView(loader.sourceConfig.table.get)
  }

  /** Throw an error when request save data in streaming
    */
  override def saveStream(): DataStreamWriter[Row] = {
    throw new NotImplementedError("Loader method is not available for streaming data source")
  }
}
