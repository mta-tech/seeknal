package tech.mta.seeknal.connector.loader

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.streaming.DataStreamWriter
import tech.mta.seeknal.connector.{BaseIO, IOMaterial}
import tech.mta.seeknal.utils.FileUtils

/**
  * Loader class for write result into hive table
  *
  * @param loader
  */
class HiveLoader(loader: IOMaterial) extends BaseIO(loader: IOMaterial) {

  final val tablePath = loader.sourceConfig.getPath
  final val tablePartitions = loader.sourceConfig.partitions
  final val tableName = loader.sourceConfig.getTableName
  final val bucketing = loader.sourceConfig.bucketing
  final val params = loader.sourceConfig.params

  /**
    * Save to hive table
    */
  override def save(): Unit = {
    write(loader.result.get, loader.spark.version)
  }

  /**
    * Writes results to hive
    *
    * @param result the DataFrame obtained from various transformations
    * @param sparkVersion spark version
    */
  protected def write(result: DataFrame,
                      sparkVersion: String = "3.2.1"): Unit = {

    val sparkVersionNumber =
      ("""\d+(\.\d+\.\d+)+""".stripMargin.r findAllIn sparkVersion).toList.head
        .replaceAll("\\.", "")
        .toInt

    var outputPath = new Path(loader.spark.sqlContext.getConf("spark.sql.warehouse.dir"))
    if (tablePath != null) {
       outputPath = new Path(tablePath)
    }

    var savingMode = "overwrite"
    if(params.isDefined) {
      val configParameter = params.get.toMap
       if (configParameter.contains(mode)) {
         savingMode = configParameter(mode).asString.get.toLowerCase()
       }
    }

    val fs = FileUtils.getFs(outputPath, loader.spark)
    // spark 2.1 needs absolute path
    val fullOutputPath = FileUtils.makeQualified(outputPath, fs)

    // save to parquet
    val writer = new ParquetWriter(fs)
    val writeResult = writer.write(result, fullOutputPath, tableName,
      tablePartitions, sparkVersionNumber, bucketing,
      savingMode)
    if (writeResult.success) {
      // update hive metadata
      // TODO: make HiveTable accept SourceConfig
      sparkVersionNumber match {
        case version if version >= 230 =>
          new HiveTable(loader.spark, tableName, fullOutputPath, tablePartitions)
            .update(result)
        case _ =>
          val hasPartition: Option[Seq[String]] = tablePartitions match {
            case Some(columns) =>
              Some(Seq(columns.head))
            case None =>
              None
          }
          new HiveTable(
            loader.spark,
            tableName,
            fullOutputPath,
            hasPartition
          ).update(result)
      }
    } else {
      throw new RuntimeException(writeResult.message)
    }
  }

  /**
    * Throw an error when request save data in streaming
    */
  override def saveStream(): DataStreamWriter[Row] = {
    throw new NotImplementedError("Loader method is not available for streaming data source")
  }
}
