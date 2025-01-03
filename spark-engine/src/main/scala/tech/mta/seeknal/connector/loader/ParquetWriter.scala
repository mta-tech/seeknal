package tech.mta.seeknal.connector.loader

import java.util.UUID

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger, LogManager}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.{asc, col}
import tech.mta.seeknal.connector.IsWriteSuccess
import tech.mta.seeknal.pipeline.Bucketing

// scalastyle:off
/** Utility to write parquet files Mainly used to write dataframe to different partitions and overwrite existing data in
  * a partition. This is required since Overwrite mode in spark parquet writer will overwrite the whole directory,
  * instead of just the directory for specific partition
  *
  * @param fs
  *   the target filesystem
  */
class ParquetWriter(fs: FileSystem) {

  val logger: Logger = LogManager.getLogger(this.getClass)
  logger.setLevel(Level.INFO)

  /** Write dataframe as parquet
    *
    * Data in the existing partitions will be replaced
    *
    * @param df
    *   the DataFrame to write
    * @param path
    *   the parent path of the parquet
    * @param partition
    *   the partition column
    */
  def write(df: DataFrame,
            path: Path,
            tableName: String,
            partition: Option[Seq[String]],
            sparkVersion: Int = 230,
            bucketing: Option[Bucketing] = None,
            mode: String = "overwrite"
  ): IsWriteSuccess = {
    sparkVersion match {
      case version if version >= 230 =>
        writeParquet(df, path, tableName, partition, bucketing, mode)
      case _ =>
        if (partition.isDefined) {
          logger.warn(
            "Your SparkVersion is lower than 2.3.0 " +
              "therefore it is not supported to write multiple partitions " +
              "so it is only write the first partition column that you defined and " +
              "the rest will not partitioned"
          )
        }
        partition match {

          case Some(hasPartition) =>
            val selectedPartition = hasPartition.head
            val partitionValues = df
              .select(selectedPartition)
              .distinct()
              .orderBy(asc(selectedPartition))
              .collect()
              .map(row => row.get(0))

            if (partitionValues.isEmpty) {
              return IsWriteSuccess(false, "Column that is selected for partition contains empty value")
            }
            fs.mkdirs(path)
            val tmpDir = createTmpDir(path)
            try {
              val partitionPaths = partitionValues.map { partitionValue =>
                val partitionedData = df
                  .filter(df(selectedPartition) === partitionValue)
                  .drop(selectedPartition)
                val partitionPath = getPartitionPath(tmpDir, selectedPartition, partitionValue)
                // stop if any of the partition fails
                val writeResult = writeParquet(partitionedData, partitionPath, tableName, None, bucketing, mode)
                if (!writeResult.success) {
                  return writeResult
                }

                partitionPath
              }

              // move all tmp files back to it's intended place
              partitionPaths.foreach { tmpPath =>
                val targetPath = new Path(path, tmpPath.getName)
                if (fs.exists(targetPath)) {
                  fs.delete(targetPath, true)
                }
                logger.info(s"Moving tmp files from $tmpPath into $targetPath")
                fs.rename(tmpPath, targetPath)
              }
            } finally {
              fs.delete(tmpDir, true)
            }
            logger.info("Done")
            IsWriteSuccess(true, "Done")
          case None =>
            writeParquet(df, path, tableName, None, bucketing, mode)
        }
    }
  }

  private def createTmpDir(path: Path): Path = {
    val uuid = UUID.randomUUID().toString
    val tmpPath = new Path(path.getParent, s"${path.getName}-$uuid")

    // create temporary directory and make sure it's deleted at the end
    fs.mkdirs(tmpPath)
    tmpPath
  }

  /** Writes the dataframe as parquet. Overwrites existing data if any
    *
    * @param df
    *   the dataframe to write
    * @param tablePath
    *   the path
    */
  private def writeParquet(df: DataFrame,
                           tablePath: Path,
                           tableName: String,
                           partition: Option[Seq[String]],
                           bucketing: Option[Bucketing],
                           mode: String = "overwrite"
  ): IsWriteSuccess = {

    if (tablePath.equals(new Path(df.sparkSession.sqlContext.getConf("spark.sql.warehouse.dir")))) {
      writeManagedTable(df, tableName, partition, bucketing, mode)
    } else {
      writeExternalTable(df, tablePath, tableName, partition, bucketing, mode)
    }
  }

  /** Writes the dataframe as External Table. Overwrites existing data if any
    *
    * @param df
    *   the dataframe to write
    * @param tablePath
    *   the path
    * @param tableName
    *   table name
    * @param partition
    *   partition details
    * @param bucketing
    *   bucketing details
    */
  private def writeExternalTable(df: DataFrame,
                                 tablePath: Path,
                                 tableName: String,
                                 partition: Option[Seq[String]],
                                 bucketing: Option[Bucketing],
                                 mode: String = "overwrite"
  ): IsWriteSuccess = {
    logger.info(s"Writing partition data for $tablePath as external table")
    try {
      partition match {
        case Some(hasPartition) =>
          df.repartition(hasPartition.map(col): _*)
          df.sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
          bucketing match {
            case Some(hasBuckets) =>
              df.write
                .partitionBy(hasPartition: _*)
                .bucketBy(bucketing.get.buckets,
                          colName = hasBuckets.bucketBy.head,
                          colNames = hasBuckets.bucketBy.tail: _*
                )
                .sortBy(colName = bucketing.get.sortBy.head, colNames = bucketing.get.sortBy.tail: _*)
                .mode(mode)
                .option("path", tablePath.toString)
                .saveAsTable(tableName)
            case None =>
              df.write
                .partitionBy(hasPartition: _*)
                .mode(mode)
                .parquet(tablePath.toString)
          }
        case None =>
          bucketing match {
            case Some(hasBuckets) =>
              df.write
                .bucketBy(bucketing.get.buckets,
                          colName = hasBuckets.bucketBy.head,
                          colNames = hasBuckets.bucketBy.tail: _*
                )
                .sortBy(colName = bucketing.get.sortBy.head, colNames = bucketing.get.sortBy.tail: _*)
                .mode(mode)
                .option("path", tablePath.toString)
                .saveAsTable(tableName)
            case None =>
              df.write
                .mode(mode)
                .parquet(tablePath.toString)
          }

      }

      val result = fs.exists(tablePath)
      var message = ""
      if (result) {
        message = s"Finished writing data for $tablePath"
        logger.info(message)
      } else {
        message = s"Failed writing data for $tablePath"
        logger.error(message)
      }
      IsWriteSuccess(result, message)
    } catch {
      case e: Exception =>
        val message = s"Exception when writing partition $tablePath: ${e.getMessage}"
        logger.error(message)
        IsWriteSuccess(false, message)
    }
  }

  /** Writes the dataframe as Managed Table. Overwrites existing data if any
    *
    * @param df
    *   the dataframe to write
    * @param tableName
    *   table name
    * @param partition
    *   partition details
    * @param bucketing
    *   bucketing details
    */

  private def writeManagedTable(df: DataFrame,
                                tableName: String,
                                partition: Option[Seq[String]],
                                bucketing: Option[Bucketing],
                                mode: String = "overwrite"
  ): IsWriteSuccess = {
    logger.info(s"Writing partition data for $tableName as internal table")
    try {
      partition match {
        case Some(hasPartition) =>
          df.repartition(hasPartition.map(col): _*)
          df.sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
          bucketing match {
            case Some(hasBuckets) =>
              df.write
                .partitionBy(hasPartition: _*)
                .bucketBy(bucketing.get.buckets,
                          colName = hasBuckets.bucketBy.head,
                          colNames = hasBuckets.bucketBy.tail: _*
                )
                .sortBy(colName = bucketing.get.sortBy.head, colNames = bucketing.get.sortBy.tail: _*)
                .mode(mode)
                .saveAsTable(tableName)
            case None =>
              df.write
                .partitionBy(hasPartition: _*)
                .mode(mode)
                .saveAsTable(tableName)
          }
        case None =>
          bucketing match {
            case Some(hasBuckets) =>
              df.write
                .bucketBy(bucketing.get.buckets,
                          colName = hasBuckets.bucketBy.head,
                          colNames = hasBuckets.bucketBy.tail: _*
                )
                .sortBy(colName = bucketing.get.sortBy.head, colNames = bucketing.get.sortBy.tail: _*)
                .mode(mode)
                .saveAsTable(tableName)
            case None =>
              df.write
                .mode(mode)
                .saveAsTable(tableName)
          }

      }

      val result = df.sparkSession.catalog.tableExists(tableName)
      var message = ""
      if (result) {
        message = s"Finished writing data for $tableName"
        logger.info(message)
      } else {
        message = s"Failed writing data for $tableName"
        logger.error(message)
      }
      IsWriteSuccess(result, message)
    } catch {
      case e: Exception =>
        val message = s"Exception when writing partition $tableName: ${e.getMessage}"
        logger.error(message)
        IsWriteSuccess(false, message)
    }
  }

  private def getPartitionPath(path: Path, partition: String, value: Any): Path = {
    // partition path format is <column>=<value>
    val partitionPath = s"$partition=${value.toString}"
    new Path(path, partitionPath)
  }
}
