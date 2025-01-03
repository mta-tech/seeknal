package tech.mta.seeknal.utils

import scala.io.Source

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

object FileUtils {

  def getFs(path: Path, spark: SparkSession): FileSystem = {
    // scalastyle:off
    // sessionState is not available on spark 2.1, so we need to do it this way
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    // scalastyle:on
    path.getFileSystem(hadoopConf)
  }

  def makeQualified(path: Path, spark: SparkSession): Path = {
    val fs = getFs(path, spark)
    makeQualified(path, fs)
  }

  def makeQualified(path: Path, fs: FileSystem): Path = {
    path.makeQualified(fs.getUri, fs.getWorkingDirectory)
  }

  /** Reads config file at a given path.
    *
    * Will search in the following order:
    *   1. spark's default filesystem 2. spark's classpath 3. classloader
    *
    * @param filePath
    *   the location of the config file
    * @param spark
    *   the spark session, to read from spark's default filesystem
    */
  def readFile(filePath: String, spark: SparkSession = null): String = {
    // read from spark's default filesystem
    if (spark != null) {
      val path = new Path(filePath)
      val fs = getFs(path, spark)
      if (fs.exists(path)) {
        return Source.fromInputStream(fs.open(path)).mkString
      }
    }

    val classLoaderStream = this.getClass.getClassLoader
      .getResourceAsStream(filePath)
    if (classLoaderStream != null) {
      // Check config in classloader
      Source.fromInputStream(classLoaderStream).mkString
    } else {
      // Check config in spark classpath
      Source.fromFile(filePath).mkString
    }
  }
}
