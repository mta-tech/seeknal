package tech.mta.seeknal

import scala.io.Source

import org.apache.log4j.{Level, Logger, LogManager}
import org.apache.spark.sql.SparkSession

/** Base spark application, creates SparkSession and close it when done
  */
abstract class SparkApp(appName: String, session: SparkSession = null) {

  val spark: SparkSession = if (session != null) session else defaultSession

  lazy val logger: Logger = LogManager.getLogger(this.getClass)

  protected def defaultSession: SparkSession = {
    SparkSession
      .builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName(appName)
      .enableHiveSupport()
      .getOrCreate()
  }

  protected def init(): Unit = {
    spark.sparkContext.setLogLevel("ERROR")
    logger.setLevel(Level.INFO)
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    LogManager.getRootLogger.setLevel(Level.ERROR)
  }

  init()

  def close(): Unit = {
    spark.close()
  }

  /** Reads config file at a given path.
    *
    * The path can be in the classloader (jar) or spark classpath
    *
    * @param filepath
    *   the location of the config file
    */
  def loadConfig(filepath: String): String = {
    val classLoaderStream = this.getClass.getClassLoader
      .getResourceAsStream(filepath)

    if (classLoaderStream != null) {
      // Check config in classloader
      Source.fromInputStream(classLoaderStream).mkString
    } else {
      // Check config in spark classpath
      Source.fromFile(filepath).mkString
    }
  }

}
