package tech.mta.seeknal.connector

import scala.collection.mutable
import io.circe.{Json, JsonObject}
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}
import org.apache.spark.sql.functions.{coalesce, col, concat, first, lit}
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQuery, Trigger}
import tech.mta.seeknal.connector.extractor._
import tech.mta.seeknal.connector.loader._
import tech.mta.seeknal.connector.extractor.{FileSource, GenericSource, HiveSource}
import tech.mta.seeknal.connector.loader.{FileLoader, GenericLoader, HiveLoader, TemporaryTableLoader}
import tech.mta.seeknal.connector.serde.BaseSerDe
import tech.mta.seeknal.params.{Config, FeatureGroup, JobEnvironment, SerDeConfig}
import tech.mta.seeknal.pipeline.{DbConfig, PipelineConfig}

case class IsWriteSuccess(success: Boolean, message: String)

/** IO material
  *
  * @param spark
  * @param sourceConfig
  * @param result
  */
case class IOMaterial(spark: SparkSession, sourceConfig: SourceConfig, var result: Option[DataFrame] = None)(implicit
    runtime: JobEnvironment = null
) {
  var ioEnv = runtime

  /** if limit is specified then return a data by taking the first `n` rows
    *
    * @param df
    *   @return
    */
  def limit(df: DataFrame): DataFrame = {
    sourceConfig.limit match {
      case Some(limit) =>
        df.limit(limit)
      case None =>
        df
    }
  }

  /** update result
    *
    * @param df
    *   @return
    */
  def setResult(df: DataFrame): this.type = {
    result = Some(df)
    this
  }

  /** get runtime
    * @return
    */
  def getRuntime(): JobEnvironment = {
    ioEnv
  }

  def updateRuntime(obj: JobEnvironment): this.type = {
    ioEnv = obj
    this
  }
}

/** Interface for IO
  */
trait IO {
  def get(extractor: IOMaterial, preStages: Seq[Transformer]): DataFrame
  def save(loader: IOMaterial, postStages: Seq[Transformer]): Unit
  def getStream(extractor: IOMaterial, preStages: Seq[Transformer]): DataFrame
  def saveStream(loader: IOMaterial, postStages: Seq[Transformer]): StreamingQuery
}

trait BaseConnector {

  var removeOptions: mutable.Set[String] = mutable.Set()

  /** Parse params
    *
    * @param key
    *   @param data
    * @return
    */
  def fetchConfigParams(key: String, data: Map[String, Json]): Option[String] = {
    if (data.contains(key)) {
      val value: Json = data(key)

      if (value.isString) {
        Some(value.asString.get)
      } else if (value.isBoolean) {
        Some(value.asBoolean.get.toString)
      } else if (value.isNumber) {
        Some(value.asNumber.get.toInt.get.toString)
      } else if (value.isArray) {
        Some(value.asArray.get.mkString(","))
      } else {
        None
      }
    } else {
      None
    }
  }
}

/** BaseIO abstract class
  *
  * @param connector
  */
abstract class BaseIO(connector: IOMaterial) extends BaseConnector {

  val paramOption: Option[JsonObject] = connector.sourceConfig.params
  var mode: String = "mode"
  var defaultMode: String = "append"
  var format: String = "format"
  var path: String = "path"
  var defaultFormat = "csv"
  val outputModeKey: String = "outputMode"
  var defaultOutputMode = "append"
  val triggerKey: String = "processingTime"
  var partitionKey = "partitions"

  /** convert Json object to Map[String, Json]
    *
    * @return
    */
  def getConfigParamMap(): Map[String, Json] = paramOption.get.toMap

  /** define option params
    *
    * @return
    */
  def getOptionsParams: Map[String, String] = {
    val configParams = getConfigParamMap()
    var parameters: Map[String, String] = Map()
    val keys = configParams.keys
    for (key <- keys) {
      if (!removeOptions.contains(key)) {
        parameters += (key -> fetchConfigParams(key, configParams).get)
      }
    }
    parameters
  }

  /** Get mode key value
    */
  def getMode: String = {
    val configParams = getConfigParamMap()
    if (configParams.contains(mode)) {
      removeOptions += mode
      return configParams(mode).asString.get.toLowerCase()
    }
    defaultMode
  }

  /** get format key value
    *
    * @return
    */
  def getFormat: String = {
    val configParams = getConfigParamMap()
    if (configParams.contains(format)) {
      removeOptions += format
      return configParams(format).asString.get.toLowerCase()
    }
    defaultFormat
  }

  /** getOutputMode
    *
    * @return
    */
  def getOutputMode: String = {
    removeOptions += outputModeKey
    fetchConfigParams(outputModeKey, getConfigParamMap())
      .getOrElse(defaultOutputMode)
  }

  /** get path
    *
    * @return
    */
  def getFilePath: String = {
    val configParams: Map[String, Json] = getConfigParamMap()
    fetchConfigParams(path, configParams).get
  }

  def getTableName: String = {
    connector.sourceConfig.getTableName
  }

  /** set default mode
    *
    * @param value
    *   @return
    */
  def setDefaultMode(value: String): this.type = {
    defaultMode = value
    this
  }

  /** set default format
    *
    * @param value
    *   @return
    */
  def setDefaultFormat(value: String): this.type = {
    defaultFormat = value
    this
  }

  /** set default outputMode
    */
  def setDefaultOutputMode(value: String): this.type = {
    defaultOutputMode = value
    this
  }

  /** save interface
    */
  def save(): Unit = {
    connector.result.get.write
      .mode(getMode)
      .format(getFormat)
      .options(getOptionsParams)
      .parquet(getFilePath)
  }

  /** get interface
    */
  def get(): DataFrame = {
    connector.spark.read
      .format(getFormat)
      .options(getOptionsParams)
      .parquet(getFilePath)
  }

  /** Base streaming read interface
    *
    * @return
    */
  def getStream(): DataFrame = {
    connector.spark.readStream
      .format("socket")
      .options(getOptionsParams)
      .load()
  }

  /** Base streaming save interface
    */
  def saveStream(): DataStreamWriter[Row] = {
    connector.result.get.writeStream
      .format("console")
      .outputMode(getOutputMode)
  }
}

/** Read and write for pipeline
  */
object PipelineIO extends IO {

  /** initiate SerDe object
    *
    * @param className
    *   @param params
    * @return
    */
  def serializationImpl(className: String, params: JsonObject): BaseSerDe = {
    PipelineConfig
      .instantiateParams(className, params)
      .asInstanceOf[BaseSerDe]
  }

  /** serialize data
    *
    * @param serde
    *   @param data
    * @return
    */
  def serialize(serde: Option[Seq[SerDeConfig]], data: DataFrame): DataFrame = {
    serde match {
      case Some(serde) =>
        var stagedData = data
        serde.foreach(x => {
          stagedData = serializationImpl(x.className, x.params.get)
            .setSerialize(true)
            .transform(stagedData)
        })
        stagedData
      case None =>
        data
    }
  }

  /** deserialize data
    *
    * @param serde
    *   @param data
    * @return
    */
  def deserialize(serde: Option[Seq[SerDeConfig]], data: DataFrame): DataFrame = {
    serde match {
      case Some(serde) =>
        var stagedData = data
        serde.foreach(x => {
          val rawDeserialized = serializationImpl(x.className, x.params.get)
            .setSerialize(false)
            .transform(stagedData)
          stagedData = x.keepCols match {
            case Some(cols) =>
              val keepColsStr = cols :+ s"${Config.SERDE_TEMP_COL}.*"
              rawDeserialized.select(keepColsStr.head, keepColsStr.tail: _*)
            case None =>
              rawDeserialized.select(s"${Config.SERDE_TEMP_COL}.*")
          }
        })

        stagedData
      case None =>
        data
    }
  }

  /** initiate extractor object
    *
    * @param extractor
    *   @return
    */

  def getImpl(extractor: IOMaterial): BaseIO = {
    extractor.sourceConfig.source match {

      case Some("hive") | None =>
        new HiveSource(extractor)
      case Some("file") =>
        new FileSource(extractor)
      case Some("generic") =>
        new GenericSource(extractor)
      case _ =>
        throw new IllegalArgumentException(s"Invalid data source: ${extractor.sourceConfig.source.get}")
    }

  }

  /** initiate loader object
    *
    * @param loader
    *   @return
    */
  def saveImpl(loader: IOMaterial): BaseIO = {
    loader.sourceConfig.source match {
      case Some("hive") | None =>
        new HiveLoader(loader)
      case Some("file") =>
        new FileLoader(loader)
      case Some("temporaryTable") =>
        new TemporaryTableLoader(loader)
      case Some("console") =>
        new BaseSocket(loader)
      case Some("generic") =>
        new GenericLoader(loader)
      case _ =>
        throw new IllegalArgumentException(s"Invalid target data source: ${loader.sourceConfig.source}")
    }
  }

  /** read data from source
    *
    * @param extractor
    *   @return
    */
  override def get(extractor: IOMaterial, preStages: Seq[Transformer] = Seq[Transformer]()): DataFrame = {
    val reader: BaseIO = getImpl(extractor)
    var data = reader.get()
    data = new Pipeline()
      .setStages(preStages.toArray)
      .fit(data)
      .transform(data)
    if (extractor.sourceConfig.repartition.isDefined) {
      data = data.repartition(extractor.sourceConfig.repartition.get)
    }
    // deserialize col if specified
    val deserializeData: DataFrame = deserialize(extractor.sourceConfig.serde, data)
    extractor.limit(deserializeData)
  }

  /** write data to target source
    *
    * @param loader
    */
  override def save(loader: IOMaterial, postStages: Seq[Transformer] = Seq[Transformer]()): Unit = {
    // serialize column if specified
    val serializedData: DataFrame = serialize(loader.sourceConfig.serde, loader.result.get)
    var data = loader.limit(serializedData)
    data = new Pipeline()
      .setStages(postStages.toArray)
      .fit(data)
      .transform(data)
    if (loader.sourceConfig.repartition.isDefined) {
      data = data.repartition(loader.sourceConfig.repartition.get)
    }
    loader.setResult(data)

    val writer: BaseIO = saveImpl(loader)
    writer.save()
  }

  /** get streaming input
    *
    * @param extractor
    *   @return
    */
  override def getStream(extractor: IOMaterial, preStages: Seq[Transformer] = Seq[Transformer]()): DataFrame = {
    val reader: BaseIO = getImpl(extractor)
    var data = reader.getStream()
    data = new Pipeline()
      .setStages(preStages.toArray)
      .fit(data)
      .transform(data)
    if (extractor.sourceConfig.repartition.isDefined) {
      data = data.repartition(extractor.sourceConfig.repartition.get)
    }
    // deserialize col if specified
    deserialize(extractor.sourceConfig.serde, data)
  }

  /** save streaming
    *
    * @param loader
    */
  override def saveStream(loader: IOMaterial, postStages: Seq[Transformer] = Seq[Transformer]()): StreamingQuery = {
    // serialize column if specified
    var serializedData: DataFrame = serialize(loader.sourceConfig.serde, loader.result.get)
    serializedData = new Pipeline()
      .setStages(postStages.toArray)
      .fit(serializedData)
      .transform(serializedData)
    if (loader.sourceConfig.repartition.isDefined) {
      serializedData = serializedData.repartition(loader.sourceConfig.repartition.get)
    }
    loader.setResult(serializedData)

    var streamWriter = saveImpl(loader).saveStream()
    if (loader.sourceConfig.trigger.isDefined) {
      val streamingTrigger = loader.sourceConfig.trigger.get
      streamingTrigger.mode match {
        case Config.TRIGGER_ONCE =>
          streamWriter = streamWriter.trigger(Trigger.Once())
        case Config.TRIGGER_CONTINUOUS =>
          streamWriter = streamWriter.trigger(Trigger.Continuous(streamingTrigger.time.get))
        case Config.TRIGGER_PROCESSING_TIME | _ =>
          streamWriter = streamWriter.trigger(Trigger.ProcessingTime(streamingTrigger.time.get))
      }
    }

    streamWriter
      .start()
  }
}

/** Base class for File connector
  *
  * @param connector
  */
class BaseFile(connector: IOMaterial) extends BaseIO(connector: IOMaterial) {
  require(connector.sourceConfig.params.isDefined, "params should contains configuration parameters")

  final val relPath = "relPath"
  final val storeKey = "store"
  var defaultStore = "hdfs"

  /** Get file path with supporting relative path The relative path work only for store = file
    *
    * @return
    */
  override def getFilePath: String = {
    val configParams: Map[String, Json] = getConfigParamMap()

    val pathVal = fetchConfigParams(path, configParams).getOrElse(
      fetchConfigParams(relPath, configParams)
        .getOrElse(throw new IllegalStateException("Either path or relPath needs to be defined in params"))
    )

    fetchConfigParams(relPath, configParams) match {
      case Some(dir) =>
        val store =
          fetchConfigParams(storeKey, configParams).getOrElse(defaultStore)
        if (store == "file") {
          val relativePath = os.RelPath(dir)
          val absPath = os.pwd / relativePath
          s"${store}://" + absPath
        } else {
          s"${store}://" + relPath
        }
      case None =>
        pathVal
    }
  }

  /** set default store
    *
    * @param value
    *   @return
    */
  def setDefaultStore(value: String): this.type = {
    defaultStore = value
    this
  }
}

class BaseSocket(connector: IOMaterial) extends BaseIO(connector: IOMaterial) {

  /** overridden getConfigParamMap
    *
    * @return
    */
  override def getConfigParamMap(): Map[String, Json] = {
    paramOption match {
      case Some(map) =>
        map.toMap
      case None =>
        Map("" -> Json.fromString(""))
    }
  }
}

