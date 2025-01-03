package tech.mta.seeknal.pipeline

import scala.collection.mutable.ListBuffer

import io.circe.Json
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.Trigger
import tech.mta.seeknal.SparkApp
import tech.mta.seeknal.connector.{BaseSocket, IOMaterial, PipelineIO, SourceConfig}
import tech.mta.seeknal.params.{Config, JobEnvironment}

/** The main job to build features see createEngine for list of parameters
  */
case class SparkPipeline(appName: String,
                         configFiles: Seq[String],
                         session: SparkSession = null,
                         inputSourceId: String = "base_input",
                         outputSourceId: String = "base_output"
) extends SparkApp(appName, session) {

  private var preStages: ListBuffer[Transformer] = ListBuffer.empty[Transformer]

  private var postStages: ListBuffer[Transformer] =
    ListBuffer.empty[Transformer]

  var config: PipelineConfig = if (configFiles.nonEmpty) {
    PipelineConfig
      .fromFiles(files = configFiles, spark = spark, inputSourceId = inputSourceId, outputSourceId = outputSourceId)
      .head
  } else {
    PipelineConfig()
  }

  private var runtime: JobEnvironment = null

  def updateRuntime(newRuntime: JobEnvironment): SparkPipeline = {
    runtime = newRuntime
    this
  }

  def updatePipelineConfig(pipelineConfig: PipelineConfig): SparkPipeline = {
    config = config.overrideWith(pipelineConfig)
    this
  }

  def updatePipeline(pipeline: FeaturePipeline): SparkPipeline = {
    config = config.overridePipeline(pipeline)
    this
  }

  def emptiedPreStages(): SparkPipeline = {
    preStages = ListBuffer.empty[Transformer]
    this
  }

  def emptiedPostStages(): SparkPipeline = {
    postStages = ListBuffer.empty[Transformer]
    this
  }

  def emptiedStages(): SparkPipeline = {
    val currentFeaturePipeline = config.pipeline.get.stages
    if (currentFeaturePipeline.isDefined) {
      val emptied = config.pipeline.get.copy(stages = None)
      config = config.overridePipeline(emptied)
    }
    this
  }

  def getRuntime: JobEnvironment = runtime
  def getConfig: PipelineConfig = config.copy()

  def prependStages(newStages: Seq[Transformer]): SparkPipeline = {
    newStages ++=: preStages
    this
  }

  def updateStagesWithId(id: String, newStages: Seq[Transformer]): SparkPipeline = {
    config.updateStagesMap(id, newStages)
    this
  }

  def appendStages(newStages: Seq[Transformer]): SparkPipeline = {
    preStages ++= newStages
    this
  }

  /** run feature engine pipeline
    *
    * @param outputPartitions
    *   location of result output
    */
  def run(myConfig: Option[PipelineConfig] = None,
          maxResults: Int = 10,
          inputDF: Option[DataFrame] = None
  ): JobResult = {
    myConfig match {
      case Some(inputConfig) =>
        config = inputConfig
      case None =>
    }
    val pipelineType = config.pipeline.get.kind.getOrElse("batch")

    implicit val sparkSession: Option[SparkSession] = Some(spark)
    var jobEnvironment: JobEnvironment = null

    val input = inputDF match {
      case Some(value) =>
        new Pipeline()
          .setStages(preStages.toArray)
          .fit(value)
          .transform(value)
      case None =>
        val inputConfig = config.getInputSource
        val extractor = IOMaterial(spark, inputConfig)(runtime)
        jobEnvironment = extractor.getRuntime()

        pipelineType match {
          case Config.PIPELINE_BATCH =>
            extractor.limit(PipelineIO.get(extractor, preStages))
          case Config.PIPELINE_STREAMING =>
            extractor.limit(PipelineIO.getStream(extractor, preStages))
        }
    }

    // Apply user defined transformations
    val result = new Pipeline()
      .setStages(config.getStages.toArray)
      .fit(input)
      .transform(input)

    if (maxResults > 0) {
      JobResult(result.limit(maxResults), jobEnvironment)
    } else {
      JobResult(result, jobEnvironment)
    }
  }

  // TODO: handle outputPartitions in outputSource
  def write(result: DataFrame,
            outputPartitions: Seq[String] = Seq(),
            myConfig: Option[PipelineConfig] = None,
            maxResults: Int = 0) {
    myConfig match {
      case Some(inputConfig) =>
        config = inputConfig
      case None =>
    }
    val pipelineType = config.pipeline.get.kind.getOrElse("batch")
    if (maxResults > 0) {
      if (pipelineType == Config.PIPELINE_STREAMING) {
        val streamParams = Json.obj(("numRows", Json.fromInt(maxResults))).asObject
        val outputConfig = SourceConfig(source = Some("console"), params = streamParams)
        val sinkSetting = IOMaterial(spark, outputConfig, Some(result))
        val streamQuery = new BaseSocket(sinkSetting)
          .saveStream()
          .trigger(Trigger.Once)
          .start()

        streamQuery.awaitTermination()
      } else {
        result.show(maxResults)
      }
    } else {
      if (!config.getPipeline.output.isDefined) {
        throw new IllegalArgumentException("output pipeline is empty.")
      }
      val outputConfig = if (outputPartitions.isEmpty) {
        config.getOutputSource
      } else {
        config.getOutputSource.copy(partitions = Some(outputPartitions))
      }

      val loader = IOMaterial(spark, outputConfig, Some(result))(runtime)

      pipelineType match {
        case Config.PIPELINE_BATCH =>
          PipelineIO.save(loader, postStages)
        case Config.PIPELINE_STREAMING =>
          val streamWriter = PipelineIO.saveStream(loader, postStages)
          streamWriter.awaitTermination()
      }
    }
  }
}

case class JobResult(result: DataFrame, env: JobEnvironment = null)
