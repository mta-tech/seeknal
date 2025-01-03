package tech.mta.seeknal.pipeline

import scala.util.{Failure, Success, Try}

import cats.syntax.either._
import io.circe.{parser, Error, JsonObject}
import io.circe.generic.auto._
import io.circe.yaml.{parser => yamlParser}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.Params
import org.apache.spark.sql.{DataFrame, SparkSession}
import tech.mta.seeknal.features.FeatureGenerator
import tech.mta.seeknal.transformers.RegexFilter
import tech.mta.seeknal.utils.{FileUtils, Utils}

/** Parses spark engine config from a yaml string
  *
  * All attributes are mandatory so the yaml string must have those keys with the corresponding valid objects
  *
  * @param feature
  *   specifies the type of feature to be generated
  * @param pipeline
  *   overrides the pipeline section from pipeline config, normally used to specify input and output
  */
case class EngineConfig(pipeline: FeaturePipeline,
                        feature: Option[Feature] = None,
                        connection: Option[Seq[Configuration]] = None
) {

  var params: Params = _

  override def toString: String = {
    feature match {
      case Some(setup) =>
        s"name=${setup.name}, features=(${setup.subFeatures.getOrElse(Seq()).mkString(",")})"
      case None =>
        "" // scalastyle:off
    }
  }

  // TODO: should keep an instance of pipelineConfig that's updated with its FeaturePipeline
  def getFeatures(pipelineConfig: PipelineConfig)(implicit spark: Option[SparkSession]): Seq[Transformer] = {

    feature match {
      case Some(setup) =>
        // set priority params in this order:
        // explicit > common > rule > default

        // apply rule
        pipelineConfig.applyParamRules(setup.featureGenerator)
        // apply input config params (the common)
        pipelineConfig.applyParams(setup.featureGenerator,
                                   pipelineConfig.getInputSource.params.getOrElse(JsonObject.empty)
        )
        // apply feature config companion params (the explicit)
        pipelineConfig.applyParams(setup.featureGenerator, setup.params.getOrElse(JsonObject.empty))

        setup.featureGenerator.configureParams(pipelineConfig)
        params = setup.featureGenerator
        setup.getTransformers
      case None =>
        Seq[Transformer]()
    }
  }

}

object EngineConfig {

  def fromString(string: String,
                 inlineParams: Option[Map[String, Either[String, Map[Boolean, Seq[String]]]]] = None
  ): EngineConfig = {

    val replacedStr: String = inlineParams match {
      case Some(params) =>
        Utils.replaceStringParams(params, string)
      case None =>
        string
    }

    Try {
      yamlParser
        .parse(replacedStr)
        .leftMap(err => err: Error)
        .flatMap(_.as[EngineConfig])
        .valueOr(throw _)
    } match {
      case Success(yaml) =>
        yaml
      case Failure(err) =>
        parser
          .parse(replacedStr)
          .leftMap(err => err: Error)
          .flatMap(_.as[EngineConfig])
          .valueOr(throw _)
    }
  }

  def fromFile(filePath: String,
               spark: SparkSession,
               inlineParams: Option[Map[String, Either[String, Map[Boolean, Seq[String]]]]] = None
  ): EngineConfig = {
    fromString(FileUtils.readFile(filePath, spark), inlineParams)
  }

  def runPipeline(pipeline: SparkPipeline,
                  filters: Seq[Transformer],
                  config: EngineConfig,
                  maxResults: Int = 10,
                  inputDF: Option[DataFrame] = None
  ): JobResult = {

    val pipelineInstance = pipeline
      .emptiedPreStages()
      .emptiedStages()
      .emptiedPostStages()
      // add dateFilter
      .prependStages(filters)
    config.feature match {
      case Some(setup) =>
        pipelineInstance
          // add feature specific transformers into the pipeline
          .updateStagesWithId("feature", config.getFeatures(pipeline.getConfig)(Some(pipeline.spark)))
          .run(maxResults = maxResults, inputDF = inputDF)
      case None =>
        pipelineInstance
          .run(maxResults = maxResults, inputDF = inputDF)
    }
  }

  def materializePipeline(result: DataFrame,
                          pipeline: SparkPipeline,
                          config: EngineConfig,
                          maxResults: Int = 0) = {
    val partitionCols = if (maxResults > 0) {
      None
    } else {
      if (config.pipeline.output.isDefined) {
        config.pipeline.output.get.partitions
      } else {
        None
      }
    }
    config.feature match {
      case Some(setup) =>
        pipeline.write(result = result,
                       outputPartitions = partitionCols.getOrElse(Seq(setup.frequency)),
                       maxResults = maxResults)
      case None =>
        pipeline.write(result = result,
                       outputPartitions = partitionCols.getOrElse(Seq()),
                       maxResults = maxResults)
    }
  }

  def getConditionFilters(conditions: Option[Map[String, String]]): Seq[Transformer] = {
    conditions match {
      case Some(condition) =>
        condition.foldLeft(Seq.empty[Transformer])(op = (seq, value) => {
          val res = Seq(RegexFilter.fromString(value._1, value._2))
          seq ++ res
        })
      case None =>
        Seq.empty[Transformer]
    }
  }

}

case class Feature(name: String,
                   frequency: String,
                   // settings to be applied to the transformer
                   params: Option[JsonObject] = None,
                   subFeatures: Option[Seq[SubFeature]] = None
) {

  val featureGenerator: FeatureGenerator = getFeatureGenerator

  private def getFeatureGenerator: FeatureGenerator = {

    val generatorClass: Option[Class[_]] = Try { Some(Class.forName(name)) } match {
      case Success(value) =>
        value
      case Failure(exception) =>
        throw new IllegalArgumentException(s"Feature '$name' is not defined")
    }

    val param = generatorClass.get
      .getConstructor()
      .newInstance()
      .asInstanceOf[Params]
    PipelineConfig.configureParams(param, params.getOrElse(JsonObject.empty))
    param.asInstanceOf[FeatureGenerator]
  }

  def getTransformers(implicit spark: Option[SparkSession]): Seq[Transformer] = {
    featureGenerator.getTransformers(frequency, subFeatures)
  }

}

// TODO: test this
case class SubFeature(name: String, dimensions: Option[Seq[FeatureDimension]] = None) {

  def getFeatures(prefix: String, excludedDimensions: Seq[FeatureDimension]): Seq[String] = {
    val filteredDimensions =
      dimensions.getOrElse(Seq()).filterNot(excludedDimensions.contains)
    constructFeatureNames(Seq(prefix + name), filteredDimensions)
  }

  def getFeatures(prefix: String = ""): Seq[String] = {
    getFeatures(prefix, Seq())
  }

  override def toString: String = name

  private def constructFeatureNames(initialFeatures: Seq[String], dimensions: Seq[FeatureDimension]): Seq[String] = {

    dimensions match {
      case dimension :: tail =>
        constructFeatureNames(appendDimension(initialFeatures, dimension), tail)
      case Nil => initialFeatures
    }
  }

  private def appendDimension(currentFeatures: Seq[String], dimension: FeatureDimension): Seq[String] = {

    for {
      feature <- currentFeatures
      dimensionValue <- dimension.values.getOrElse(Seq[String]())
    } yield {
      if (feature.isEmpty) {
        dimensionValue
      } else {
        s"${feature}_$dimensionValue"
      }
    }
  }

}

case class FeatureDimension(name: String, values: Option[Seq[String]] = None)
