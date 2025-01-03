package tech.mta.seeknal.pipeline

import scala.collection.mutable
import scala.util.{Failure, Success, Try}
import cats.syntax.either._
import io.circe.{Error, JsonObject, parser}
import io.circe.generic.JsonCodec
import io.circe.generic.auto._
import io.circe.yaml.{parser => yamlParser}
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{Column, SparkSession}
import tech.mta.seeknal.aggregators.BaseAggregator
import tech.mta.seeknal.connector.SourceConfig
import tech.mta.seeknal.params.{HasDateCol, HasEntityCol, HasIdCol, HasLocationCol}
import tech.mta.seeknal.transformers.GroupingTransformer
import tech.mta.seeknal.utils.{FileUtils, Utils}

/** Parses spark engine common config from a yaml string
  *
  * All attributes are mandatory so the yaml string must have those keys with the corresponding valid objects
  *
  * This will be merged into EngineConfig on https://eurekaanalytics.atlassian.net/browse/FEATENG-71
  *
  * @param sources
  *   the input
  * @param transformations
  *   the list of reusable transformations
  * @param pipeline
  *   the list of reusable transformations
  * @param rules
  *   the list of reusable rules
  * @param connection
  *   centralized credential config section for manage data connection
  */

case class PipelineConfig(sources: Option[Seq[SourceConfig]] = None,
                          transformations: Option[Seq[FeatureTransformer]] = None,
                          pipeline: Option[FeaturePipeline] = Some(FeaturePipeline()),
                          rules: Option[Seq[FeatureRule]] = None,
                          connection: Option[Seq[Configuration]] = None
) {

  var baseKeyInput = "base_input"
  var baseKeyOutput = "base_output"

  /** setter for baseKeyInput
    */
  def setBaseKeyInput(key: String): this.type = {
    baseKeyInput = key
    this
  }

  /** setter for baseKeyOutput
    */
  def setBaseKeyOutput(key: String): this.type = {
    baseKeyOutput = key
    this
  }

  // TODO: copy this when copying the whole PipelineConfig
  private val stagesMap: mutable.Map[String, Seq[PipelineStage]] =
    mutable.HashMap.empty[String, Seq[PipelineStage]]

  // TODO: apply rules to this as well
  val sourceMap: Map[String, SourceConfig] =
    sources.getOrElse(Seq()).map(src => (src.id.get, src)).toMap

  val transformationMap: Map[String, FeatureTransformer] =
    transformations.getOrElse(Seq()).map(trans => (trans.id.get, trans)).toMap

  val ruleMap: Map[String, FeatureRule] =
    rules.getOrElse(Seq()).map(rule => (rule.id, rule)).toMap

  val connMap: Map[String, Configuration] =
    connection
      .getOrElse(Seq())
      .map(configuration => (configuration.connId.get, configuration))
      .toMap

  def getSource(source: Option[SourceConfig], baseKey: String = null): SourceConfig = {
    if (source.isEmpty) {
      throw new IllegalStateException("Data source cannot be empty")
    }
    // return source if it's already defined
    if (source.get.source.isDefined | source.get.table.isDefined) {
      return source.get
    }
    // else find it in the table mapping

    var id: String = null
    if (source.get.id.isDefined) {
      id = source.get.id.get
    } else {
      return source.get
    }

    if (sourceMap.contains(id)) {
      // returns the source from common config mapping
      return sourceMap(id)
    } else {
      // TODO: apply params from base input / output as well
      if (sourceMap.contains(baseKey) && sourceMap(baseKey).db.isDefined) {
        val baseKeyDb = sourceMap(baseKey)
        val dbConfig = baseKeyDb.db.get
        val source = baseKeyDb.source.getOrElse("hive")

        if (!id.contains('.')) {
          // if the table name doesn't contain db name, include global db config
          return SourceConfig(id = Some(id), source = Some(source), table = Some(id), db = Some(dbConfig))
        }
      }
    }
    // treat the name as full table name
    SourceConfig(id = Some(id), table = Some(id))
  }

  def mergeJsonObjects(json1: JsonObject, json2: JsonObject): JsonObject = {
    var mergedJson = json1.toMap.++(json2.toMap)
    return JsonObject.fromMap(mergedJson)
  }

  def addConfigParams(sourceConfig: SourceConfig): SourceConfig = {
    if (!sourceConfig.connId.isDefined) {
      return sourceConfig
    }
    var id: String = sourceConfig.connId.get
    if (connMap.contains(id)) {
      val configuration = connMap(id)
      val configParams = configuration.params.getOrElse(JsonObject.empty)
      val sourceParams = sourceConfig.params.getOrElse(JsonObject.empty)
      val mergeParams = mergeJsonObjects(configParams, sourceParams)
      return sourceConfig.copy(params = Some(mergeParams))
    } else {
      throw new IllegalStateException("connId not found for " + id)
    }
    return sourceConfig
  }

  def getInputSource: SourceConfig = {
    addConfigParams(getSource(getPipeline.input, baseKeyInput))
  }

  def getOutputSource: SourceConfig = {
    addConfigParams(getSource(getPipeline.output, baseKeyOutput))
  }

  def getPipeline: FeaturePipeline = {
    if (pipeline.isEmpty) {
      throw new IllegalStateException("Pipeline cannot be empty")
    }
    pipeline.get
  }

  def updateStagesMap(id: String, stages: Seq[PipelineStage]): PipelineConfig = {
    stagesMap.put(id, stages)
    this
  }

  def overrideWith(that: PipelineConfig): PipelineConfig = {
    PipelineConfig(sources = Some(sources.getOrElse(Seq()) ++ that.sources.getOrElse(Seq())),
                   transformations = Some(transformations.getOrElse(Seq()) ++ that.transformations.getOrElse(Seq())),
                   pipeline = Some(pipeline.getOrElse(FeaturePipeline()).overrideWith(that.pipeline)),
                   rules = Some(rules.getOrElse(Seq()) ++ that.rules.getOrElse(Seq())),
                   connection = Some(connection.getOrElse(Seq()) ++ that.connection.getOrElse(Seq()))
    )
  }

  def applyParamRules(instance: Params): Params = {
    PipelineConfig.applyParamRules(instance, ruleMap)
  }

  def applyParams(instance: Params, param: JsonObject): Params = {
    PipelineConfig.configureParams(instance, param)
    instance
  }

  def overridePipeline(that: FeaturePipeline): PipelineConfig = {
    overrideWith(PipelineConfig(pipeline = Some(that)))
  }

  def getStages()(implicit spark: Option[SparkSession] = None): Seq[PipelineStage] = {
    pipeline.get.stages.getOrElse(Seq()).flatMap(createTransformer)
  }

  def getTransformer(id: Option[String]): Option[FeatureTransformer] = {
    // override value in common config with explicit value
    transformationMap.get(id.getOrElse(""))
  }

  private def createAggregator(agg: Aggregator)(implicit spark: Option[SparkSession]): Seq[Column] = {
    val aggregator: Params = getInputSource.params match {
      case Some(inputSource) =>
        val initTransformer = applyParamRules(
          // override value in stage params with rule params
          PipelineConfig.instantiateParams[BaseAggregator](agg.className, agg.params.getOrElse(JsonObject.empty))
        )
        // override value in stage params with input params
        applyParams(initTransformer, inputSource)
      case None =>
        applyParamRules(
          // override value in stage params with rule params
          PipelineConfig.instantiateParams[PipelineStage](agg.className, agg.params.getOrElse(JsonObject.empty))
        )
    }
    aggregator
      .asInstanceOf[BaseAggregator]
      .getAll()
  }

  private def createTransformer(
      featureTransformer: FeatureTransformer
  )(implicit spark: Option[SparkSession]): Seq[PipelineStage] = {

    if (
      featureTransformer.id.isDefined ||
      featureTransformer.aggregators.isDefined ||
      featureTransformer.className.isDefined ||
      featureTransformer.params.isDefined
    ) {
      // TODO: bad hack
      if (stagesMap.contains(featureTransformer.id.getOrElse(""))) {
        return stagesMap(featureTransformer.id.get)
      }

      // try finding referenced transformer if any
      val commonTransformer = getTransformer(featureTransformer.id).getOrElse(FeatureTransformer())
      // and update it with the specific one
      val updatedTransformer = commonTransformer.update(featureTransformer)

      val transformer: Params = getInputSource.params match {
        case Some(inputSource) =>
          val initTransformer = applyParamRules(
            // override value in stage params with rule params
            PipelineConfig.instantiateParams[PipelineStage](updatedTransformer.className.get,
                                                            updatedTransformer.params.getOrElse(JsonObject.empty)
            )
          )
          // override value in stage params with input params
          applyParams(initTransformer, inputSource)
        case None =>
          applyParamRules(
            // override value in stage params with rule params
            PipelineConfig.instantiateParams[PipelineStage](updatedTransformer.className.get,
                                                            updatedTransformer.params.getOrElse(JsonObject.empty)
            )
          )
      }

      if (featureTransformer.aggregators.isDefined) {
        val aggregators = featureTransformer.aggregators.get.flatMap(createAggregator)
        Seq(
          transformer
            .asInstanceOf[GroupingTransformer]
            .withAggregators(aggregators)
            .asInstanceOf[PipelineStage]
        )
      } else {
        Seq(transformer.asInstanceOf[PipelineStage])
      }
    } else {
      Seq[PipelineStage]()
    }

  }

}

object PipelineConfig {

  def fromString(string: String,
                 inlineParams: Option[Map[String, Either[String, Map[Boolean, Seq[String]]]]] = None
  ): PipelineConfig = {
    if (string == null || string.isEmpty) {
      PipelineConfig()
    } else {

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
          .flatMap(_.as[PipelineConfig])
          .valueOr(throw _)
      } match {
        case Success(yaml) =>
          yaml
        case Failure(err) =>
          parser
            .parse(replacedStr)
            .leftMap(err => err: Error)
            .flatMap(_.as[PipelineConfig])
            .valueOr(throw _)
      }
    }
  }

  def fromFiles(files: Seq[String],
                spark: SparkSession,
                inlineParams: Option[Map[String, Either[String, Map[Boolean, Seq[String]]]]] = None,
                inputSourceId: String = "base_input",
                outputSourceId: String = "base_output"
  ): Seq[PipelineConfig] = {
    files.foldLeft(Seq[PipelineConfig]()) { (current, filePath) =>
      val parsed = fromString(FileUtils.readFile(filePath, spark), inlineParams)
        .setBaseKeyInput(inputSourceId)
        .setBaseKeyOutput(outputSourceId)
      // current.overrideWith(parsed)
      current :+ parsed
    }
  }

  /** Apply applicable param rules defined in `rules`
    *
    * @param instance
    *   the instance to be configured
    * @param ruleMap
    *   mapping of rule name and rule to be applied
    */
  def applyParamRules(instance: Params, ruleMap: Map[String, FeatureRule]): Params = {
    instance.params.foreach { p =>
      val param = instance.getParam(p.name)
      val rule = ruleMap.get(param.name)
      if (rule.isDefined && !instance.isSet(param)) {
        // only set if the value is not explicitly set
        // this will also override the default value defined in the code
        val jsonValueString = rule.get.rule.toMap("value").noSpaces
        instance.set(param, param.jsonDecode(jsonValueString))
      }
    }
    instance
  }

  /** Configure Params object with the given settings
    *
    * @param instance
    *   the Params instance to be configured
    * @param params
    *   the settings to be applied
    */
  def configureParams(instance: Params, params: JsonObject): Unit = {
    params.toList.foreach { case (paramName, jsonValue) =>
      if (instance.hasParam(paramName)) {
        val param = instance.getParam(paramName)
        val value = param.jsonDecode(jsonValue.toString())
        instance.set(param, value)
      }
    }
  }

  /** Create new instance of a specified class and set its corresponding parameters
    *
    * @param className
    *   the full class name of class to be initialized
    * @param params
    *   the settings to be applied
    */
  def instantiateParams[T](className: String, params: JsonObject): T = {
    val cls = Class.forName(className)
    val instance = cls
      .getConstructor()
      .newInstance()
      .asInstanceOf[Params]
    configureParams(instance, params)
    instance.asInstanceOf[T]
  }

}

// TODO: find better names for Feature*, they are not just in use for feature engine
case class FeatureTransformer(
    // must be unique, if specified will be used to identify / reference transformer
    id: Option[String] = None,
    // settings to be applied to the transformer
    params: Option[JsonObject] = None,
    // name of the transformer, must be supplied if id is not supplied
    className: Option[String] = None,
    // list of aggregations, if this transformer is GroupingTransformer
    aggregators: Option[Seq[Aggregator]] = None
) {

  def update(that: FeatureTransformer): FeatureTransformer = {
    FeatureTransformer(id = if (that.id.isDefined) that.id else id,
                       params = if (that.params.isDefined) that.params else params,
                       className = if (that.className.isDefined) that.className else className,
                       aggregators = if (that.aggregators.isDefined) that.aggregators else aggregators
    )
  }

}

case class Aggregator(className: String, name: Option[String], params: Option[JsonObject] = None)

case class FeaturePipeline(input: Option[SourceConfig] = None,
                           output: Option[SourceConfig] = None,
                           stages: Option[Seq[FeatureTransformer]] = None,
                           kind: Option[String] = None
) {

  def overrideWith(maybeThat: Option[FeaturePipeline]): FeaturePipeline = {
    if (maybeThat.isEmpty) {
      this
    } else {
      val that = maybeThat.get
      FeaturePipeline(input = if (that.input.isDefined) that.input else input,
                      output = if (that.output.isDefined) that.output else output,
                      stages = if (that.stages.isDefined) that.stages else stages,
                      kind = if (that.kind.isDefined) that.kind else kind
      )
    }
  }

}

case class Configuration(connId: Option[String] = None, params: Option[JsonObject] = None)

// TODO: break this down into multiple classes, depending on the data source's type
class SourceProperties(override val uid: String)
    extends HasIdCol
    with HasDateCol
    with HasEntityCol
    with HasLocationCol {

  def this() = this(Identifiable.randomUID("SourceProperties"))
  override def copy(extra: ParamMap): SourceProperties = defaultCopy(extra)

  // these are the defaults from feature engine outputs
  setDefault(dateCol, "day")
  setDefault(idCol, "id")
  setDefault(entityCol, "entity")
}

case class DbConfig(
    // database name
    name: String,
    // parent path of all external tables in the dataset, if empty, all tables will be managed tables
    path: Option[String]
)

case class FeatureRule(
    // should be unique
    id: String,
    // free-form json object with `value` key, eg. {value: <actual values>}
    rule: JsonObject
)

case class Bucketing(buckets: Int, bucketBy: Seq[String], sortBy: Seq[String])
