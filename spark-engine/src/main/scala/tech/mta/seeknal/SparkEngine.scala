package tech.mta.seeknal

import scala.collection.JavaConverters._
import scala.util.{Failure, Left, Success, Try}
import com.typesafe.config.{Config, ConfigRenderOptions}
import io.circe.{Json, JsonObject}
import io.circe.generic.auto._
import io.circe.syntax._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.ml.Transformer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.storage.StorageLevel
import tech.mta.seeknal
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import tech.mta.seeknal.connector.{IOMaterial, PipelineIO, SourceConfig}
import tech.mta.seeknal.params.{JobEnvironment, Config => myConstant}
import tech.mta.seeknal.pipeline.{EngineConfig, FeaturePipeline, PipelineConfig, SparkPipeline}
import tech.mta.seeknal.transformers.{RegexFilter, StructAssembler}
import tech.mta.seeknal.utils.FileUtils
import tech.mta.seeknal.utils.Utils.{getAuthorFromConfig, parseRangeDate}

/** The main job to build features see createEngine for list of parameters
  */

case class JobSpec(file: Option[String], string: Option[String])
case class CommonObject(file: Option[String] = None, string: Option[String] = None)
case class DataframeContent(content: Array[String], schema: Array[String])

case class JobResult(content: Map[String, Seq[DataFrame]], jobEnvironment: Option[JobEnvironment] = None) {
  def get(dateString: String = ""): Seq[DataFrame] = {
    if (dateString == "") {
      content.head._2
    } else {
      content(dateString)
    }
  }
  def getDateString(): Seq[String] = {
    content.keys.toList
  }
}

case class SparkEngine(jobSpec: Seq[JobSpec],
                         filter: Option[Map[String, String]] = None,
                         maxResults: Int = 10,
                         commonObject: CommonObject = CommonObject(),
                         range: Seq[String] = Seq(""),
                         datePattern: String,
                         inlineParam: Option[Map[String, String]] = None,
                         appName: String = SparkEngine.DEFAULT_NAME,
                         inputSourceId: Option[String] = None,
                         outputSourceId: Option[String] = None,
                         enableHivemall: Boolean = false
                        )(implicit session: SparkSession = null) {

  val logger: Logger = LogManager.getLogger(this.getClass)
  logger.setLevel(Level.INFO)
  val sparkPipeline: SparkPipeline = {
    val baseInput = inputSourceId.getOrElse("base_input")
    val baseOutput = outputSourceId.getOrElse("base_output")

    commonObject match {
      case CommonObject(Some(file), None) =>
        val commConfigFile = (Seq() :+ file)
          .filter(c => c != null)
        new SparkPipeline(appName, commConfigFile, session, baseInput, baseOutput)
      case CommonObject(None, Some(string)) =>
        val commConfigFile = jobSpec.head match {
          case JobSpec(Some(file), None) =>
            Seq(file)
          case _ =>
            Seq()
        }
        val sparkPipeline = new SparkPipeline(appName, commConfigFile, session, baseInput, baseOutput)
        val pipelineConfig = PipelineConfig
          .fromString(string)
          .setBaseKeyInput(baseInput)
          .setBaseKeyOutput(baseOutput)
        sparkPipeline.updatePipelineConfig(pipelineConfig)
      case CommonObject(None, None) | _ =>
        val commConfigFile = jobSpec.head match {
          case JobSpec(Some(file), None) =>
            Seq(file)
          case _ =>
            Seq()
        }
        val sparkPipeline = new SparkPipeline(appName, commConfigFile, session, baseInput, baseOutput)
        jobSpec.head match {
          case JobSpec(None, Some(string)) =>
            val pipelineConfig = PipelineConfig.fromString(string)
            sparkPipeline.updatePipelineConfig(pipelineConfig)
          case _ =>
            sparkPipeline
        }
    }
  }

  /** Initiate Spark Pipeline object and inline parameter given either common config file or common config string being
   * specified
   */
  def initSparkPipeline(): SparkPipeline = {
    val baseInput = inputSourceId.getOrElse("base_input")
    val baseOutput = outputSourceId.getOrElse("base_output")

    commonObject match {
      case CommonObject(Some(file), None) =>
        val commConfigFile = (Seq() :+ file)
          .filter(c => c != null)
        new SparkPipeline(appName, commConfigFile, session, baseInput, baseOutput)
      case CommonObject(None, Some(string)) =>
        val commConfigFile = jobSpec.head match {
          case JobSpec(Some(file), None) =>
            Seq(file)
          case _ =>
            Seq()
        }
        val sparkPipeline = new SparkPipeline(appName, commConfigFile, session, baseInput, baseOutput)
        val pipelineConfig = PipelineConfig
          .fromString(string)
          .setBaseKeyInput(baseInput)
          .setBaseKeyOutput(baseOutput)
        sparkPipeline.updatePipelineConfig(pipelineConfig)
      case CommonObject(None, None) | _ =>
        val commConfigFile = jobSpec.head match {
          case JobSpec(Some(file), None) =>
            Seq(file)
          case _ =>
            Seq()
        }
        val sparkPipeline = new SparkPipeline(appName, commConfigFile, session, baseInput, baseOutput)
        jobSpec.head match {
          case JobSpec(None, Some(string)) =>
            val pipelineConfig = PipelineConfig.fromString(string)
            sparkPipeline.updatePipelineConfig(pipelineConfig)
          case _ =>
            sparkPipeline
        }
    }
  }

  def getInlineParams()(implicit pipeline: SparkPipeline): Map[String, Either[String, Map[Boolean, Seq[String]]]] = {
    // if rules defined, set rules as default inlineParam
    var inlineParams: Map[String, Either[String, Map[Boolean, Seq[String]]]] =
      if (pipeline.config.rules.isDefined) {
        pipeline.config.ruleMap
          .foldLeft(Map[String, Either[String, Map[Boolean, Seq[String]]]]())(op = (ruleMap, rule) => {
            val jsonValue = rule._2.rule.toMap("value")
            val valueString = jsonValue match {
              case a if jsonValue.isArray == true =>
                a.asArray.get.toArray.map(_.noSpaces.replace("\"", "\'")).mkString("(", ",", ")")
              case _ =>
                jsonValue.noSpaces.replace("\"", "")
            }
            val AddruleMap = Map(rule._1 -> Left(valueString))
            ruleMap ++ AddruleMap
          })
      } else {
        Map[String, Either[String, Map[Boolean, Seq[String]]]]()
      }

    // if user defined inlineParam overwrite any same key with user inlineParam
    if (inlineParam.isDefined) {
      inlineParam.get.foreach(param => {
        inlineParams = inlineParams ++ Map(param._1 -> Left(param._2))
        inlineParams.updated(param._1, Left(param._2))
      })
    }
    inlineParams
  }

  def transform(input: Option[DataFrame] = None, dateCol: String = "day")
               (implicit pipeline: SparkPipeline = null): JobResult = {

    val myPipeline = if (pipeline == null) {
      this.sparkPipeline
    } else {
      pipeline
    }

    var inlineParams = getInlineParams()(myPipeline)

    val rangeLoop = if (range.isEmpty) {
      Seq(null)
    } else {
      range
    }

    val rangeResult = rangeLoop
      .map(rangeStr => {
        val dateByString: Seq[String] = if (rangeStr != null) {
          parseRangeDate(rangeStr, datePattern)
        } else {
          Seq("")
        }

        dateByString
          .map(thisDate => {
            if (thisDate != "") {
              logger.info(s"Running for date=$thisDate")
              inlineParams = Map("date" -> Left(thisDate)) ++ inlineParams
            }

            val result = jobSpec.map(spec => {
              val config = spec match {
                case JobSpec(Some(file), None) =>
                  logger.info(s"Running: $file")
                  EngineConfig.fromFile(file, myPipeline.spark, Some(inlineParams))
                case JobSpec(None, Some(str)) =>
                  EngineConfig.fromString(str, Some(inlineParams))
                case JobSpec(Some(file), Some(str)) =>
                  logger.info(s"Running: $file")
                  EngineConfig.fromFile(file, myPipeline.spark, Some(inlineParams))
                case JobSpec(None, None) | _ =>
                  throw new Exception("JobSpec should specified as file or string")
              }
              myPipeline.updatePipeline(config.pipeline)
              if (input.isDefined) {
                val newFeaturePipeline = FeaturePipeline(
                  input = Some(
                    SourceConfig(params = Json.obj(
                      ("dateCol", Json.fromString(dateCol))
                    ).asObject)),
                  output = myPipeline.config.pipeline.get.output,
                  stages = myPipeline.config.pipeline.get.stages,
                  kind = myPipeline.config.pipeline.get.kind
                )
                myPipeline.updatePipeline(newFeaturePipeline)
              }
              val conditionFilter = EngineConfig.getConditionFilters(filter)
              val dateFilters = if (thisDate != "") {
                Seq(RegexFilter.fromString(myPipeline.getConfig.getInputSource.sourceProperties.getDateCol, thisDate))
                  .filter(_ != null)
              } else {
                Seq[Transformer]()
              }
              val jobResult = EngineConfig.runPipeline(myPipeline, dateFilters ++ conditionFilter, config,
                maxResults, input)
              myPipeline.updateRuntime(jobResult.env)
              jobResult.result
            })
            (thisDate -> result)
          })
          .toMap
      })
      .reduce((x, y) => x ++ y)

    JobResult(rangeResult, Some(myPipeline.getRuntime))
  }

  /** Main run feature engine job. The job spec specified either from files or string
   *
   * @return
   * returns as Map with date as key and value either string or the result dataframe
   */
  def run()(implicit pipeline: SparkPipeline = null) = {
    val results = transform()
    materialize(results.content)
  }

  def materialize(results: Map[String, Seq[DataFrame]])(implicit pipeline: SparkPipeline = null) = {
    val myPipeline = if (pipeline == null) {
      this.sparkPipeline
    } else {
      pipeline
    }
    var inlineParams = getInlineParams()(myPipeline)

    val rangeLoop = if (range.isEmpty) {
      Seq(null)
    } else {
      range
    }

    rangeLoop
      .foreach(rangeStr => {
        val dateByString: Seq[String] = if (rangeStr != null) {
          parseRangeDate(rangeStr, datePattern)
        } else {
          Seq("")
        }

        dateByString
          .foreach(thisDate => {
            if (thisDate != "") {
              logger.info(s"Materialize for date=$thisDate")
            }

            if (thisDate != "") {
              inlineParams = Map("date" -> Left(thisDate)) ++ inlineParams
            }

            jobSpec.zipWithIndex.foreach(spec => {
              val config = spec._1 match {
                case JobSpec(Some(file), None) =>
                  logger.info(s"Running: $file")
                  EngineConfig.fromFile(file, myPipeline.spark, Some(inlineParams))
                case JobSpec(None, Some(str)) =>
                  EngineConfig.fromString(str, Some(inlineParams))
                case JobSpec(Some(file), Some(str)) =>
                  logger.info(s"Running: $file")
                  EngineConfig.fromFile(file, myPipeline.spark, Some(inlineParams))
                case JobSpec(None, None) | _ =>
                  throw new Exception("JobSpec should specified as file or string")
              }
              myPipeline.updatePipeline(config.pipeline)
              EngineConfig.materializePipeline(results(thisDate)(spec._2), myPipeline, config, maxResults)
            })
          })
      })
  }

  /** Returns the feature engine job result as records organized by date and job spec. The format of result will like
   * this: Map( "<date>" -> Seq( "<job_spec_one>" -> Array("{{\"column_one\": \"value\", ...}"}), ... ) )
   * @return
   *   collection.Map (see above for the format)
   */
  def show()(implicit pipeline: SparkPipeline = null): (Map[String, Seq[Map[String, Array[String]]]], JobEnvironment) = {
    val result = transform()(pipeline)
    val output = if (maxResults > 0) {
      result.content.map(x => {
        val collection = x._2.map(y => {
          val content = y.limit(myConstant.MAX_RESULT).toJSON.collect()
          val schema = y.schema.fields.map(f => {
            Array(s"'${f.name}'", s"'${f.dataType.sql}'").mkString("{", ":", "}")
          })
          Map("content" -> content, "schema" -> schema)
        })
        (x._1, collection)
      })
    } else {
      Map("" -> Seq(Map("content" -> Array[String](), "schema" -> Array[String]())))
    }
    (output, result.jobEnvironment.get)
  }

  def getAvroSchema()(implicit pipeline: SparkPipeline = null): Map[String, Seq[Map[String, String]]] = {
    val result = transform()(pipeline)
    val output = result.content.map(x => {
      val collection = x._2.map(y => {
        val df = new StructAssembler()
          .setOutputCol(myConstant.STRUCT_COLUMN_FOR_AVRO)
          .transform(y)
        val content = AvroSchemaUtils.toAvroSchema(df, myConstant.STRUCT_COLUMN_FOR_AVRO)
        Map("content" -> content.toString)
      })
      (x._1, collection)
    })
    output
  }
}

/** Feature Engine object
  */
object SparkEngine {
  val DEFAULT_NAME = "Spark Engine"

  case class Entry(config: Seq[String] = Seq(),
                   commonConfig: Option[String] = None,
                   show: Int = 10,
                   filter: Map[String, String] = Map(),
                   name: Option[String] = None,
                   range: Seq[String] = Seq[String](),
                   inlineParam: Map[String, String] = Map(),
                   datePattern: String = "yyyyMMdd",
                   inputSourceId: Option[String] = None,
                   outputSourceId: Option[String] = None
  )

  /** get default spark app name
    * @param config
    *   Entry object
    * @return
    *   spark app name
    */
  def getDefaultAppName(config: Entry): String = {

    val filter = config.filter.map(x => {
      x._1 + "=" + x._2
    })

    s"$DEFAULT_NAME - ${config.config.mkString(",")} " +
      s"show:${config.show} " +
      s"filters:${filter.mkString(",")} " +
      s"date:${config.range.mkString(",")}"
  }

  /** create a feature engine object from arguments
    * @param args
    *   arguments
    * @return
    *   feature engine object
    */
  def createEngine(args: Array[String]): Option[SparkEngine] = {
    val parser = new scopt.OptionParser[Entry]("") {
      override def showUsageOnError = Some(true)

      opt[String]("name")
        .action((x, c) => c.copy(name = Some(x.stripPrefix("'").stripSuffix("'"))))
        .text(
          "spark application name, defaults to" +
            s" '$DEFAULT_NAME - <config> show:<show> filter:<filter> date:<date>'"
        )

      opt[Seq[String]]("config")
        .required()
        .action((x, c) => c.copy(config = x))
        .text("config files to run")

      opt[String]("common-config")
        .action((x, c) => c.copy(commonConfig = Some(x)))
        .text("common config file, relative to the classloader (src/main/resources)")

      opt[Int]("show")
        .action((x, c) => c.copy(show = x))
        .text("if > 0, only show the first n rows and don't save the DataFrame")

      opt[Map[String, String]]("filter")
        .action((x, c) => c.copy(filter = x))
        .text(
          "filter rows matching column name and value. " +
            "usage: column_name_1=value_1,column_name_2=value_2"
        )

      opt[Seq[String]]("date")
        .optional()
        .action((x, c) => c.copy(range = x))
        .text(
          "range of date to run. only consider rows with matching time, " +
            "this is done via regex matching against " +
            "dateColumn column specified in yaml config and its datePattern specified via --date-pattern. " +
            "If you choose run range of date then the format is specified with delimiter " +
            "'space' and enclosed by single quote (`'`). " +
            "In contrast, if you want run specific multiple date then the format is the date with " +
            "delimiter comma (','). " +
            "The date format specified in this argument should have same format. " +
            "Example usage: 'startDate endDate',date_1,date_2 " +
            "(eg. --date '20180715 20180720',20180721) " +
            "it will run for date=20180715,...,20180720,20180721"
        )

      opt[String]("date-pattern")
        .optional()
        .action((x, c) => c.copy(datePattern = x))
        .text("date format pattern for parse date in argument '--date'")

      opt[Map[String, String]]("inline-param")
        .action((x, c) => c.copy(inlineParam = x))
        .text(
          "parameterized config file. " +
            "Usage: parameter_1=value_1,parameter_2=value_2"
        )

      opt[String]("input-sourceid")
        .action((x, c) => c.copy(inputSourceId = Some(x)))
        .text("input source id to be used if id is not found on the reference. Default is 'base_input'")

      opt[String]("output-sourceid")
        .action((x, c) => c.copy(outputSourceId = Some(x)))
        .text("output source id to be used if id is not found on the reference. Default is 'base_output'")

      help("help")
        .text("prints this usage text")
    }

    parser.parse(args, Entry()) match {
      case Some(config) =>
        val appName = config.name.getOrElse(getDefaultAppName(config))
        val jobSpec = config.config.map(x => JobSpec(file = Some(x), string = None))
        val commonObject = CommonObject(file = config.commonConfig)

        Some(
          SparkEngine(jobSpec,
                        Some(config.filter),
                        config.show,
                        commonObject,
                        config.range,
                        config.datePattern,
                        Some(config.inlineParam),
                        appName,
                        config.inputSourceId,
                        config.outputSourceId
          )
        )

      case None => None
    }
  }

  /** main runner
    * @param args
    *   arguments
    */
  def main(args: Array[String]) {
    createEngine(args) match {
      case Some(engine) =>
        implicit val pipeline = engine.initSparkPipeline()
        val results = engine.transform()
        if (engine.maxResults > 0) {
          results.content.foreach(x => {
            if (x._1 != "") {
              println(s"Date: ${x._1}") // scalastyle:off
            }
            x._2 foreach { y => y.show(engine.maxResults, false) }
          })
        } else {
          engine.materialize(results.content)
        }

      case None =>
    }
  }

  /** create feature engine object from typesafe config
    * @param config
    *   typesafe config object
    * @return
    *   feature engine object
    */
  def initFromConfig(config: Config, root: String = "input"): SparkEngine = {
    val inputConfig = config.getConfig(root)
    val configFiles = if (inputConfig.hasPath("configFiles")) {
      inputConfig.getStringList("configFiles").asScala
    } else {
      Seq()
    }
    val filter: Option[Map[String, String]] =
      if (inputConfig.hasPath("filter")) {
        Some(
          inputConfig
            .getConfig("filter")
            .entrySet()
            .asScala
            .map(e => e.getKey -> e.getValue.render())
            .toMap
        )
      } else {
        None
      }
    val commonConfigFile: Option[String] =
      if (inputConfig.hasPath("commonConfigFile")) {
        Some(inputConfig.getString("commonConfigFile"))
      } else {
        None
      }
    val datePattern: String = if (inputConfig.hasPath("datePattern")) {
      inputConfig.getString("datePattern")
    } else {
      "yyyyMMdd"
    }
    val range: Seq[String] = if (inputConfig.hasPath("date")) {
      inputConfig.getStringList("date").asScala
    } else {
      Seq[String]()
    }
    val baseInput: Option[String] = if (inputConfig.hasPath("baseInput")) {
      Some(inputConfig.getString("baseInput"))
    } else {
      Some("base_input")
    }
    val baseOutput: Option[String] = if (inputConfig.hasPath("baseOutput")) {
      Some(inputConfig.getString("baseOutput"))
    } else {
      Some("base_output")
    }
    val inlineParam: Option[Map[String, String]] =
      if (inputConfig.hasPath("inlineParam")) {
        Some(
          inputConfig
            .getConfig("inlineParam")
            .entrySet()
            .asScala
            .map(e => {
              e.getKey -> e.getValue.render().replace("\"", "")
            })
            .toMap
        )
      } else {
        None
      }
    val saveOrShow: Int = if (inputConfig.hasPath("show")) {
      inputConfig.getInt("show")
    } else {
      10
    }
    val configString: Seq[String] = if (inputConfig.hasPath("configString")) {
      inputConfig
        .getList("configString")
        .asScala
        .map(x => x.render(ConfigRenderOptions.concise()))
    } else {
      Seq()
    }
    val commonConfigString: Option[String] =
      if (inputConfig.hasPath("commonConfigString")) {
        Some(
          inputConfig
            .getConfig("commonConfigString")
            .root()
            .render(ConfigRenderOptions.concise())
        )
      } else {
        None
      }

    val jobSpec: Seq[JobSpec] = if (configFiles.nonEmpty) {
      configFiles.map(x => JobSpec(file = Some(x), string = None))
    } else if (configString.nonEmpty) {
      configString.map(x => JobSpec(file = None, string = Some(x)))
    } else {
      Seq(JobSpec(None, None))
    }

    val commonObject = CommonObject(file = commonConfigFile, string = commonConfigString)

    SparkEngine(jobSpec = jobSpec,
                  filter = filter,
                  commonObject = commonObject,
                  maxResults = saveOrShow,
                  range = range,
                  datePattern = datePattern,
                  inlineParam = inlineParam,
                  inputSourceId = baseInput,
                  outputSourceId = baseOutput
    )
  }

}
