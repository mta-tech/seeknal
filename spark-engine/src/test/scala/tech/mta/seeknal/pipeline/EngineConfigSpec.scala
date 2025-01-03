package tech.mta.seeknal.pipeline

import io.circe.{Json, ParsingFailure}
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import org.scalatest.matchers.should.Matchers._
import tech.mta.seeknal.BaseSparkSpec
import tech.mta.seeknal.connector.SourceConfig
import tech.mta.seeknal.features.FeatureGenerator
import tech.mta.seeknal.transformers.{AddDate, ColumnRenamed}

@RunWith(classOf[JUnitRunner])
class EngineConfigSpec extends BaseSparkSpec {

  "EngineConfig" should {

    val transformersConfig = Seq(
      FeatureTransformer(className = Some("ai.eureka.featureengine.transformers.AddDate"),
                         params = Json.obj(("inputCol", Json.fromString("month_test"))).asObject
      )
    )

    val featurePipeline = FeaturePipeline(
      input = Some(
        SourceConfig(Some("input"),
                     table = Some("table_input"),
                     params = Json
                       .obj(("idCol", Json.fromString("user_id")),
                            ("inputDatePattern", Json.fromString("yyyyMMdd HH:mm:SS"))
                       )
                       .asObject
        )
      ),
      output = Some(SourceConfig(Some("output"), table = Some("table_output"))),
      stages = Some(transformersConfig)
    )

    val featureCell = Feature(name = "interaction",
                              frequency = "day",
                              params = Json
                                .obj(("entityCol", Json.fromString("cell")), ("dateCol", Json.fromString("day")))
                                .asObject,
                              subFeatures = Some(Seq(SubFeature("user_entity", None)))
    )

    val featureDomain = Feature(name = "interaction",
                                frequency = "day",
                                params = Json
                                  .obj(("idCol", Json.fromString("msisdn")), ("entityCol", Json.fromString("domain")))
                                  .asObject,
                                subFeatures = Some(Seq(SubFeature("user_entity", None)))
    )

    val configCell = EngineConfig(featurePipeline, Some(featureCell))
    val configDomain = EngineConfig(featurePipeline, Some(featureDomain))

    "be represented in string with name and sub features" in {
      configCell.toString shouldBe "name=interaction, features=(user_entity)"
      configDomain.toString shouldBe "name=interaction, features=(user_entity)"
    }

    "return transformers need to compute the requested features" in {
      implicit val baseSparkSessionOption: Option[SparkSession] = Some(spark)
      val features = configCell.getFeatures(PipelineConfig(pipeline = Some(featurePipeline)))

      features.length shouldBe 4
      // 3 transformers involved in daily interaction features
      // rename id, user_id -> id
      val renameId = features.head.asInstanceOf[ColumnRenamed]
      renameId.getInputCol shouldBe "user_id"
      renameId.getOutputCol shouldBe "id"
      // rename entity, cell -> entity
      val renameEntity = features(1).asInstanceOf[ColumnRenamed]
      renameEntity.getInputCol shouldBe "cell"
      renameEntity.getOutputCol shouldBe "entity"
      // format date, day -> day
      val formatDate = features(2).asInstanceOf[AddDate]
      formatDate.getInputCol shouldBe "day"
      formatDate.getOutputCol shouldBe "day"
      // yyyyMMdd HH:mm:SS -> yyyy-MM-dd
      formatDate.getInputPattern shouldBe "yyyyMMdd HH:mm:SS"
      formatDate.getOutputPattern shouldBe "yyyy-MM-dd"
      formatDate.getRemoveInputCol shouldBe false
    }

    "return configured default rules" in {
      implicit val baseSparkSessionOption: Option[SparkSession] = Some(spark)
      val pipelineConfig = PipelineConfig(pipeline = Some(featurePipeline),
                                          rules = Some(
                                            Seq(
                                              // this will not override idCol, since idCol is taken from input source
                                              FeatureRule(
                                                id = "idCol",
                                                rule = Json.obj(("value", Json.fromString("msisdn-new"))).asObject.get
                                              ),
                                              // this will not override inputDatePattern formatDate
                                              FeatureRule(
                                                id = "inputDatePattern",
                                                rule = Json.obj(("value", Json.fromString("yyyy.MM.dd"))).asObject.get
                                              ),
                                              // this will be using since dateCol not be defined anywhere
                                              FeatureRule(id = "dateCol",
                                                          rule =
                                                            Json.obj(("value", Json.fromString("date_id"))).asObject.get
                                              ),
                                              // this will be ignored, since Interaction
                                              // dont has this variable
                                              FeatureRule(id = "blah",
                                                          rule =
                                                            Json.obj(("value", Json.fromString("abcd"))).asObject.get
                                              )
                                            )
                                          )
      )

      val features = configDomain.getFeatures(pipelineConfig)

      features.length shouldBe 4
      val renameId = features.head.asInstanceOf[ColumnRenamed]
      renameId.getInputCol shouldBe "msisdn"

      val renameEntity = features(1).asInstanceOf[ColumnRenamed]
      renameEntity.getInputCol shouldBe "domain"

      val formatDate = features(2).asInstanceOf[AddDate]
      formatDate.getInputPattern shouldBe "yyyyMMdd HH:mm:SS"
      formatDate.getInputCol shouldBe "date_id"
    }

    "throw exceptions when given invalid feature" in {
      // valid feature names are those specififed in FeatureGenerator's featureMap
      an[IllegalArgumentException] should be thrownBy
        configCell.copy(feature = Some(Feature(frequency = "day", name = "nonexistentfeature")))
    }

    "get valid feature object by specified className" in {
      assert(
        Feature(frequency = "day", name = "ai.eureka.featureengine.features.Communication")
          .isInstanceOf[Feature]
      )
    }
  }

  "EngineConfig companion" should {

    "create instance from yaml string" in {
      val yamlString =
        """---
          |pipeline:
          |  input:
          |    table: traffic_day
          |    params:
          |      dateCol: date_id
          |      idCol: msisdn
          |  output:
          |    id: dummy
          |feature:
          |  name: communication
          |  frequency: day
          |  params:
          |    inputDatePattern: abcd
          |  subFeatures:
          |    - name: count
          |      dimensions:
          |        - name: comm_type
          |          values:
          |            - call
          |        - name: direction
          |          values:
          |            - out""".stripMargin

      val config = EngineConfig.fromString(yamlString)

      config.feature.get.subFeatures.get.length shouldBe 1
      val count = config.feature.get.subFeatures.get.head
      count.name shouldBe "count"
      count.dimensions.get.length shouldBe 2
      count.dimensions.get.head.name shouldBe "comm_type"
      count.dimensions.get.head.values.get shouldBe Seq("call")
      count.dimensions.get(1).name shouldBe "direction"
      count.dimensions.get(1).values.get shouldBe Seq("out")
      val featureGenerator = config.getFeatures(PipelineConfig(pipeline = Some(config.pipeline)))
      featureGenerator(2).asInstanceOf[AddDate].getInputPattern shouldBe "abcd"
    }

    "create instance from yaml that without `feature" in {
      val yamlString =
        """
          |---
          |pipeline:
          |  input:
          |    table: traffic_day
          |    params:
          |      dateCol: date_id
          |      idCol: msisdn
          |  output:
          |    id: dummy
          |  stages:
          |     - className: ai.eureka.featureengine.transformers.AddColumnByExpr
          |       params:
          |         outputCol: abc
          |         expression: '1'
          """.stripMargin

      val config = EngineConfig.fromString(yamlString)

      config.pipeline.stages.get.head.className.get shouldBe
        "ai.eureka.featureengine.transformers.AddColumnByExpr"
    }

    "throw exception given yaml string with missing keys" in {
      // missing pipeline should throw error
      val yamlString = """---
                         |sourceTable: source
                         |destTable: dest
                         |outputDir: /some/dir""".stripMargin

      a[ParsingFailure] should be thrownBy EngineConfig.fromString(yamlString)
    }

    "replace any {date} to set date" in {
      val yamlString =
        """---
          |pipeline:
          |  input:
          |    id: traffic_day
          |
          |  output:
          |    id: feateng_random_day
          |  stages:
          |    - className: org.apache.spark.ml.feature.SQLTransformer
          |      params:
          |        statement: SELECT * FROM __THIS__ WHERE date = {date}
          |feature:
          |  name: random
          |  frequency: day
          |            """.stripMargin

      val params: Map[String, Either[String, Map[Boolean, Seq[String]]]] = Map("date" -> Left("20190301"))
      val config = PipelineConfig.fromString(yamlString, Some(params))

      config.pipeline.get.stages.get.head.params shouldBe Json
        .obj(("statement", Json.fromString("SELECT * FROM __THIS__ WHERE date = 20190301")))
        .asObject
    }
  }
}
