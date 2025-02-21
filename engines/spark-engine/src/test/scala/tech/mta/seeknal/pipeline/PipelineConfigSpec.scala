package tech.mta.seeknal.pipeline

import io.circe.{Json, ParsingFailure}
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.matchers.should.Matchers._
import org.mockito.Mockito.when
import org.scalatestplus.junit.JUnitRunner
import org.scalatestplus.mockito.MockitoSugar
import tech.mta.seeknal.BaseSparkSpec
import tech.mta.seeknal.connector.SourceConfig

@RunWith(classOf[JUnitRunner])
class PipelineConfigSpec extends BaseSparkSpec with MockitoSugar {

  "PipelineConfig" should {
    val inputSource = SourceConfig(id = Some("input"),
                                   source = Some("hive"),
                                   db = Some(DbConfig("db", Some("/some/db/in_path"))),
                                   table = Some("table"),
                                   path = Some("/some/db/table/in_path")
    )
    val defaultInputDb = DbConfig("bridge", Some("/hive/bridge"))
    val inputSourceBase = SourceConfig(id = Some("base_input"), source = Some("hive"), db = Some(defaultInputDb))
    val inputSourceBaseTwo =
      SourceConfig(id = Some("base_input_two"), source = Some("hive"), db = Some(DbConfig("blah", Some("/hive/blah"))))

    val outputSource = SourceConfig(id = Some("output"),
                                    source = Some("hive"),
                                    db = Some(DbConfig("db", Some("/some/db/out_path"))),
                                    table = Some("table"),
                                    path = Some("/some/db/table/out_path")
    )
    val defaultOutputDb = DbConfig("feateng", Some("/hive/feateng"))
    val outputSourceBase = SourceConfig(id = Some("base_output"), source = Some("hive"), db = Some(defaultOutputDb))

    val outputSourceBaseTwo =
      SourceConfig(id = Some("base_output_two"), source = Some("hive"), db = Some(DbConfig("geo", Some("/hive/geo"))))

    val featurePipeline =
      FeaturePipeline(input = Some(SourceConfig(Some("input"))), output = Some(SourceConfig(Some("output"))))
    val config = PipelineConfig(
      sources = Some(
        Seq(inputSource, inputSourceBase, inputSourceBaseTwo, outputSource, outputSourceBase, outputSourceBaseTwo)
      ),
      pipeline = Some(featurePipeline)
    )

    "returns requested source config" in {
      config.getInputSource shouldBe inputSource
      config.getOutputSource shouldBe outputSource
    }

    "throw exception if input is not available when requested" in {
      an[IllegalStateException] should be thrownBy PipelineConfig().getInputSource
    }

    "throw exception if output is not available when requested" in {
      an[IllegalStateException] should be thrownBy PipelineConfig().getOutputSource
    }

    "returns sources with full table name when base sources contains db definition" in {
      val featurePipeline =
        FeaturePipeline(input = Some(SourceConfig(Some("blah"))), output = Some(SourceConfig(Some("bleh"))))
      val updateConfig = config.overridePipeline(featurePipeline)

      val inputSource = updateConfig.getInputSource
      inputSource shouldBe SourceConfig(Some("blah"),
                                        table = Some("blah"),
                                        source = Some("hive"),
                                        db = Some(defaultInputDb)
      )
      inputSource.getTableName shouldBe "bridge.blah"

      val outputSource = updateConfig.getOutputSource
      outputSource shouldBe SourceConfig(Some("bleh"),
                                         table = Some("bleh"),
                                         source = Some("hive"),
                                         db = Some(defaultOutputDb)
      )
      outputSource.getTableName shouldBe "feateng.bleh"
    }

    "returns sources with id as table name when id include database name" in {
      val featurePipeline =
        FeaturePipeline(input = Some(SourceConfig(Some("db1.blah"))), output = Some(SourceConfig(Some("db2.bleh"))))
      val updateConfig = config.overridePipeline(featurePipeline)

      updateConfig.getInputSource shouldBe SourceConfig(Some("db1.blah"), table = Some("db1.blah"))

      updateConfig.getOutputSource shouldBe SourceConfig(Some("db2.bleh"), table = Some("db2.bleh"))
    }

    "returns sources based on selected base key" in {
      val featurePipeline =
        FeaturePipeline(input = Some(SourceConfig(Some("blah"))), output = Some(SourceConfig(Some("bleh"))))
      val updateConfig = config.overridePipeline(featurePipeline)

      val inputSource = updateConfig
        .setBaseKeyInput("base_input_two")
        .getInputSource
      inputSource shouldBe SourceConfig(Some("blah"),
                                        table = Some("blah"),
                                        source = Some("hive"),
                                        db = Some(DbConfig("blah", Some("/hive/blah")))
      )
      inputSource.getTableName shouldBe "blah.blah"

      val outputSource = updateConfig
        .setBaseKeyOutput("base_output_two")
        .getOutputSource
      outputSource shouldBe SourceConfig(Some("bleh"),
                                         table = Some("bleh"),
                                         source = Some("hive"),
                                         db = Some(DbConfig("geo", Some("/hive/geo")))
      )
      outputSource.getTableName shouldBe "geo.bleh"
    }
  }

  "PipelineConfig companion" should {
    "create instance from yaml string" in {
      val yamlString = """---
                         |sources:
                         |  - id: input1
                         |    source: hive
                         |    db:
                         |      name: abcd
                         |      path: /some/path/db
                         |    table: table_name
                         |    path: /some/path/table
                         |  - id: output1
                         |    source: hive
                         |    db:
                         |      name: abcd
                         |      path: /some/path/db
                         |    table: table_name
                         |    path: /some/path/table
                         |  - id: output2
                         |    table: some.table
                         |transformations:
                         |  - id: trans1
                         |    className: class1
                         |    params:
                         |      inputCol: col1
                         |  - id: trans2
                         |    className: class2
                         |    params:
                         |      inputCol: col2
                         |pipeline:
                         |  input:
                         |    id: input1
                         |  output:
                         |    id: output1
                         |  stages:
                         |    - id: trans1
                         |rules:
                         |  - id: rule1
                         |    rule:
                         |      value: col1
                         |  - id: rule2
                         |    rule:
                         |      value: col2""".stripMargin

      val config = PipelineConfig.fromString(yamlString)

      config.sources.get should have length 3
      config.sources.get.head.id shouldBe Some("input1")
      config.sources.get.head.source shouldBe Some("hive")
      config.sources.get.head.db.get.name shouldBe "abcd"
      config.sources.get.head.db.get.path shouldBe Some("/some/path/db")
      config.sources.get.head.table shouldBe Some("table_name")
      config.sources.get.head.path shouldBe Some("/some/path/table")

      config.sources.get(1).id shouldBe Some("output1")
      config.sources.get(1).source shouldBe Some("hive")
      config.sources.get(1).db.get.name shouldBe "abcd"
      config.sources.get(1).db.get.path shouldBe Some("/some/path/db")
      config.sources.get(1).table shouldBe Some("table_name")
      config.sources.get(1).path shouldBe Some("/some/path/table")

      config.sources.get(2).id shouldBe Some("output2")
      config.sources.get(2).source shouldBe None
      config.sources.get(2).db shouldBe None
      config.sources.get(2).table shouldBe Some("some.table")
      config.sources.get(2).path shouldBe None

      config.pipeline.get.input shouldBe Some(SourceConfig(Some("input1"), source = None))
      config.pipeline.get.output shouldBe Some(SourceConfig(Some("output1"), source = None))
      config.pipeline.get.stages.get.length shouldBe 1
      config.pipeline.get.stages.get.head.id shouldBe Some("trans1")

      config.transformations.get should have length 2
      config.transformations.get.head.id shouldBe Some("trans1")
      config.transformations.get.head.className shouldBe Some("class1")
      config.transformations.get.head.params shouldBe Json.obj(("inputCol", Json.fromString("col1"))).asObject
      config.transformations.get(1).id shouldBe Some("trans2")
      config.transformations.get(1).className shouldBe Some("class2")
      config.transformations.get(1).params shouldBe Json.obj(("inputCol", Json.fromString("col2"))).asObject

      config.rules.get should have length 2
      config.rules.get.head.id shouldBe "rule1"
      config.rules.get.head.rule shouldBe Json.obj(("value", Json.fromString("col1"))).asObject.get
      config.rules.get(1).id shouldBe "rule2"
      config.rules.get(1).rule shouldBe Json.obj(("value", Json.fromString("col2"))).asObject.get
    }

    "replace any {date} to set date" in {
      val yamlString = """---
                         |sources:
                         |  - id: input1
                         |    source: hive
                         |    db:
                         |      name: abcd
                         |      path: /some/path/db
                         |    table: table_name
                         |    path: /some/path/table
                         |  - id: output1
                         |    source: hive
                         |    db:
                         |      name: abcd
                         |      path: /some/path/db
                         |    table: table_name
                         |    path: /some/path/table
                         |  - id: output2
                         |    table: some.table
                         |transformations:
                         |  - id: trans1
                         |    className: org.apache.spark.ml.feature.SQLTransformer
                         |    params:
                         |      statement: select * from __THIS__ where date = {date}
                         |pipeline:
                         |  input:
                         |    id: input1
                         |  output:
                         |    id: output1
                         |  stages:
                         |    - id: trans1
                         |rules:
                         |  - id: rule1
                         |    rule:
                         |      value: col1
                         |  - id: rule2
                         |    rule:
                         |      value: col2""".stripMargin

      val params: Map[String, Either[String, Map[Boolean, Seq[String]]]] = Map("date" -> Left("20190301"))
      val config = PipelineConfig.fromString(yamlString, Some(params))

      config.transformations.get.head.params shouldBe Json
        .obj(("statement", Json.fromString("select * from __THIS__ where date = 20190301")))
        .asObject

    }

    "returns empty config when given empty string or null" in {
      PipelineConfig.fromString("") shouldBe PipelineConfig()
      PipelineConfig.fromString(null) shouldBe PipelineConfig()
    }

    "throw exception when transformers do not have id" in {
      // missing dateColumn, transformers and grouping
      val yamlString = """---
                         |transformations:
                         |  - id: trans1
                         |    className: class1
                         |    params:
                         |      inputCol: col1
                         |  - className: class2
                         |    params:
                         |      inputCol: col2""".stripMargin

      a[ParsingFailure] should be thrownBy PipelineConfig.fromString(yamlString)
    }

    "ignore optional attributes" in {
      // everything are optional, but we still need at least one attribute
      // so that the config can be parsed
      val yamlString = """---
                         |transformations:
                         |  - id: trans1
                         |    className: class1
                         |    params:
                         |      inputCol: col1""".stripMargin

      val config = PipelineConfig.fromString(yamlString)
      config.transformations.get should have length 1
      config.transformations.get.head.id shouldBe Some("trans1")
      config.transformations.get.head.className shouldBe Some("class1")
      config.transformations.get.head.params shouldBe Json.obj(("inputCol", Json.fromString("col1"))).asObject

      config.sources shouldBe None
      config.pipeline shouldBe None
      config.rules shouldBe None
    }
  }

  "SourceConfig" should {
    val mockSession: Option[SparkSession] = Some(mock[SparkSession])
    when(mockSession.get.table("table_name")).thenReturn(null)

    val config = SourceConfig(Some("id"),
                              table = Some("table_name"),
                              db = Some(DbConfig("feateng", Some("/hive/feateng"))),
                              path = Some("/some/path")
    )

    "returns the given path" in {
      config.getPath shouldBe "/some/path"
    }

    "returns null if path doesn't exists and both table or db are empty" in {
      config.copy(path = None, table = None).getPath shouldBe null
      config.copy(path = None, db = None).getPath shouldBe null
      config.copy(path = None, db = Some(DbConfig("feateng", None))).getPath shouldBe null
    }

    "appends table name to db path" in {
      config.copy(path = None).getPath shouldBe "/hive/feateng/table_name"
      config.copy(path = None, table = Some("ab.cd")).getPath shouldBe "/hive/feateng/cd"
    }
  }

  "FeatureTransformer" should {
    "be updated with defined attributes from another FeatureTransformer" in {
      val current = FeatureTransformer(id = Some("old_id"),
                                       params = Json.obj(("inputCol", Json.fromString("col2"))).asObject,
                                       className = None
      )
      val update = FeatureTransformer(id = Some("id"), params = None, className = Some("blah"))
      val updated = current.update(update)

      updated.id shouldBe Some("id")
      updated.params shouldBe Json.obj(("inputCol", Json.fromString("col2"))).asObject
      updated.className shouldBe Some("blah")
    }
  }
}
