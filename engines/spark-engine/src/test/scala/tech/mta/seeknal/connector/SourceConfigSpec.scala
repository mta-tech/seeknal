package tech.mta.seeknal.connector

import io.circe.Json
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import tech.mta.seeknal.BaseSparkSpec

@RunWith(classOf[JUnitRunner])
class SourceConfigSpec extends BaseSparkSpec {

  "SourceConfig" should {
    "get table name from path" in {
      val config: SourceConfig = SourceConfig(id = Some("test"), source = Some("file"), path = Some("build/test/path"))

      val tableName: String = config.getTableName
      assert(tableName === "test")
    }

    "get explicit path" in {
      val params = Json
        .obj(("path", Json.fromString("param/path")), ("format", Json.fromString("parquet")))
        .asObject

      val config: SourceConfig =
        SourceConfig(id = Some("test"), source = Some("file"), params = params, path = Some("explicit/path"))

      val pathName = config.getPath
      assert(pathName === "explicit/path")
    }

    "get path from param" in {
      val params = Json
        .obj(("path", Json.fromString("param/path")), ("format", Json.fromString("parquet")))
        .asObject
      val config: SourceConfig = SourceConfig(id = Some("test"), source = Some("file"), params = params)

      val pathName = config.getPath
      assert(pathName === "param/path")
    }
  }
}
