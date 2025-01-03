package tech.mta.seeknal.utils

import org.apache.hadoop.fs.{FileSystem, Path}
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatestplus.junit.JUnitRunner
import tech.mta.seeknal.BaseSparkSpec

@RunWith(classOf[JUnitRunner])
class FileUtilsSpec extends BaseSparkSpec with BeforeAndAfterAll {

  val dbName = "utilTest"
  val outputDir = new Path("build/utilSpec")
  val tableName = s"$dbName.input"

  override def beforeAll(): Unit = {
    super.beforeAll()

  }

  override def afterAll(): Unit = {

    super.afterAll()
  }

  "File Utils test" should {

    "read file" in {
      val output = FileUtils.readFile("src/test/resources/databases/sample.txt")
      assert(output.length > 0)
    }

    "read file with spark" in {
      val output = FileUtils.readFile("src/test/resources/databases/sample.txt", spark)
      assert(output.length > 0)
    }

    "getFs test" in {
      val output = FileUtils.getFs(outputDir, spark)
//       scalastyle:off
      // sessionState is not available on spark 2.1, so we need to do it this way
      val hadoopConf = spark.sparkContext.hadoopConfiguration
//       scalastyle:on
      assert(outputDir.getFileSystem(hadoopConf) == output)
    }

    "makeQualified test" in {
      val output = FileUtils.makeQualified(outputDir, spark)
      assert(output.isUriPathAbsolute)
    }

    "makeQualified with file system argument test" in {
      val output = FileUtils.makeQualified(outputDir, FileUtils.getFs(outputDir, spark))
      assert(output.isUriPathAbsolute)
    }

  }
}
