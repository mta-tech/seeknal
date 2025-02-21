package tech.mta.seeknal.connector.loader

import io.circe.Json

import java.io.File
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{asc, col}
import org.apache.spark.sql.types.{BooleanType, StringType, StructField}
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, Ignore}
import org.scalatestplus.junit.JUnitRunner
import tech.mta.seeknal.{BaseSparkSpec, DataFrameBuilder}
import tech.mta.seeknal.connector.{IOMaterial, SourceConfig}
import tech.mta.seeknal.pipeline.Bucketing

@Ignore
@RunWith(classOf[JUnitRunner])
class HiveLoaderSpec extends BaseSparkSpec with BeforeAndAfterAll {

  val outputDirOne = "build/integ-test/hive-loader-result-one"
  val outputDirTwo = "build/integ-test/hive-loader-result-two"
  val outputDirThree = "build/integ-test/hive-loader-result-three"
  val outputDirFour = "build/integ-test/hive-loader-result-four"
  val dbName = "dummy"

  override def beforeAll() {

    super.beforeAll()
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName").collect()
  }

  override def afterAll() {
    spark.sql(s"DROP DATABASE IF EXISTS $dbName CASCADE").collect()

    FileUtils.deleteDirectory(new File(outputDirOne))
    FileUtils.deleteDirectory(new File(outputDirTwo))
    FileUtils.deleteDirectory(new File(outputDirThree))
    FileUtils.deleteDirectory(new File(outputDirFour))
    super.afterAll()
    new File("metastore_db").delete()
  }

  // TODO: write more tests
  "HiveLoader" should {

    val inputBuilder = DataFrameBuilder(Seq(Row("1", "a"), Row("2", "b")),
                                        Seq(StructField("id", StringType), StructField("column", StringType)))
    val incorrectInputBuilder = DataFrameBuilder(Seq(Row("1", "a"), Row("2", "b")),
                                           Seq(StructField("column", StringType), StructField("column", StringType)))

    "catch error when save the incorrect df to hive table" in {
      val output = SourceConfig(source = Some("hive"), path = Some(outputDirOne), table = Some(s"$dbName.dummy_table"))
      val loader = IOMaterial(spark, output, Some(incorrectInputBuilder.build()))
      val throw_error = intercept[RuntimeException] {
        new HiveLoader(loader).save()
      }
      assert(throw_error.getMessage.contains("Exception when writing partition"))
    }

    "save the df to hive table without partition" in {
      val output = SourceConfig(source = Some("hive"), path = Some(outputDirOne), table = Some(s"$dbName.dummy_table"))
      val loader = IOMaterial(spark, output, Some(inputBuilder.build()))
      new HiveLoader(loader).save()

      val result = spark
        .table(s"$dbName.dummy_table")
        .orderBy(asc("id"))
      assertDataFrameEquals(result, inputBuilder.build())
    }

    "save the df to hive table without partition with buckets" in {
      val bucket: Bucketing = new Bucketing(buckets = 2, bucketBy = Seq("column"), sortBy = Seq("column"))
      val output = SourceConfig(source = Some("hive"),
                                path = Some(outputDirOne),
                                table = Some(s"$dbName.dummy_table"),
                                bucketing = Some(bucket)
      )
      val loader = IOMaterial(spark, output, Some(inputBuilder.build()))
      new HiveLoader(loader).save()

      val result = spark.table(s"$dbName.dummy_table").orderBy(asc("id"))

      val numBuckets = spark
        .sql(s"describe extended $dbName.dummy_table")
        .filter(col("col_name") === "Num Buckets")
        .select("data_type")
        .first()
        .get(0)
        .asInstanceOf[String]

      assertDataFrameEquals(result, inputBuilder.build())
      assert(numBuckets == "2")
    }

    "save the df to Managed hive table " in {
      val output = SourceConfig(source = Some("hive"), table = Some(s"$dbName.managed_dummy_table"))
      val loader = IOMaterial(spark, output, Some(inputBuilder.build()))
      new HiveLoader(loader).save()

      val result = spark
        .table(s"$dbName.managed_dummy_table")
        .orderBy(asc("id"))

      val tableType: String = spark.catalog
        .listTables(s"$dbName")
        .where(col("name") === "managed_dummy_table")
        .select(col("tableType"))
        .first()
        .get(0)
        .asInstanceOf[String]

      assert(tableType == "MANAGED")
      assertDataFrameEquals(result, inputBuilder.build())
    }

    "save the df to hive table with partition" in {
      val output = SourceConfig(source = Some("hive"),
                                path = Some(outputDirTwo),
                                table = Some(s"$dbName.dummy_table_two"),
                                partitions = Some(Seq("column"))
      )
      val loader = IOMaterial(spark, output, Some(inputBuilder.build()))
      new HiveLoader(loader).save()

      val result = spark
        .table(s"$dbName.dummy_table_two")
        .orderBy(asc("id"))
      assertDataFrameEquals(result, inputBuilder.build())
    }

    "save the df to external hive table with append mode" in {
      val params = Json
        .obj(("mode", Json.fromString("append")))
        .asObject
      val output = SourceConfig(source = Some("hive"),
                                path = Some(outputDirFour),
                                table = Some(s"$dbName.dummy_table_append"),
                                params = params
      )
      val loader = IOMaterial(spark, output, Some(inputBuilder.build()))
      //      first time save
      new HiveLoader(loader).save()
      //      second time append
      new HiveLoader(loader).save()

      val result = spark
        .table(s"$dbName.dummy_table_append")
        .orderBy(asc("id"))

      assert(result.count() == 2 * inputBuilder.build().count())
    }

    "save the df to Managed hive table with append " in {
      val params = Json
        .obj(("mode", Json.fromString("append")))
        .asObject

      val output =
        SourceConfig(source = Some("hive"), table = Some(s"$dbName.managed_dummy_table_append"), params = params)
      val loader = IOMaterial(spark, output, Some(inputBuilder.build()))
      //      first time save
      new HiveLoader(loader).save()
      //      second time append
      new HiveLoader(loader).save()

      val result = spark
        .table(s"$dbName.managed_dummy_table_append")
        .orderBy(asc("id"))

      assert(result.count() == 2 * inputBuilder.build().count())
    }

  }

  "save the df to hive table with partition and bucketing" in {

    val inputBuilder = DataFrameBuilder(
      Seq(Row("1", "a", "d"), Row("2", "b", "e")),
      Seq(StructField("id", StringType), StructField("column", StringType), StructField("bucket_column", StringType))
    )

    val bucket: Bucketing = new Bucketing(buckets = 2, bucketBy = Seq("bucket_column"), sortBy = Seq("bucket_column"))
    val output = SourceConfig(source = Some("hive"),
                              path = Some(outputDirTwo),
                              table = Some(s"$dbName.dummy_table_two"),
                              partitions = Some(Seq("column")),
                              bucketing = Some(bucket)
    )
    val loader = IOMaterial(spark, output, Some(inputBuilder.build()))
    new HiveLoader(loader).save()

    val result = spark
      .table(s"$dbName.dummy_table_two")
      .orderBy(asc("id"))

    val numBuckets = spark
      .sql(s"describe extended $dbName.dummy_table_two")
      .filter(col("col_name") === "Num Buckets")
      .select("data_type")
      .first()
      .get(0)
      .asInstanceOf[String]

    assertDataFrameEquals(result.select("id"), inputBuilder.build().select("id"))
    assert(numBuckets == "2")
  }

  "write" should {
    val schema = Seq(StructField("col1", StringType), StructField("col2", StringType), StructField("col3", BooleanType))
    val inputDf =
      DataFrameBuilder(Seq(Row("a", "c", true), Row("b", "d", false)), schema)
    val tableName = s"$dbName.dummy_three"

    "write dataframe with multiple partitions for SparkVersion 2.3.0" in {
      val config = SourceConfig(Some(tableName),
                                Some("hive"),
                                None,
                                None,
                                None,
                                Some(outputDirThree),
                                Some(Seq("col1", "col2")),
                                None
      )
      val loader = IOMaterial(spark, config, Some(inputDf.build()))
      val hiveLoader = new HiveLoaderTest(loader)
      hiveLoader.writeTest()

      val output = spark
        .table(tableName)
        .select("col1", "col2", "col3")
      assertDataFrameEquals(inputDf.build(), output)

      val hiveTable =
        new HiveTable(spark, tableName, new Path(outputDirThree), Some(Seq("col1", "col2")))
      val outputPartition = hiveTable.getPartitionColumn

      // check whether output table has partitions col1,col2
      assert("col1,col2" === outputPartition)
    }

    "write dataframe with multiple partitions for SparkVersion lower than 2.3.0" in {
      val config = SourceConfig(Some(tableName),
                                Some("hive"),
                                None,
                                None,
                                None,
                                Some(outputDirThree),
                                Some(Seq("col1", "col2")),
                                None
      )
      // save dataframe, it will only use col1 as partition due to sparkVersion
      val loader = IOMaterial(spark, config, Some(inputDf.build()))
      val hiveLoader = new HiveLoaderTest(loader)
      hiveLoader.writeTest("2.2.0")

      val output = spark
        .table(tableName)
        .select("col1", "col2", "col3")
      assertDataFrameEquals(inputDf.build(), output)

      val hiveTable =
        new HiveTable(spark, tableName, new Path(outputDirThree), Some(Seq("col1", "col2")))
      val outputPartition = hiveTable.getPartitionColumn

      // check whether output table only has partition col1
      assert("col1" === outputPartition)
    }

    "write dataframe with multiple partitions for SparkVersion in cloudera-format" in {
      val config = SourceConfig(Some(tableName),
                                Some("hive"),
                                None,
                                None,
                                None,
                                Some(outputDirThree),
                                Some(Seq("col1", "col2")),
                                None
      )

      val loader = IOMaterial(spark, config, Some(inputDf.build()))
      val hiveLoader = new HiveLoaderTest(loader)
      hiveLoader.writeTest("2.3.0-cloudera4")

      val output = spark
        .table(tableName)
        .select("col1", "col2", "col3")
      assertDataFrameEquals(inputDf.build(), output)

      val hiveTable =
        new HiveTable(spark, tableName, new Path(outputDirThree), Some(Seq("col1", "col2")))
      val outputPartition = hiveTable.getPartitionColumn

      // check whether output table only has partition col1
      assert("col1,col2" === outputPartition)
    }

  }
}

class HiveLoaderTest(loader: IOMaterial) extends HiveLoader(loader: IOMaterial) {

  def writeTest(version: String = "2.3.0"): Unit = {
    write(loader.result.get, version)
  }
}
