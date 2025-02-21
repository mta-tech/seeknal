package tech.mta.seeknal.connector.loader

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{BooleanType, StringType, StructField}
import org.junit.runner.RunWith
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.Mockito.doAnswer
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfterAll
import org.scalatestplus.junit.JUnitRunner
import org.scalatest.matchers.should.Matchers._
import org.scalatestplus.mockito.MockitoSugar
import tech.mta.seeknal.{BaseSparkSpec, DataFrameBuilder}
import tech.mta.seeknal.utils.FileUtils

@RunWith(classOf[JUnitRunner])
class ParquetWriterSpec
    extends BaseSparkSpec
    with MockitoSugar
    with BeforeAndAfterAll {

  val rootPathV230 = new Path("build/parquet-writer-tmp-230")
  val rootPathV220 = new Path("build/parquet-writer-tmp-220")
  implicit val ordering: Ordering[Path] = Ordering.by(_.toString)

  override def beforeAll() {
    super.beforeAll()
    getFs.mkdirs(rootPathV230)
    getFs.mkdirs(rootPathV220)
  }

  override def afterAll() {
    val fs = getFs
    if (fs.exists(rootPathV230)) {
      fs.delete(rootPathV230, true)
    }
    if (fs.exists(rootPathV220)) {
      fs.delete(rootPathV220, true)
    }
    super.afterAll()
  }

  def getFs: FileSystem = {
    rootPathV230.getFileSystem(
      baseSparkSession.sparkContext.hadoopConfiguration)
    rootPathV220.getFileSystem(
      baseSparkSession.sparkContext.hadoopConfiguration)
  }

  def createPathArray(fs: FileSystem,
                      parent: Path,
                      children: Seq[String]): Array[Path] = {
    children
      .map(child => FileUtils.makeQualified(new Path(parent, child), fs))
      .toArray
  }

  "ParquetWriter using SparkVersion = 2.3.0" should {
    val schema = Seq(StructField("col1", StringType),
                     StructField("col2", StringType),
                     StructField("col3", BooleanType))
    val inputDf =
      DataFrameBuilder(Seq(Row("a", "c", true), Row("b", "d", false)), schema)

    "write DataFrame as partitioned parquet" in {
      val fs = getFs
      val outputPath = new Path(rootPathV230, "first")
      val writer = new ParquetWriter(fs)
      val df = inputDf.build()

      writer.write(df, outputPath, "dummy_table", Some(Seq("col1", "col2")))

      // Seems like _SUCCESS file is not created anymore
      val expectedPartitions =
        createPathArray(fs, outputPath, Seq("col1=a", "col1=b"))
      val partitions = fs.listStatus(outputPath)
      partitions.map(f => f.getPath).sorted shouldBe expectedPartitions

      // check if second partition is written
      val outputPathSecond = new Path(rootPathV230, "first/col1=a")
      val expectedPartitionSecond =
        createPathArray(fs, outputPathSecond, Seq("col2=c"))
      val partitionSecond = fs.listStatus(outputPathSecond)
      partitionSecond
        .map(f => f.getPath)
        .sorted shouldBe expectedPartitionSecond

    }

    "only overwrite new partitions" in {
      val fs = getFs
      val outputPath = new Path(rootPathV230, "second")
      val writer = new ParquetWriter(fs)

      // existing data, have 2 partitions a and b
      val df = inputDf.build()
      writer.write(df, outputPath, "dummy_table", Some(Seq("col1", "col2")))

      val inputExtraDf = DataFrameBuilder(Seq(Row("a", "f", true),
                                              Row("b", "d", true),
                                              Row("c", "c", true),
                                              Row("d", "d", false)),
                                          schema)

      // add c and d
      val extraDf = inputExtraDf.build()
      writer.write(extraDf, outputPath, "dummy_table", Some(Seq("col1", "col2")))

      // there should be 4 partitions, partition a, b should still exists
      val expectedPartitions =
        createPathArray(fs,
                        outputPath,
                        Seq("col1=a", "col1=b", "col1=c", "col1=d"))
      val partitions = fs.listStatus(outputPath)
      partitions.map(f => f.getPath).sorted shouldBe expectedPartitions

      val bResults = baseSparkSession.read
        .parquet(outputPath.toString)
        .filter(col("col1") === "b")
        .collect()
      // b should reflect the new value
      bResults shouldBe Array(Row(true, "b", "d"))

      val sparkVersionNumber =
        ("""\d+(\.\d+\.\d+)+""".stripMargin.r findAllIn spark.version).toList.head
          .replaceAll("\\.", "")
          .toInt

      // check if second partition is written
      val outputPathSecond = new Path(rootPathV230, "second/col1=a")

      if (sparkVersionNumber >= 230) {
        // only version 2.3.0 above can correctly insert new partition
        val expectedPartitionSecond =
          createPathArray(fs, outputPathSecond, Seq("col2=c", "col2=f"))
        val partitionSecond = fs.listStatus(outputPathSecond)
        partitionSecond
          .map(f => f.getPath)
          .sorted shouldBe expectedPartitionSecond
      } else {
        // < version 2.3.0 will overwrite whole folder
        val expectedPartitionSecond =
          createPathArray(fs, outputPathSecond, Seq("col2=f"))
        val partitionSecond = fs.listStatus(outputPathSecond)
        partitionSecond
          .map(f => f.getPath)
          .sorted shouldBe expectedPartitionSecond
      }
    }

    "abort write if at least one partition fails" in {
      val fs = Mockito.spy(getFs)
      val outputPath = new Path(rootPathV230, "third")
      val writer = new ParquetWriter(fs)
      val df = inputDf.build()

      // anything without col1=b will not be marked as successfully written
      val answer = new Answer[Boolean] {
        override def answer(invocation: InvocationOnMock): Boolean = {
          val path = invocation.getArgument[Path](0)
          path.toString.contains("col1=b")
        }
      }
      doAnswer(answer).when(fs).exists(any[Path])

      // col1=b will succeed, col1=a will fail, the write will be aborted
      writer.write(df, outputPath, "dummy_table", Some(Seq("col1")))
      fs.exists(outputPath) shouldBe false
    }
  }

  "ParquetWriter using SparkVersion < 2.3.0" should {
    val schema = Seq(StructField("col1", StringType),
                     StructField("col2", StringType),
                     StructField("col3", BooleanType))
    val inputDf =
      DataFrameBuilder(Seq(Row("a", "c", true), Row("b", "d", false)), schema)

    "write DataFrame as partitioned parquet" in {
      val fs = getFs
      val outputPath = new Path(rootPathV220, "first")
      val writer = new ParquetWriter(fs)
      val df = inputDf.build()

      writer.write(df, outputPath, "dummy_table", Some(Seq("col1", "col2")), 220)

      val expectedPartitions =
        createPathArray(fs, outputPath, Seq("col1=a", "col1=b"))
      val partitions = fs.listStatus(outputPath)

      partitions.map(f => f.getPath).sorted shouldBe expectedPartitions

      // the temp folder should be gone
      fs.listStatus(rootPathV220)
        .forall(f => f.getPath.getName.startsWith("first-")) shouldBe false
    }

    "only overwrite new partitions" in {
      val fs = getFs
      val outputPath = new Path(rootPathV220, "second")
      val writer = new ParquetWriter(fs)

      // existing data, have 2 partitions a and b
      val df = inputDf.build()
      writer.write(df, outputPath, "dummy_table", Some(Seq("col1")), 220)

      // new data, have 2 partitions b and c
      val newDf =
        DataFrameBuilder(Seq(Row("b", "f", true), Row("c", "g", false)), schema)
          .build()
      writer.write(newDf, outputPath, "dummy_table", Some(Seq("col1")), 220)

      // there should be 3 partitions, partition a should still exists
      val expectedPartitions =
        createPathArray(fs, outputPath, Seq("col1=a", "col1=b", "col1=c"))
      val partitions = fs.listStatus(outputPath)
      partitions.map(f => f.getPath).sorted shouldBe expectedPartitions

      val bResults = baseSparkSession.read
        .parquet(outputPath.toString)
        .filter(col("col1") === "b")
        .collect()
      // b should reflect the new value
      bResults shouldBe Array(Row("f", true, "b"))

      // the temp folder should be gone
      fs.listStatus(rootPathV220)
        .forall(f => f.getPath.getName.startsWith("second-")) shouldBe false
    }

    "abort write if at least one partition fails" in {
      val fs = Mockito.spy(getFs)
      val outputPath = new Path(rootPathV220, "third")
      val writer = new ParquetWriter(fs)
      val df = inputDf.build()

      // anything without col1=b will not be marked as successfully written
      val answer = new Answer[Boolean] {
        override def answer(invocation: InvocationOnMock): Boolean = {
          val path = invocation.getArgument[Path](0)
          path.toString.contains("col1=b")
        }
      }
      doAnswer(answer).when(fs).exists(any[Path])

      // col1=b will succeed, col1=a will fail, the write will be aborted
      writer.write(df, outputPath, "dummy_table", Some(Seq("col1")), 220)
      fs.exists(outputPath) shouldBe false

      // the temp folder should be gone
      fs.listStatus(rootPathV220)
        .forall(f => f.getPath.getName.startsWith("third-")) shouldBe false
    }
  }
}
