package tech.mta.seeknal.connector.loader

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito.{times, verify, when}
import org.scalatestplus.junit.JUnitRunner
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers._
import tech.mta.seeknal.{BaseSparkSpec, DataFrameBuilder}

@RunWith(classOf[JUnitRunner])
class HiveTableSpec extends BaseSparkSpec with MockitoSugar {

  "HiveTable update" should {
    // empty DataFrame, used to indicate successful result after executing a statement
    val emptyResult = mock[DataFrame]
    when(emptyResult.collect()).thenReturn(Array.empty[Row])

    case class Fixture(inputDf: DataFrame, partitionResult: DataFrame, mockSpark: SparkSession)

    def fixture(spark: SparkSession): Fixture = {
      val inputDf: DataFrame =
        DataFrameBuilder(
          Seq(Row("a", true, "b")),
          Seq(StructField("col1", StringType), StructField("col2", BooleanType), StructField("col3", StringType))
        )
          .build()

      // result to return for 'SHOW PARTITIONS', in this case partitions are col2,col3
      val partitionResult: DataFrame =
        DataFrameBuilder(Seq(Row("col2=abcd/col3=bcd")), Seq(StructField("partition", StringType)))
          .build()

      // all statements will return empty result
      val mockSpark: SparkSession = mock[SparkSession]
      when(mockSpark.sql(anyString())).thenReturn(emptyResult)

      Fixture(inputDf, partitionResult, mockSpark)
    }

    "create table when table doesn't exist" in {
      val f = fixture(spark)
      // hive table that always return false when checking for existing table
      val table = new HiveTableTest(f.mockSpark, "table_a", new Path("/some/dir"), Some(Seq("col2")))
      table.update(f.inputDf)

      val argument: ArgumentCaptor[String] =
        ArgumentCaptor.forClass(classOf[String])
      // executes two statements:
      // - CREATE TABLE
      // - REPAIR TABLE
      verify(f.mockSpark, times(2)).sql(argument.capture)
      argument.getAllValues.get(0) should startWith("CREATE EXTERNAL TABLE table_a")
      argument.getAllValues.get(1) shouldBe "MSCK REPAIR TABLE table_a"
    }

    "only repair table when table already exist with the same schema" in {
      val f = fixture(spark)
      when(f.mockSpark.sql("SHOW PARTITIONS table_a"))
        .thenReturn(f.partitionResult)
      // return the new DataFrame when querying existing table
      when(f.mockSpark.table(anyString())).thenReturn(f.inputDf)

      // hive table that always return true when checking for existing table
      val table = new HiveTableTest(f.mockSpark, "table_a", new Path("/some/dir"), Some(Seq("col2", "col3")), true)
      table.update(f.inputDf)

      val argument: ArgumentCaptor[String] =
        ArgumentCaptor.forClass(classOf[String])
      // executes three statements:
      // - SHOW PARTITIONS: to compare the schema
      // - REPAIR TABLE
      // - REFRESH TABLE
      verify(f.mockSpark, times(3)).sql(argument.capture)
      argument.getAllValues.get(0) shouldBe "SHOW PARTITIONS table_a"
      argument.getAllValues.get(1) shouldBe "REFRESH table table_a"
      argument.getAllValues.get(2) shouldBe "MSCK REPAIR TABLE table_a"
    }

    "only refresh table when table already exist with the same schema and" +
      "table is non-partition" in {
        val f = fixture(spark)
        // return the new DataFrame when querying existing table
        when(f.mockSpark.table(anyString())).thenReturn(f.inputDf)

        // hive table that always return true when checking for existing table
        val table = new HiveTableTest(f.mockSpark, "table_a", new Path("/some/dir"), None, true)
        table.update(f.inputDf)

        val argument: ArgumentCaptor[String] =
          ArgumentCaptor.forClass(classOf[String])
        // executes two statements:
        // - SHOW PARTITIONS: to compare the schema
        // - REFRESH TABLE
        verify(f.mockSpark, times(2)).sql(argument.capture)
        argument.getAllValues.get(0) shouldBe "SHOW PARTITIONS table_a"
        argument.getAllValues.get(1) shouldBe "REFRESH table table_a"
      }

    "drop and create table when table already exist and the schema is changed" in {
      val f = fixture(spark)
      // SHOW PARTITIONS query now returns empty, so the schema will always be different
      // return the new DataFrame (input) when querying existing table
      when(f.mockSpark.table(anyString())).thenReturn(f.inputDf)

      // hive table that always return true when checking for existing table
      val table = new HiveTableTest(f.mockSpark, "table_a", new Path("/some/dir"), Some(Seq("col2")), true)
      table.update(f.inputDf)

      val argument: ArgumentCaptor[String] =
        ArgumentCaptor.forClass(classOf[String])
      // executes four statements:
      // - SHOW PARTITIONS: to compare the schema
      // - DROP TABLE
      // - CREATE TABLE
      // - REPAIR TABLE
      verify(f.mockSpark, times(4)).sql(argument.capture)
      argument.getAllValues.get(0) shouldBe "SHOW PARTITIONS table_a"
      argument.getAllValues.get(1) shouldBe "DROP TABLE table_a"
      argument.getAllValues.get(2) should startWith("CREATE EXTERNAL TABLE table_a")
      argument.getAllValues.get(3) shouldBe "MSCK REPAIR TABLE table_a"
    }
  }

  "HiveTable companion" should {
    "generate CREATE TABLE statement for a given schema with partition" in {
      val schema = StructType(
        Seq(StructField("col1", StringType),
            StructField("col2", BooleanType),
            // nullable doesn't affect the SQL statement
            StructField("col3", LongType, nullable = false),
            StructField("col4", StringType)
        )
      )

      val createTableString =
        HiveTable.makeCreateTableString("table_a", schema, Some(Seq("col4")), new Path("/some/dir"))
      createTableString shouldBe """CREATE EXTERNAL TABLE table_a (col1 STRING,
                                   |col2 BOOLEAN,
                                   |col3 BIGINT)
                                   |PARTITIONED BY (col4 STRING)
                                   |STORED AS PARQUET
                                   |LOCATION "/some/dir"""".stripMargin.replaceAll("\n", " ")
    }

    "generate CREATE TABLE statement for a given schema with no partition" in {
      val schema = StructType(
        Seq(StructField("col1", StringType),
            StructField("col2", BooleanType),
            // nullable doesn't affect the SQL statement
            StructField("col3", LongType, nullable = false),
            StructField("col4", StringType)
        )
      )

      val createTableString =
        HiveTable.makeCreateTableString("table_a", schema, None, new Path("/some/dir"))
      createTableString shouldBe """CREATE EXTERNAL TABLE table_a (col1 STRING,
                                   |col2 BOOLEAN,
                                   |col3 BIGINT,
                                   |col4 STRING)
                                   |STORED AS PARQUET
                                   |LOCATION "/some/dir"""".stripMargin
        .replaceAll("\n", " ")
    }

    "throw exception when given invalid partition" in {
      val schema = StructType(Seq(StructField("col1", StringType), StructField("col2", StringType)))

      a[NoSuchElementException] should be thrownBy HiveTable
        .makeCreateTableString("table_a",
                               schema,
                               // col3 is not in the above schema
                               Some(Seq("col3")),
                               new Path("/some/dir")
        )
    }
  }
}

class HiveTableTest(spark: SparkSession,
                    name: String,
                    location: Path,
                    partitionColumn: Option[Seq[String]],
                    tableExistsResult: Boolean = false
) extends HiveTable(spark, name, location, partitionColumn) {

  override protected def checkTableExists(name: String): Boolean =
    tableExistsResult

  override protected def refresh(): Unit =
    spark.sql(s"REFRESH table $name").collect()
}
