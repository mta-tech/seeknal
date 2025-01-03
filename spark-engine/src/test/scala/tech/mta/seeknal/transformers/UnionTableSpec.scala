package tech.mta.seeknal.transformers

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.asc
import org.apache.spark.sql.types.{StringType, StructField}
import org.junit.runner.RunWith
import org.scalatest.Outcome
import org.scalatestplus.junit.JUnitRunner
import tech.mta.seeknal.{BaseSparkSpec, DataFrameBuilder}

@RunWith(classOf[JUnitRunner])
class UnionTableSpec extends BaseSparkSpec {

  override def withFixture(test: NoArgTest): Outcome = {

    val tableToJoin =
      DataFrameBuilder(Seq(Row("3", "c", "2"), Row("4", "d", "2"), Row("3", "c", "3"), Row("4", "d", "3")),
                       Seq(StructField("id", StringType),
                           StructField("column", StringType),
                           StructField("partitionCol", StringType)
                       )
      )
        .build()

    tableToJoin.write.mode("overwrite").partitionBy("partitionCol").saveAsTable("test")

    try super.withFixture(test)
    finally {
      spark.sql("DROP TABLE IF EXISTS test")
    }
  }

  "UnionTable" should {
    val inputBuilder = DataFrameBuilder(
      Seq(Row("1", "a", "1"), Row("2", "b", "1")),
      Seq(StructField("id", StringType), StructField("column", StringType), StructField("partitionCol", StringType))
    )

    "provide new dataframe from two table by union" in {
      val expectedOutputDf = DataFrameBuilder(
        Seq(Row("1", "a", "1"),
            Row("2", "b", "1"),
            Row("3", "c", "2"),
            Row("4", "d", "2"),
            Row("3", "c", "3"),
            Row("4", "d", "3")
        ),
        Seq(StructField("id", StringType), StructField("column", StringType), StructField("partitionCol", StringType))
      )
        .build()

      val transformer = new UnionTable()
        .setTableName("test")

      val outputDf = transformer
        .transform(inputBuilder.build())
        .orderBy(asc("partitionCol"), asc("id"), asc("column"))

      assertDataFrameEquals(outputDf, expectedOutputDf)
    }

    "provide new dataframe from two table by union with filter" in {
      val expectedOutputDf =
        DataFrameBuilder(Seq(Row("1", "a", "1"), Row("2", "b", "1"), Row("4", "d", "2"), Row("4", "d", "3")),
                         Seq(StructField("id", StringType),
                             StructField("column", StringType),
                             StructField("partitionCol", StringType)
                         )
        )
          .build()

      val transformer = new UnionTable()
        .setTableName("test")
        .setFilterExpression("id != 3")

      val outputDf = transformer
        .transform(inputBuilder.build())
        .orderBy(asc("partitionCol"), asc("id"), asc("column"))

      assertDataFrameEquals(outputDf, expectedOutputDf)
    }

    "provide new result from union with the latest data" in {
      val expectedOutputDf =
        DataFrameBuilder(Seq(Row("1", "a", "1"), Row("2", "b", "1"), Row("3", "c", "3"), Row("4", "d", "3")),
                         Seq(StructField("id", StringType),
                             StructField("column", StringType),
                             StructField("partitionCol", StringType)
                         )
        )
          .build()

      val transformer = new UnionTable()
        .setTableName("test")
        .setUseLatestData(true)
        .setPartitionCols(Array("partitionCol"))

      val outputDf = transformer
        .transform(inputBuilder.build())
        .orderBy(asc("partitionCol"), asc("id"), asc("column"))

      assertDataFrameEquals(outputDf, expectedOutputDf)
    }

    "provide new result from union with the latest data without specifying partitionCol" in {
      val expectedOutputDf =
        DataFrameBuilder(Seq(Row("1", "a", "1"), Row("2", "b", "1"), Row("3", "c", "3"), Row("4", "d", "3")),
                         Seq(StructField("id", StringType),
                             StructField("column", StringType),
                             StructField("partitionCol", StringType)
                         )
        )
          .build()

      val transformer = new UnionTable()
        .setTableName("test")
        .setUseLatestData(true)

      val outputDf = transformer
        .transform(inputBuilder.build())
        .orderBy(asc("partitionCol"), asc("id"), asc("column"))

      assertDataFrameEquals(outputDf, expectedOutputDf)
    }
  }
}
