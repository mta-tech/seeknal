package tech.mta.seeknal.transformers

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, count, lit, sum}
import org.apache.spark.sql.types.{BooleanType, LongType, StringType, StructField}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import tech.mta.seeknal.{BaseSparkSpec, DataFrameBuilder}
import tech.mta.seeknal.params.{ColumnExpression, RenameColumn}

@RunWith(classOf[JUnitRunner])
class GroupByColumnsSpec extends BaseSparkSpec {

  "GroupByColumns" should {
    val inputBuilder = DataFrameBuilder(Seq(Row("abc", "1"), Row("abc", "2"), Row("abc", "2"), Row("def", "1")),
                                        Seq(StructField("day", StringType), StructField("user", StringType))
    )

    "group and aggregate values" in {
      val grouping = new GroupByColumns(Seq(count(col("*"))))
        .setInputCols(Array("day", "user"))

      val expectedOutputDf = DataFrameBuilder(Seq(Row("abc", "1", 1L), Row("abc", "2", 2L), Row("def", "1", 1L)),
                                              Seq(StructField("day", StringType),
                                                  StructField("user", StringType),
                                                  // result from count is not nullable
                                                  StructField("count(1)", LongType, nullable = false)
                                              )
      )
        .build()

      val outputDf = grouping
        .transform(inputBuilder.build())
        .orderBy(col("day").asc, col("user").asc)
      assertDataFrameEquals(outputDf, expectedOutputDf)
    }

    "group and aggregate values with multiple aggregators and renamed column" in {
      val grouping = new GroupByColumns(Seq(count(col("*")), sum(lit(2))))
        .setInputCols(Array("day", "user"))
        .setRenamedCols(Array(RenameColumn("count(1)", "total"), RenameColumn("sum(2)", "sum")))

      val expectedOutputDf =
        DataFrameBuilder(Seq(Row("abc", "1", 1L, 2L), Row("abc", "2", 2L, 4L), Row("def", "1", 1L, 2L)),
                         Seq(StructField("day", StringType),
                             StructField("user", StringType),
                             // result from count is not nullable
                             StructField("total", LongType, nullable = false),
                             StructField("sum", LongType)
                         )
        )
          .build()

      val outputDf = grouping
        .transform(inputBuilder.build())
        .orderBy(col("day").asc, col("user").asc)
      assertDataFrameEquals(outputDf, expectedOutputDf)
    }

    "group and add new column with a expression" in {
      val grouping = new GroupByColumns(Seq(count(col("*")).alias("total")))
        .setInputCols(Array("day", "user"))
        .setColsByExpression(Array(ColumnExpression("more_than_one", "total > 1")))

      val expectedOutputDf =
        DataFrameBuilder(Seq(Row("abc", "1", 1L, false), Row("abc", "2", 2L, true), Row("def", "1", 1L, false)),
                         Seq(StructField("day", StringType),
                             StructField("user", StringType),
                             StructField("total", LongType, nullable = false),
                             StructField("more_than_one", BooleanType, nullable = false)
                         )
        )
          .build()

      val outputDf = grouping
        .transform(inputBuilder.build())
        .orderBy(col("day").asc, col("user").asc)
      assertDataFrameEquals(outputDf, expectedOutputDf)
    }

    "group and pivot with a function" in {
      val grouping = new GroupByColumns(Seq(count(col("day")).alias("hit")))
        .setInputCols(Array("user"))
        .setPivotKeyCol("day")
        .setPivotValueCols(Array("abc"))

      val expectedOutputDf = DataFrameBuilder(Seq(Row("1", 1L), Row("2", 2L)),
                                              Seq(StructField("user", StringType), StructField("abc", LongType))
      )
        .build()

      val outputDf = grouping
        .transform(inputBuilder.build())

      assertDataFrameEquals(expectedOutputDf, outputDf)
    }
  }
}
