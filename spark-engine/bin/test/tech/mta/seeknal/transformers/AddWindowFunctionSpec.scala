package tech.mta.seeknal.transformers

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import tech.mta.seeknal.{BaseSparkSpec, DataFrameBuilder}

@RunWith(classOf[JUnitRunner])
class AddWindowFunctionSpec extends BaseSparkSpec {

  "AddWindowFunction" should {

    val inputBuilder =
      DataFrameBuilder(Seq(Row("1", 10L, "a"), Row("1", 10L, "b"), Row("2", 10L, "a"), Row("2", 10L, "a")),
                       Seq(StructField("id", StringType),
                           StructField("column", LongType),
                           StructField("new_column", StringType)
                       )
      )

    "provide new avg window function column" in {
      val expectedOutputDf = DataFrameBuilder(
        Seq(Row("1", 10L, "b", 10.0), Row("1", 10L, "a", 10.0), Row("2", 10L, "a", 10.0), Row("2", 10L, "a", 10.0)),
        Seq(StructField("id", StringType),
            StructField("column", LongType),
            StructField("new_column", StringType),
            StructField("window_agg_column", DoubleType)
        )
      )
        .build()

      val transformer = new AddWindowFunction()
        .setInputCol("column")
        .setWindowFunction("avg")
        .setPartitionCols(Array("id", "new_column"))
        .setOrderCols(Array("id"))
        .setAscending(true)
        .setOutputCol("window_agg_column")

      val outputDf = transformer.transform(inputBuilder.build())
      assertDataFrameEquals(outputDf.orderBy("id", "new_column"), expectedOutputDf.orderBy("id", "new_column"))
    }

    "provide new sum window function column" in {
      val expectedOutputDf = DataFrameBuilder(
        Seq(Row("1", 10L, "b", 10L), Row("1", 10L, "a", 10L), Row("2", 10L, "a", 20L), Row("2", 10L, "a", 20L)),
        Seq(StructField("id", StringType),
            StructField("column", LongType),
            StructField("new_column", StringType),
            StructField("window_agg_column", LongType)
        )
      )
        .build()

      val transformer = new AddWindowFunction()
        .setInputCol("column")
        .setWindowFunction("sum")
        .setPartitionCols(Array("id", "new_column"))
        .setOrderCols(Array("id"))
        .setAscending(true)
        .setOutputCol("window_agg_column")

      val outputDf = transformer.transform(inputBuilder.build())
      assertDataFrameEquals(outputDf.orderBy("id", "new_column"), expectedOutputDf.orderBy("id", "new_column"))
    }

    "provide new count window function column" in {
      val expectedOutputDf = DataFrameBuilder(
        Seq(Row("1", 10L, "b", 1L), Row("1", 10L, "a", 1L), Row("2", 10L, "a", 2L), Row("2", 10L, "a", 2L)),
        Seq(StructField("id", StringType),
            StructField("column", LongType),
            StructField("new_column", StringType),
            StructField("window_agg_column", LongType, nullable = false)
        )
      )
        .build()

      val transformer = new AddWindowFunction()
        .setInputCol("column")
        .setWindowFunction("count")
        .setPartitionCols(Array("id", "new_column"))
        .setOrderCols(Array("id"))
        .setAscending(true)
        .setOutputCol("window_agg_column")

      val outputDf = transformer.transform(inputBuilder.build())
      assertDataFrameEquals(outputDf.orderBy("id", "new_column"), expectedOutputDf.orderBy("id", "new_column"))
    }

    "provide new max window function column" in {
      val inputBuilder =
        DataFrameBuilder(Seq(Row("1", 20L, "a"), Row("1", 10L, "b"), Row("2", 30L, "a"), Row("2", 10L, "a")),
                         Seq(StructField("id", StringType),
                             StructField("column", LongType),
                             StructField("new_column", StringType)
                         )
        )

      val expectedOutputDf = DataFrameBuilder(
        Seq(Row("1", 20L, "a", 20L), Row("1", 10L, "b", 20L), Row("2", 30L, "a", 30L), Row("2", 10L, "a", 30L)),
        Seq(StructField("id", StringType),
            StructField("column", LongType),
            StructField("new_column", StringType),
            StructField("window_agg_column", LongType)
        )
      )
        .build()

      val transformer = new AddWindowFunction()
        .setInputCol("column")
        .setWindowFunction("max")
        .setPartitionCols(Array("id"))
        .setOrderCols(Array("id"))
        .setAscending(true)
        .setOutputCol("window_agg_column")

      val outputDf = transformer.transform(inputBuilder.build())
      assertDataFrameEquals(outputDf, expectedOutputDf)
    }

    "provide new min window function column" in {
      val inputBuilder =
        DataFrameBuilder(Seq(Row("1", 20L, "a"), Row("1", 10L, "b"), Row("2", 30L, "a"), Row("2", 10L, "a")),
                         Seq(StructField("id", StringType),
                             StructField("column", LongType),
                             StructField("new_column", StringType)
                         )
        )

      val expectedOutputDf = DataFrameBuilder(
        Seq(Row("1", 20L, "a", 10L), Row("1", 10L, "b", 10L), Row("2", 30L, "a", 10L), Row("2", 10L, "a", 10L)),
        Seq(StructField("id", StringType),
            StructField("column", LongType),
            StructField("new_column", StringType),
            StructField("window_agg_column", LongType)
        )
      )
        .build()

      val transformer = new AddWindowFunction()
        .setInputCol("column")
        .setWindowFunction("min")
        .setPartitionCols(Array("id"))
        .setOrderCols(Array("id"))
        .setAscending(true)
        .setOutputCol("window_agg_column")

      val outputDf = transformer.transform(inputBuilder.build())
      assertDataFrameEquals(outputDf, expectedOutputDf)
    }

    "provide new rank window function column" in {
      val inputBuilder =
        DataFrameBuilder(Seq(Row("1", 20L, "a"), Row("1", 10L, "b"), Row("2", 30L, "a"), Row("2", 10L, "a")),
                         Seq(StructField("id", StringType),
                             StructField("column", LongType),
                             StructField("new_column", StringType)
                         )
        )

      val expectedOutputDf = DataFrameBuilder(
        Seq(Row("1", 20L, "a", 1), Row("1", 10L, "b", 2), Row("2", 30L, "a", 1), Row("2", 10L, "a", 2)),
        Seq(StructField("id", StringType),
            StructField("column", LongType),
            StructField("new_column", StringType),
            StructField("window_agg_column", IntegerType)
        )
      )
        .build()

      val transformer = new AddWindowFunction()
        .setWindowFunction("rank")
        .setPartitionCols(Array("id"))
        .setOrderCols(Array("column"))
        .setAscending(false)
        .setOutputCol("window_agg_column")

      val outputDf = transformer.transform(inputBuilder.build())
      assertDataFrameEquals(outputDf, expectedOutputDf)
    }

    "provide new dense_rank window function column" in {
      val dataset = spark.range(5).withColumn("bucket", col("id") % 3)

      val expectedOutputDf = DataFrameBuilder(
        Seq(Row(0L, 0L, 1),
            Row(0L, 0L, 1),
            Row(3L, 0L, 2),
            Row(3L, 0L, 2),
            Row(1L, 1L, 1),
            Row(1L, 1L, 1),
            Row(4L, 1L, 2),
            Row(4L, 1L, 2),
            Row(2L, 2L, 1),
            Row(2L, 2L, 1)
        ),
        Seq(StructField("id", LongType, nullable = false),
            StructField("bucket", LongType),
            StructField("rank", IntegerType)
        )
      )
        .build()

      val transformer = new AddWindowFunction()
        .setWindowFunction("dense_rank")
        .setPartitionCols(Array("bucket"))
        .setOrderCols(Array("id"))
        .setAscending(true)
        .setOutputCol("rank")

      val outputDf = transformer.transform(dataset.union(dataset))
      assertDataFrameEquals(outputDf, expectedOutputDf)
    }

    "provide new percent_rank window function column" in {
      val dataset = spark.range(5).withColumn("bucket", col("id") % 3)

      val expectedOutputDf = DataFrameBuilder(Seq(Row(0L, 0L, 0.0),
                                                  Row(0L, 0L, 0.0),
                                                  Row(3L, 0L, 0.6666666666666666),
                                                  Row(3L, 0L, 0.6666666666666666),
                                                  Row(1L, 1L, 0.0),
                                                  Row(1L, 1L, 0.0),
                                                  Row(4L, 1L, 0.6666666666666666),
                                                  Row(4L, 1L, 0.6666666666666666),
                                                  Row(2L, 2L, 0.0),
                                                  Row(2L, 2L, 0.0)
                                              ),
                                              Seq(StructField("id", LongType, nullable = false),
                                                  StructField("bucket", LongType),
                                                  StructField("rank", DoubleType)
                                              )
      )
        .build()

      val transformer = new AddWindowFunction()
        .setWindowFunction("percent_rank")
        .setPartitionCols(Array("bucket"))
        .setOrderCols(Array("id"))
        .setAscending(true)
        .setOutputCol("rank")

      val outputDf = transformer.transform(dataset.union(dataset))
      assertDataFrameEquals(outputDf, expectedOutputDf)
    }

    "provide new row_number window function column" in {
      val dataset = spark.range(5).withColumn("bucket", col("id") % 3)

      val expectedOutputDf = DataFrameBuilder(
        Seq(Row(0L, 0L, 1),
            Row(0L, 0L, 2),
            Row(3L, 0L, 3),
            Row(3L, 0L, 4),
            Row(1L, 1L, 1),
            Row(1L, 1L, 2),
            Row(4L, 1L, 3),
            Row(4L, 1L, 4),
            Row(2L, 2L, 1),
            Row(2L, 2L, 2)
        ),
        Seq(StructField("id", LongType, nullable = false),
            StructField("bucket", LongType),
            StructField("rank", IntegerType)
        )
      )
        .build()

      val transformer = new AddWindowFunction()
        .setWindowFunction("row_number")
        .setPartitionCols(Array("bucket"))
        .setOrderCols(Array("id"))
        .setAscending(true)
        .setOutputCol("rank")

      val outputDf = transformer.transform(dataset.union(dataset))
      assertDataFrameEquals(outputDf, expectedOutputDf)
    }

    "provide new cume_dist window function column" in {
      val dataset = spark.range(5).withColumn("bucket", col("id") % 3)

      val expectedOutputDf = DataFrameBuilder(
        Seq(Row(0L, 0L, 0.5),
            Row(0L, 0L, 0.5),
            Row(3L, 0L, 1.0),
            Row(3L, 0L, 1.0),
            Row(1L, 1L, 0.5),
            Row(1L, 1L, 0.5),
            Row(4L, 1L, 1.0),
            Row(4L, 1L, 1.0),
            Row(2L, 2L, 1.0),
            Row(2L, 2L, 1.0)
        ),
        Seq(StructField("id", LongType, nullable = false),
            StructField("bucket", LongType),
            StructField("rank", DoubleType)
        )
      )
        .build()

      val transformer = new AddWindowFunction()
        .setWindowFunction("cume_dist")
        .setPartitionCols(Array("bucket"))
        .setOrderCols(Array("id"))
        .setAscending(true)
        .setOutputCol("rank")

      val outputDf = transformer.transform(dataset.union(dataset))
      assertDataFrameEquals(outputDf, expectedOutputDf)
    }

    "provide new ntile window function column" in {
      val dataset = spark.range(5).withColumn("bucket", col("id") % 3)

      val expectedOutputDf =
        DataFrameBuilder(Seq(Row(0L, 0L, 1), Row(3L, 0L, 2), Row(1L, 1L, 1), Row(4L, 1L, 2), Row(2L, 2L, 1)),
                         Seq(StructField("id", LongType, nullable = false),
                             StructField("bucket", LongType),
                             StructField("rank", IntegerType)
                         )
        )
          .build()

      val transformer = new AddWindowFunction()
        .setOffset("2")
        .setWindowFunction("ntile")
        .setPartitionCols(Array("bucket"))
        .setOrderCols(Array("id"))
        .setAscending(true)
        .setOutputCol("rank")

      val outputDf = transformer.transform(dataset)
      assertDataFrameEquals(outputDf, expectedOutputDf)
    }

    "provide new lag window function column" in {
      val dataset = spark.range(5).withColumn("bucket", col("id") % 3)

      val expectedOutputDf =
        DataFrameBuilder(Seq(Row(0L, 0L, null), Row(3L, 0L, 0L), Row(1L, 1L, null), Row(4L, 1L, 1L), Row(2L, 2L, null)),
                         Seq(StructField("id", LongType, nullable = false),
                             StructField("bucket", LongType),
                             StructField("rank", LongType)
                         )
        )
          .build()

      val transformer = new AddWindowFunction()
        .setInputCol("id")
        .setOffset("1")
        .setWindowFunction("lag")
        .setPartitionCols(Array("bucket"))
        .setOrderCols(Array("id"))
        .setAscending(true)
        .setOutputCol("rank")

      val outputDf = transformer.transform(dataset)
      assertDataFrameEquals(outputDf, expectedOutputDf)
    }

    "provide new lead window function column" in {
      val dataset = spark.range(5).withColumn("bucket", col("id") % 3)

      val expectedOutputDf =
        DataFrameBuilder(Seq(Row(0L, 0L, 3L), Row(3L, 0L, null), Row(1L, 1L, 4L), Row(4L, 1L, null), Row(2L, 2L, null)),
                         Seq(StructField("id", LongType, nullable = false),
                             StructField("bucket", LongType),
                             StructField("rank", LongType)
                         )
        )
          .build()

      val transformer = new AddWindowFunction()
        .setInputCol("id")
        .setOffset("1")
        .setWindowFunction("lead")
        .setPartitionCols(Array("bucket"))
        .setOrderCols(Array("id"))
        .setAscending(true)
        .setOutputCol("rank")

      val outputDf = transformer.transform(dataset)
      assertDataFrameEquals(outputDf, expectedOutputDf)
    }

    "throw an error when requesting invalid window function" in {
      an[IllegalArgumentException] should be thrownBy new AddWindowFunction()
        .setInputCol("column")
        .setWindowFunction("blah")
        .setPartitionCols(Array("id", "new_column"))
        .setOrderCols(Array("id"))
        .setAscending(true)
        .setOutputCol("window_agg_column")
        .transform(inputBuilder.build())
    }
  }

  "getLastDistinctByExpression" should {

    val inputBuilder = DataFrameBuilder(
      Seq(Row("1", 10L, 1, 1),
          Row("1", 0L, 0, 2),
          Row("1", 0L, -1, 3),
          Row("1", 7L, 1, 4),
          Row("1", 0L, 0, 5),
          Row("2", 2L, -1, 1),
          Row("2", 0L, 0, 2),
          Row("2", 0L, 0, 3)
      ),
      Seq(StructField("id", StringType),
          StructField("bucket", LongType),
          StructField("is_outside", IntegerType),
          StructField("order", IntegerType)
      )
    )

    "get last distinct value based on specified expression" in {

      val expected = DataFrameBuilder(Seq(Row("1", 10L, 1, 1, 10L),
                                          Row("1", 0L, 0, 2, 10L),
                                          Row("1", 0L, -1, 3, 0L),
                                          Row("1", 7L, 1, 4, 7L),
                                          Row("1", 0L, 0, 5, 7L),
                                          Row("2", 2L, -1, 1, 2L),
                                          Row("2", 0L, 0, 2, 2L),
                                          Row("2", 0L, 0, 3, 2L)
                                      ),
                                      Seq(StructField("id", StringType),
                                          StructField("bucket", LongType),
                                          StructField("is_outside", IntegerType),
                                          StructField("order", IntegerType),
                                          StructField("last_distinct_bucket", LongType)
                                      )
      ).build()

      val transformer = new AddWindowFunction()
        .setInputCol("bucket")
        .setPartitionCols(Array("id"))
        .setWindowFunction("last_distinct")
        .setExpression("is_outside in (1, -1)")
        .setOrderCols(Array("order"))
        .setOutputCol("last_distinct_bucket")

      val result = transformer
        .transform(inputBuilder.build())
        .orderBy("id", "order")

      assertDataFrameEquals(expected, result)
    }
  }
}
