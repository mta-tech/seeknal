package tech.mta.seeknal.aggregators

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{BooleanType, DoubleType, LongType, StringType, StructField}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import tech.mta.seeknal.{BaseSparkSpec, DataFrameBuilder}

@RunWith(classOf[JUnitRunner])
class DayTypeAggregatorSpec extends BaseSparkSpec {

  "DayTypeAggregator" should {
    val inputSchema =
      Seq(StructField("day", StringType), StructField("is_weekday", BooleanType), StructField("hit", LongType))

    val inputData = Seq(
      // first set
      Row("a", true, 2L),
      Row("a", false, 4L),
      Row("a", true, 8L),
      Row("a", false, 16L)
    )

    "sum up all matching hits" in {
      val inputDf = DataFrameBuilder(inputData, inputSchema)
        .build()

      val expectedOutputSchema = Seq(StructField("day", StringType),
                                     StructField("total_alldays", LongType),
                                     StructField("total_wkday", LongType),
                                     StructField("total_wkend", LongType)
      )
      val expectedOutputDf = DataFrameBuilder(Seq(Row("a", 30L, 10L, 20L)), expectedOutputSchema)
        .build()

      val aggregators = new DayTypeAggregator()
        .setInputCol("hit")
        .setOutputCol("total")
        .setWeekdayCol("is_weekday")
        .getAll()(Some(spark))

      val outputDf = inputDf
        .groupBy(col("day"))
        .agg(aggregators.head, aggregators.drop(1): _*)

      assertDataFrameEquals(outputDf, expectedOutputDf)
    }

    "count all matching hits" in {
      val inputDf = DataFrameBuilder(inputData, inputSchema)
        .build()

      val expectedOutputSchema = Seq(StructField("day", StringType),
                                     StructField("count_alldays", LongType, nullable = false),
                                     StructField("count_wkday", LongType, nullable = false),
                                     StructField("count_wkend", LongType, nullable = false)
      )
      val expectedOutputDf = DataFrameBuilder(Seq(Row("a", 4L, 2L, 2L)), expectedOutputSchema)
        .build()

      val aggregators = new DayTypeAggregator()
        .setInputCol("hit")
        .setAccumulatorFunction("count")
        .setOutputCol("count")
        .setWeekdayCol("is_weekday")
        .getAll()(Some(spark))

      val outputDf = inputDf
        .groupBy(col("day"))
        .agg(aggregators.head, aggregators.drop(1): _*)

      assertDataFrameEquals(outputDf, expectedOutputDf)
    }

    "compute average on matching hits" in {
      val inputDf = DataFrameBuilder(inputData, inputSchema)
        .build()

      val expectedOutputSchema = Seq(StructField("day", StringType),
                                     StructField("mean_alldays", DoubleType),
                                     StructField("mean_wkday", DoubleType),
                                     StructField("mean_wkend", DoubleType)
      )
      val expectedOutputDf = DataFrameBuilder(Seq(
                                                // average is based on matching rows only
                                                Row("a", 7.5, 5.0, 10.0)
                                              ),
                                              expectedOutputSchema
      )
        .build()

      val aggregators = new DayTypeAggregator()
        .setInputCol("hit")
        .setAccumulatorFunction("avg")
        .setOutputCol("mean")
        .setWeekdayCol("is_weekday")
        .getAll()(Some(spark))

      val outputDf = inputDf
        .groupBy(col("day"))
        .agg(aggregators.head, aggregators.drop(1): _*)

      assertDataFrameEquals(outputDf, expectedOutputDf)
    }
  }
}
