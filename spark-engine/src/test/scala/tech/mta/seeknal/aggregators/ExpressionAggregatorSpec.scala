package tech.mta.seeknal.aggregators

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{LongType, StringType, StructField}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import tech.mta.seeknal.{BaseSparkSpec, DataFrameBuilder}

@RunWith(classOf[JUnitRunner])
class ExpressionAggregatorSpec extends BaseSparkSpec {
  val inputSchema = Seq(StructField("day", StringType), StructField("duration", LongType), StructField("hit", LongType))

  val inputData = Seq(
    // first set
    Row("a", 1L, 2L),
    Row("a", 4L, 8L),
    Row("b", 16L, 32L),
    Row("b", 64L, 128L)
  )

  "ExpressionAggregator" should {
    "sum up all hits and duration with expression" in {
      val inputDf = DataFrameBuilder(inputData, inputSchema)
        .build()

      val expectedOutputSchema =
        Seq(StructField("day", StringType),
            StructField("total_hits", LongType),
            StructField("total_duration", LongType)
        )
      val expectedOutputDf =
        DataFrameBuilder(Seq(Row("a", 10L, 5L), Row("b", 160L, 80L)), expectedOutputSchema)
          .build()

      var aggregators = new ExpressionAggregator()
        .setExpression("sum(hit)")
        .setOutputCol("total_hits")
        .getAll()(Some(spark))
      aggregators = aggregators ++ new ExpressionAggregator()
        .setExpression("sum(duration)")
        .setOutputCol("total_duration")
        .getAll()(Some(spark))

      val outputDf = inputDf
        .groupBy(col("day"))
        .agg(aggregators.head, aggregators.drop(1): _*)
        .orderBy(col("day"))

      assertDataFrameEquals(outputDf, expectedOutputDf)
    }
  }
}
