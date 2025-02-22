package tech.mta.seeknal.aggregators

import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.{LongType, StringType, StructField}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import org.scalatest.matchers.should.Matchers._
import tech.mta.seeknal.{BaseSparkSpec, DataFrameBuilder}

@RunWith(classOf[JUnitRunner])
class FunctionAggregatorSpec extends BaseSparkSpec {
  val inputSchema = Seq(StructField("day", StringType), StructField("duration", LongType), StructField("hit", LongType))

  val inputData = Seq(
    // first set
    Row("a", 1L, 2L),
    Row("a", 4L, 8L),
    Row("b", 16L, 32L),
    Row("b", 64L, 128L)
  )

  "FunctionAggregator" should {
    "sum up all hits" in {
      val inputDf = DataFrameBuilder(inputData, inputSchema)
        .build()

      val expectedOutputSchema =
        Seq(StructField("day", StringType), StructField("total_hits", LongType))
      val expectedOutputDf =
        DataFrameBuilder(Seq(Row("a", 10L), Row("b", 160L)), expectedOutputSchema)
          .build()

      val aggregators = new FunctionAggregator()
        .setInputCol("hit")
        .setOutputCol("total_hits")
        .getAll()(Some(spark))

      val outputDf = inputDf
        .groupBy(col("day"))
        .agg(aggregators.head, aggregators.drop(1): _*)
        .orderBy(col("day"))

      assertDataFrameEquals(outputDf, expectedOutputDf)
    }

    "able use any Spark aggregator function" in {
      val inputDf = DataFrameBuilder(inputData, inputSchema)
        .build()

      val expectedOutputSchema =
        Seq(StructField("day", StringType), StructField("first_hit", LongType))
      val expectedOutputDf =
        DataFrameBuilder(Seq(Row("a", 2L), Row("b", 32L)), expectedOutputSchema)
          .build()

      val aggregator = new FunctionAggregator()
        .setInputCol("hit")
        .setAccumulatorFunction("first")
        .setOutputCol("first_hit")
        .getAll()(Some(spark))

      val outputDf = inputDf
        .groupBy(col("day"))
        .agg(aggregator.head, aggregator.tail: _*)
        .orderBy("day")

      assertDataFrameEquals(expectedOutputDf, outputDf)
    }
  }

  "FunctionAggregator with conditional field" should {
    class TestFunctionAggregator(override val uid: String) extends FunctionAggregator {
      override def columns: Seq[(String, Column)] = {
        val condition = col("duration") > 5L
        Seq((s"all_hits", col("hit")), (s"matching_hits", when(condition, col("hit"))))
      }
    }
    val functionAggregator = new TestFunctionAggregator("123")

    "sum up hits and outputs null for non-matching rows when there's no default value" in {
      val inputDf = DataFrameBuilder(inputData, inputSchema)
        .build()

      val expectedOutputSchema =
        Seq(StructField("day", StringType), StructField("all_hits", LongType), StructField("matching_hits", LongType))
      val expectedOutputDf =
        DataFrameBuilder(Seq(Row("a", 10L, null), Row("b", 160L, 160L)), expectedOutputSchema)
          .build()

      val aggregators = functionAggregator.getAll()(Some(spark))

      val outputDf = inputDf
        .groupBy(col("day"))
        .agg(aggregators.head, aggregators.drop(1): _*)
        .orderBy(col("day"))

      assertDataFrameEquals(outputDf, expectedOutputDf)
    }

    "sum up hits and outputs default value for non-matching rows" in {
      val inputDf = DataFrameBuilder(inputData, inputSchema)
        .build()

      val expectedOutputSchema = Seq(StructField("day", StringType),
                                     // the results are not nullable since there's default value configured
                                     StructField("all_hits", LongType, nullable = false),
                                     StructField("matching_hits", LongType, nullable = false)
      )
      val expectedOutputDf =
        DataFrameBuilder(Seq(Row("a", 10L, 0L), Row("b", 160L, 160L)), expectedOutputSchema)
          .build()

      val aggregators = functionAggregator
        .setDefaultAggregateValue("0")
        .setDefaultAggregateValueType("long")
        .getAll()(Some(spark))

      val outputDf = inputDf
        .groupBy(col("day"))
        .agg(aggregators.head, aggregators.drop(1): _*)
        .orderBy(col("day"))

      assertDataFrameEquals(outputDf, expectedOutputDf)
    }

  }

  "FunctionAggregator companion" should {
    "convert string to requested object" in {
      var result = FunctionAggregator.convertStringToObject("0", "long")
      result shouldBe 0
      result shouldBe a[java.lang.Long]

      result = FunctionAggregator.convertStringToObject("0", "int")
      result shouldBe 0
      result shouldBe a[java.lang.Integer]

      result = FunctionAggregator.convertStringToObject("0", "double")
      result shouldBe 0.0
      result shouldBe a[java.lang.Double]

      result = FunctionAggregator.convertStringToObject("0", "float")
      result shouldBe 0.0
      result shouldBe a[java.lang.Float]

      result = FunctionAggregator.convertStringToObject("0", "string")
      result shouldBe "0"
      result shouldBe a[java.lang.String]
    }

    "throws exception when converting invalid value / type" in {
      // bad type
      an[IllegalArgumentException] should be thrownBy
        FunctionAggregator.convertStringToObject("0", "bad-type")

      // correct type, bad value
      an[IllegalArgumentException] should be thrownBy
        FunctionAggregator.convertStringToObject("a", "long")
      an[IllegalArgumentException] should be thrownBy
        FunctionAggregator.convertStringToObject("a", "int")
      an[IllegalArgumentException] should be thrownBy
        FunctionAggregator.convertStringToObject("a", "double")
      an[IllegalArgumentException] should be thrownBy
        FunctionAggregator.convertStringToObject("a", "float")
      an[IllegalArgumentException] should be thrownBy
        FunctionAggregator.convertStringToObject("0.0", "long")
      an[IllegalArgumentException] should be thrownBy
        FunctionAggregator.convertStringToObject("0.0", "int")
    }
  }
}
