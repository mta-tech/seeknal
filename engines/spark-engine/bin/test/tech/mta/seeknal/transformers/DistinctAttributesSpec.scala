package tech.mta.seeknal.transformers

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.asc
import org.apache.spark.sql.types.{DoubleType, StringType, StructField}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import tech.mta.seeknal.{BaseSparkSpec, DataFrameBuilder}

@RunWith(classOf[JUnitRunner])
class DistinctAttributesSpec extends BaseSparkSpec {

  "DistinctAttributes" should {
    val inputBuilder = DataFrameBuilder(
      Seq(Row("abc", "1", "97", 1.0),
          Row("abc", "1", "99", 2.0),
          Row("abc", "2", "98", 1.0),
          Row("abc", "2", "98", 1.0),
          Row("def", "1", "90", 1.0),
          Row("def", "", "90", 1.0),
          Row("def", "1", "", 1.0),
          Row("def", "1", "97", 1.0),
          Row("def", "1", "97", null),
          Row("def", "1", "97", Double.NaN),
          Row("def", "1", null, 1.0)
      ),
      Seq(StructField("day", StringType),
          StructField("user", StringType),
          StructField("counterparty", StringType, nullable = true),
          StructField("value", DoubleType, nullable = true)
      )
    )

    "provide distinct values and ignore empty ones" in {
      val grouping = new DistinctAttributes()
        .setInputCols(Array("day", "user", "counterparty", "value"))
      val expectedOutputDf = DataFrameBuilder(Seq(Row("abc", "1", "97", 1.0),
                                                  Row("abc", "1", "99", 2.0),
                                                  Row("abc", "2", "98", 1.0),
                                                  Row("def", "1", "90", 1.0),
                                                  Row("def", "1", "97", 1.0),
                                                  Row("def", "1", "97", Double.NaN)
                                              ),
                                              Seq(StructField("day", StringType),
                                                  StructField("user", StringType),
                                                  StructField("counterparty", StringType),
                                                  StructField("value", DoubleType)
                                              )
      )
        .build()

      val outputDf = grouping
        .transform(inputBuilder.build())
        .orderBy(asc("day"), asc("user"), asc("counterparty"), asc("value"))
      assertDataFrameEquals(outputDf, expectedOutputDf)
    }

    "provide distinct values and NOT ignore empty ones" in {
      val grouping = new DistinctAttributes()
        .setInputCols(Array("day", "user", "counterparty", "value"))
        .setNullFilter(false)
      val expectedOutputDf = DataFrameBuilder(Seq(Row("abc", "1", "97", 1.0),
                                                  Row("abc", "1", "99", 2.0),
                                                  Row("abc", "2", "98", 1.0),
                                                  Row("def", "1", "90", 1.0),
                                                  Row("def", "", "90", 1.0),
                                                  Row("def", "1", "97", 1.0),
                                                  Row("def", "1", "97", null),
                                                  Row("def", "1", "97", Double.NaN),
                                                  Row("def", "1", null, 1.0),
                                                  Row("def", "1", "", 1.0)
                                              ),
                                              Seq(StructField("day", StringType),
                                                  StructField("user", StringType),
                                                  StructField("counterparty", StringType),
                                                  StructField("value", DoubleType)
                                              )
      )
        .build()

      val expected = expectedOutputDf.orderBy(asc("day"), asc("user"), asc("counterparty"), asc("value"))

      val outputDf = grouping
        .transform(inputBuilder.build())
        .orderBy(asc("day"), asc("user"), asc("counterparty"), asc("value"))

      assertDataFrameEquals(outputDf, expected)
    }

    "distinct values with only string columns" in {
      val grouping = new DistinctAttributes()
        .setInputCols(Array("day", "user", "counterparty"))

      val expectedOutputDf = DataFrameBuilder(
        Seq(Row("abc", "1", "97"),
            Row("abc", "1", "99"),
            Row("abc", "2", "98"),
            Row("def", "1", "90"),
            Row("def", "1", "97")
        ),
        Seq(StructField("day", StringType), StructField("user", StringType), StructField("counterparty", StringType))
      )
        .build()

      val outputDf = grouping
        .transform(inputBuilder.build())
        .orderBy(asc("day"), asc("user"), asc("counterparty"))
      assertDataFrameEquals(outputDf, expectedOutputDf)
    }

    "distinct values with only non-string columns" in {
      val grouping = new DistinctAttributes()
        .setInputCols(Array("value"))

      val expectedOutputDf =
        DataFrameBuilder(Seq(Row(1.0), Row(2.0), Row(Double.NaN)), Seq(StructField("value", DoubleType)))
          .build()

      val outputDf = grouping
        .transform(inputBuilder.build())
        .orderBy(asc("value"))
      assertDataFrameEquals(outputDf, expectedOutputDf)
    }
  }
}
