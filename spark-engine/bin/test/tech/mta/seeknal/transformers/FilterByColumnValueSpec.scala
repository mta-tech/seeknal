package tech.mta.seeknal.transformers

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import tech.mta.seeknal.{BaseSparkSpec, DataFrameBuilder}

@RunWith(classOf[JUnitRunner])
class FilterByColumnValueSpec extends BaseSparkSpec {

  "FilterByColumnValue" should {
    val inputBuilder = DataFrameBuilder(Seq(Row("1", "a", null, "a"), Row("2", "b", "a", "b"), Row("2", "c", "a", "b")),
                                        Seq(StructField("id", StringType),
                                            StructField("column_to_filter", StringType),
                                            StructField("column_value", StringType),
                                            StructField("column_value_2", StringType)
                                        )
    )

    "provide filtered dataframe by first non-null value from a column" in {
      val expectedOutputDf = DataFrameBuilder(Seq(Row("1", "a", null, "a")),
                                              Seq(StructField("id", StringType),
                                                  StructField("column_to_filter", StringType),
                                                  StructField("column_value", StringType),
                                                  StructField("column_value_2", StringType)
                                              )
      )
        .build()

      val transformer = new FilterByColumnValue()
        .setInputCol("column_to_filter")
        .setValueCol("column_value")

      val outputDf = transformer.transform(inputBuilder.build())
      assertDataFrameEquals(outputDf, expectedOutputDf)
    }

    "provide filtered dataframe by distinct values from a column" in {
      val expectedOutputDf = DataFrameBuilder(Seq(Row("1", "a", null, "a"), Row("2", "b", "a", "b")),
                                              Seq(StructField("id", StringType),
                                                  StructField("column_to_filter", StringType),
                                                  StructField("column_value", StringType),
                                                  StructField("column_value_2", StringType)
                                              )
      )
        .build()

      val transformer = new FilterByColumnValue()
        .setInputCol("column_to_filter")
        .setValueCol("column_value_2")

      val outputDf = transformer.transform(inputBuilder.build())
      assertDataFrameEquals(outputDf, expectedOutputDf)
    }

  }
}
