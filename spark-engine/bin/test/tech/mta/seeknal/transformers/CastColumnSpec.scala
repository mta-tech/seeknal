package tech.mta.seeknal.transformers

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DecimalType, DoubleType, FloatType, IntegerType, LongType, StringType, StructField}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import tech.mta.seeknal.{BaseSparkSpec, DataFrameBuilder}

@RunWith(classOf[JUnitRunner])
class CastColumnSpec extends BaseSparkSpec {

  "CastColumn" should {
    val inputBuilder = DataFrameBuilder(
      Seq(Row("1", "1.2", "1.234", 1, "20180301", "abc"), Row("2", "2.3", "2.345", 2, "20180302", "cba")),
      Seq(StructField("column", StringType),
          StructField("column_1", StringType),
          StructField("column_2", StringType),
          StructField("column_3", IntegerType),
          StructField("column_4", StringType),
          StructField("column_5", StringType)
      )
    )

    "cast column to integer" in {
      val expectedOutputDf = DataFrameBuilder(Seq(Row("1", "1.2", "1.234", 1, "20180301", "abc", 1),
                                                  Row("2", "2.3", "2.345", 2, "20180302", "cba", 2)
                                              ),
                                              Seq(StructField("column", StringType),
                                                  StructField("column_1", StringType),
                                                  StructField("column_2", StringType),
                                                  StructField("column_3", IntegerType),
                                                  StructField("column_4", StringType),
                                                  StructField("column_5", StringType),
                                                  StructField("new_column", IntegerType)
                                              )
      ).build()

      val transformer = new CastColumn()
        .setInputCol("column")
        .setOutputCol("new_column")
        .setDataType("int")

      val outputDf = transformer.transform(inputBuilder.build())
      assertDataFrameEquals(outputDf, expectedOutputDf)
    }

    "cast column to double" in {
      val expectedOutputDf = DataFrameBuilder(Seq(Row("1", "1.2", "1.234", 1, "20180301", "abc", 1.2),
                                                  Row("2", "2.3", "2.345", 2, "20180302", "cba", 2.3)
                                              ),
                                              Seq(StructField("column", StringType),
                                                  StructField("column_1", StringType),
                                                  StructField("column_2", StringType),
                                                  StructField("column_3", IntegerType),
                                                  StructField("column_4", StringType),
                                                  StructField("column_5", StringType),
                                                  StructField("new_column", DoubleType)
                                              )
      ).build()

      val transformer = new CastColumn()
        .setInputCol("column_1")
        .setOutputCol("new_column")
        .setDataType("double")

      val outputDf = transformer.transform(inputBuilder.build())
      assertDataFrameEquals(outputDf, expectedOutputDf)
    }

    "cast column to bigint" in {
      val expectedOutputDf = DataFrameBuilder(Seq(Row("1", "1.2", "1.234", 1, "20180301", "abc", 1L),
                                                  Row("2", "2.3", "2.345", 2, "20180302", "cba", 2L)
                                              ),
                                              Seq(StructField("column", StringType),
                                                  StructField("column_1", StringType),
                                                  StructField("column_2", StringType),
                                                  StructField("column_3", IntegerType),
                                                  StructField("column_4", StringType),
                                                  StructField("column_5", StringType),
                                                  StructField("new_column", LongType)
                                              )
      ).build()

      val transformer = new CastColumn()
        .setInputCol("column")
        .setOutputCol("new_column")
        .setDataType("bigint")

      val outputDf = transformer.transform(inputBuilder.build())
      assertDataFrameEquals(outputDf, expectedOutputDf)
    }

    "cast column to float" in {
      val expectedOutputDf = DataFrameBuilder(Seq(Row("1", "1.2", "1.234", 1, "20180301", "abc", 1.2f),
                                                  Row("2", "2.3", "2.345", 2, "20180302", "cba", 2.3f)
                                              ),
                                              Seq(StructField("column", StringType),
                                                  StructField("column_1", StringType),
                                                  StructField("column_2", StringType),
                                                  StructField("column_3", IntegerType),
                                                  StructField("column_4", StringType),
                                                  StructField("column_5", StringType),
                                                  StructField("new_column", FloatType)
                                              )
      ).build()

      val transformer = new CastColumn()
        .setInputCol("column_1")
        .setOutputCol("new_column")
        .setDataType("float")

      val outputDf = transformer.transform(inputBuilder.build())
      assertDataFrameEquals(outputDf, expectedOutputDf)
    }

    "cast column to decimal" in {
      val expectedOutputDf = DataFrameBuilder(Seq(Row("1", "1.2", "1.234", 1, "20180301", "abc", BigDecimal(1.234)),
                                                  Row("2", "2.3", "2.345", 2, "20180302", "cba", BigDecimal(2.345))
                                              ),
                                              Seq(StructField("column", StringType),
                                                  StructField("column_1", StringType),
                                                  StructField("column_2", StringType),
                                                  StructField("column_3", IntegerType),
                                                  StructField("column_4", StringType),
                                                  StructField("column_5", StringType),
                                                  StructField("new_column", DecimalType(10, 3))
                                              )
      ).build()

      val transformer = new CastColumn()
        .setInputCol("column_2")
        .setOutputCol("new_column")
        .setDataType("decimal")
        .setPrecision(10)
        .setScale(3)

      val outputDf = transformer.transform(inputBuilder.build())
      assertDataFrameEquals(outputDf, expectedOutputDf)
    }

    "cast column to string" in {
      val expectedOutputDf = DataFrameBuilder(Seq(Row("1", "1.2", "1.234", 1, "20180301", "abc", "1"),
                                                  Row("2", "2.3", "2.345", 2, "20180302", "cba", "2")
                                              ),
                                              Seq(StructField("column", StringType),
                                                  StructField("column_1", StringType),
                                                  StructField("column_2", StringType),
                                                  StructField("column_3", IntegerType),
                                                  StructField("column_4", StringType),
                                                  StructField("column_5", StringType),
                                                  StructField("new_column", StringType)
                                              )
      ).build()

      val transformer = new CastColumn()
        .setInputCol("column_3")
        .setOutputCol("new_column")
        .setDataType("string")

      val outputDf = transformer.transform(inputBuilder.build())
      assertDataFrameEquals(outputDf, expectedOutputDf)
    }

    "throw an error when input invalid datatype" in {
      an[IllegalArgumentException] should be thrownBy new CastColumn()
        .setInputCol("column_4")
        .setOutputCol("new_column")
        .setDataType("blah")
        .transform(inputBuilder.build())
    }

    "cast invalid input become null" in {
      val expectedOutputDf = DataFrameBuilder(Seq(Row("1", "1.2", "1.234", 1, "20180301", "abc", null),
                                                  Row("2", "2.3", "2.345", 2, "20180302", "cba", null)
                                              ),
                                              Seq(StructField("column", StringType),
                                                  StructField("column_1", StringType),
                                                  StructField("column_2", StringType),
                                                  StructField("column_3", IntegerType),
                                                  StructField("column_4", StringType),
                                                  StructField("column_5", StringType),
                                                  StructField("new_column", IntegerType)
                                              )
      ).build()

      val transformer = new CastColumn()
        .setInputCol("column_5")
        .setOutputCol("new_column")
        .setDataType("int")

      val outputDf = transformer.transform(inputBuilder.build())
      assertDataFrameEquals(outputDf, expectedOutputDf)
    }
  }
}
