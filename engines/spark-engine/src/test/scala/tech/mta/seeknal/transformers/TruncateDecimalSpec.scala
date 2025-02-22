package tech.mta.seeknal.transformers

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, StringType, StructField}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import tech.mta.seeknal.{BaseSparkSpec, DataFrameBuilder}

@RunWith(classOf[JUnitRunner])
class TruncateDecimalSpec extends BaseSparkSpec {

  "TruncateDecimal" should {
    val inputBuilder = DataFrameBuilder(Seq(Row("1", 1.12345), Row("2", 2.34567), Row(null, 5.678), Row("3", 6.1)),
                                        Seq(StructField("id", StringType), StructField("column", DoubleType))
    )

    "provide new renamed column from a column" in {
      val expectedOutputDf = DataFrameBuilder(
        Seq(Row("1", 1.12345, 1.12), Row("2", 2.34567, 2.34), Row(null, 5.678, 5.67), Row("3", 6.1, 6.1)),
        Seq(StructField("id", StringType), StructField("column", DoubleType), StructField("new_column", DoubleType))
      )
        .build()

      val transformer = new TruncateDecimal()
        .setInputCol("column")
        .setOutputCol("new_column")
        .setDecimalOffset(2)
        .setRoundUp(false)

      val outputDf = transformer
        .transform(inputBuilder.build())

      assertDataFrameEquals(outputDf, expectedOutputDf)
    }
  }
}
