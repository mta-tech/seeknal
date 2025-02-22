package tech.mta.seeknal.transformers

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StructField}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import tech.mta.seeknal.{BaseSparkSpec, DataFrameBuilder}

@RunWith(classOf[JUnitRunner])
class AddColumnByExprSpec extends BaseSparkSpec {

  "AddColumnByExpr" should {
    val inputBuilder =
      DataFrameBuilder(Seq(Row(1L, 1L), Row(2L, 3L)), Seq(StructField("id", LongType), StructField("column", LongType)))

    "provide new column from a expression" in {
      val expectedOutputDf = DataFrameBuilder(
        Seq(Row(1L, 1L, 2L), Row(2L, 3L, 5L)),
        Seq(StructField("id", LongType), StructField("column", LongType), StructField("new_column", LongType))
      )
        .build()

      val transformer = new AddColumnByExpr()
        .setExpression("id + column")
        .setOutputCol("new_column")

      val outputDf = transformer
        .transform(inputBuilder.build())
      assertDataFrameEquals(outputDf, expectedOutputDf)
    }
  }
}
