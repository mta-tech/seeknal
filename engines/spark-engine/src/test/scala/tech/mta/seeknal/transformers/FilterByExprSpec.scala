package tech.mta.seeknal.transformers

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import tech.mta.seeknal.{BaseSparkSpec, DataFrameBuilder}

@RunWith(classOf[JUnitRunner])
class FilterByExprSpec extends BaseSparkSpec {

  "FilterByExprSpec" should {
    val inputBuilder = DataFrameBuilder(Seq(Row("1", "a"), Row("2", "b")),
                                        Seq(StructField("id", StringType), StructField("column", StringType))
    )

    "provide new filtered dataframe" in {
      val expectedOutputDf =
        DataFrameBuilder(Seq(Row("1", "a")), Seq(StructField("id", StringType), StructField("column", StringType)))
          .build()

      val transformer = new FilterByExpr()
        .setExpression("column='a'")

      val outputDf = transformer.transform(inputBuilder.build())
      assertDataFrameEquals(outputDf, expectedOutputDf)
    }
  }
}
