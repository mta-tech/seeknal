package tech.mta.seeknal.transformers

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import tech.mta.seeknal.{BaseSparkSpec, DataFrameBuilder}

@RunWith(classOf[JUnitRunner])
class CoalesceSpec extends BaseSparkSpec {

  "Coalesce" should {
    val inputBuilder =
      DataFrameBuilder(Seq(Row("1", "a", null, null), Row("2", null, "b", null), Row("3", null, null, "c")),
                       Seq(StructField("id", StringType),
                           StructField("a", StringType),
                           StructField("b", StringType),
                           StructField("c", StringType)
                       )
      )

    "provide new coalesced column from columns" in {
      val expectedOutputDf = DataFrameBuilder(
        Seq(Row("1", "a", null, null, "a"), Row("2", null, "b", null, "b"), Row("3", null, null, "c", "c")),
        Seq(StructField("id", StringType),
            StructField("a", StringType),
            StructField("b", StringType),
            StructField("c", StringType),
            StructField("new_column", StringType)
        )
      )
        .build()

      val transformer = new Coalesce()
        .setInputCols(Array("a", "b", "c"))
        .setOutputCol("new_column")

      val outputDf = transformer.transform(inputBuilder.build())
      assertDataFrameEquals(outputDf, expectedOutputDf)
    }
  }
}
