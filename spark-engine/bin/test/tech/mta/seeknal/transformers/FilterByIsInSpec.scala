package tech.mta.seeknal.transformers

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import tech.mta.seeknal.{BaseSparkSpec, DataFrameBuilder}

@RunWith(classOf[JUnitRunner])
class FilterByIsInSpec extends BaseSparkSpec {

  "FilterByIsInSpec" should {
    val inputBuilder = DataFrameBuilder(Seq(Row("1", "a"), Row("2", "b"), Row("3", "c")),
                                        Seq(StructField("id", StringType), StructField("column", StringType))
    )

    "get the data meet on specified List" in {
      val expectedOutputDf = DataFrameBuilder(Seq(Row("1", "a"), Row("2", "b")),
                                              Seq(StructField("id", StringType), StructField("column", StringType))
      )
        .build()

      val filter = new FilterByIsIn()
        .setInputCol("column")
        .setValues(List("a", "b"))

      val res = filter.transform(inputBuilder.build())
      assertDataFrameEquals(res, expectedOutputDf)
    }
  }
}
