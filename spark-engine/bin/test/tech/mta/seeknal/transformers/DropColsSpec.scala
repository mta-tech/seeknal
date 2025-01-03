package tech.mta.seeknal.transformers

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import tech.mta.seeknal.{BaseSparkSpec, DataFrameBuilder}

@RunWith(classOf[JUnitRunner])
class DropColsSpec extends BaseSparkSpec {

  "DropColsSpec" should {
    val inputBuilder = DataFrameBuilder(Seq(Row("1", "a", "a", "a"), Row("2", "b", "a", "a"), Row("3", "c", "a", "a")),
                                        Seq(StructField("id", StringType),
                                            StructField("column_one", StringType),
                                            StructField("column_two", StringType),
                                            StructField("column_three", StringType)
                                        )
    )

    "drop columns" in {
      val expectedOutputDf = DataFrameBuilder(Seq(Row("1", "a"), Row("2", "b"), Row("3", "c")),
                                              Seq(StructField("id", StringType), StructField("column_one", StringType))
      )
        .build()

      val dropColumns = new DropCols()
        .setInputCols(Array("column_two", "column_three"))

      val res = dropColumns.transform(inputBuilder.build())
      assertDataFrameEquals(res, expectedOutputDf)
    }
  }
}
