package tech.mta.seeknal.transformers

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import tech.mta.seeknal.{BaseSparkSpec, DataFrameBuilder}

@RunWith(classOf[JUnitRunner])
class ColumnRenamedSpec extends BaseSparkSpec {

  "AddColumnRenamed" should {
    val inputBuilder = DataFrameBuilder(Seq(Row("1", "a"), Row("2", "b")),
                                        Seq(StructField("id", StringType), StructField("column", StringType))
    )

    "provide new renamed column from a column" in {
      val expectedOutputDf = DataFrameBuilder(Seq(Row("1", "a"), Row("2", "b")),
                                              Seq(StructField("id", StringType), StructField("new_column", StringType))
      )
        .build()

      val transformer = new ColumnRenamed()
        .setInputCol("column")
        .setOutputCol("new_column")

      val outputDf = transformer.transform(inputBuilder.build())
      assertDataFrameEquals(outputDf, expectedOutputDf)
    }
  }
}
