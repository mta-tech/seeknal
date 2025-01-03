package tech.mta.seeknal.transformers

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import tech.mta.seeknal.{BaseSparkSpec, DataFrameBuilder}
import tech.mta.seeknal.params.ValueMapping

@RunWith(classOf[JUnitRunner])
class ColumnValueRenamedSpec extends BaseSparkSpec {

  "ColumnValueRenamed" should {
    val inputBuilder = DataFrameBuilder(Seq(Row("1", "a"), Row("2", "b")),
                                        Seq(StructField("id", StringType), StructField("column", StringType))
    )

    "provide new column with value renamed" in {
      val expectedOutputDf = DataFrameBuilder(
        Seq(Row("1", "a", "1"), Row("2", "b", "2")),
        Seq(StructField("id", StringType), StructField("column", StringType), StructField("new_column", StringType))
      )
        .build()

      val transformer = new ColumnValueRenamed()
        .setInputCol("column")
        .setValueMappings(Array(ValueMapping("a", "1"), ValueMapping("b", "2")))
        .setOutputCol("new_column")

      val outputDf = transformer.transform(inputBuilder.build())
      assertDataFrameEquals(outputDf, expectedOutputDf)
    }
  }
}
