package tech.mta.seeknal.transformers

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField}
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatestplus.junit.JUnitRunner
import tech.mta.seeknal.{BaseSparkSpec, DataFrameBuilder}

@RunWith(classOf[JUnitRunner])
class MeltSpec extends BaseSparkSpec with BeforeAndAfterAll {

  "MeltSpec" should {
    val input = DataFrameBuilder(
      Seq(Row("1", "a", "d"), Row("2", "b", "c")),
      Seq(StructField("id", StringType), StructField("column", StringType), StructField("column_b", StringType))
    )

    "Reshape dataframe from wide to long" in {

      val expected = DataFrameBuilder(
        Seq(Row("1", "column", "a"), Row("1", "column_b", "d"), Row("2", "column", "b"), Row("2", "column_b", "c")),
        Seq(StructField("id", StringType), StructField("key", StringType), StructField("val", StringType))
      ).build()

      val reshape = new Melt()
        .setInputCols(Array("column", "column_b"))
        .setKeyCols(Array("id"))
        .setOutputKeyCol("key")
        .setOutputValueCol("val")

      val result = reshape.transform(input.build())

      assertDataFrameEquals(expected, result)
    }
  }
}
