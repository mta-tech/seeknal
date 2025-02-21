package tech.mta.seeknal.transformers

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField}
import org.scalatest.BeforeAndAfterAll
import tech.mta.seeknal.{BaseSparkSpec, DataFrameBuilder}

class SelectColumnsSpec extends BaseSparkSpec with BeforeAndAfterAll {

  "SelectColumns" should {

    val input = DataFrameBuilder(Seq(Row(1, 10L, "a"), Row(1, 10L, "b"), Row(2, 10L, "a"), Row(2, 10L, "a")),
                                 Seq(StructField("id", IntegerType),
                                     StructField("column1", LongType),
                                     StructField("column2", StringType)
                                 )
    )

    "select the specified columns" in {

      val expectedOutput = DataFrameBuilder(Seq(Row(1, "a"), Row(1, "b"), Row(2, "a"), Row(2, "a")),
                                            Seq(StructField("id", IntegerType), StructField("column2", StringType))
      ).build()

      val transformer = new SelectColumns()
        .setInputCols(Array[String]("id", "column2"))

      val output = transformer.transform(input.build())

      assertDataFrameEquals(expectedOutput, output)
    }
  }
}
