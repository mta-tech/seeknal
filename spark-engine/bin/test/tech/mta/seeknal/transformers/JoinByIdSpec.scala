package tech.mta.seeknal.transformers

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField}
import org.junit.runner.RunWith
import org.scalatest.Outcome
import org.scalatestplus.junit.JUnitRunner
import tech.mta.seeknal.{BaseSparkSpec, DataFrameBuilder}

@RunWith(classOf[JUnitRunner])
class JoinByIdSpec extends BaseSparkSpec {

  override def withFixture(test: NoArgTest): Outcome = {
    val tableToJoin = DataFrameBuilder(
      Seq(Row("1", "c", "a"), Row("2", "d", "d")),
      Seq(StructField("id", StringType), StructField("column_2", StringType), StructField("column", StringType))
    )
      .build()

    tableToJoin.write.mode("overwrite").saveAsTable("test")

    try super.withFixture(test)
    finally {
      spark.sql("DROP TABLE IF EXISTS test")
    }
  }

  "JoinById" should {
    val inputBuilder = DataFrameBuilder(Seq(Row("1", "a"), Row("2", "b")),
                                        Seq(StructField("id", StringType), StructField("column", StringType))
    )

    "provide new joined table" in {
      val expectedOutputDf = DataFrameBuilder(Seq(Row("1", "a", "c", "a"), Row("2", "b", "d", "d")),
                                              Seq(StructField("id", StringType),
                                                  StructField("column", StringType),
                                                  StructField("column_2", StringType),
                                                  StructField("column", StringType)
                                              )
      )
        .build()

      val transformer = new JoinById()
        .setInputCols(Array("id"))
        .setTargetTable("test")
        .setJoinType("left")

      val outputDf = transformer.transform(inputBuilder.build())
      assertDataFrameEquals(outputDf, expectedOutputDf)
    }

    "provide new joined table with filter" in {
      val expectedOutputDf = DataFrameBuilder(Seq(Row("1", "a", null, null), Row("2", "b", "d", "d")),
                                              Seq(StructField("id", StringType),
                                                  StructField("column", StringType),
                                                  StructField("column_2", StringType),
                                                  StructField("column", StringType)
                                              )
      )
        .build()

      val transformer = new JoinById()
        .setInputCols(Array("id"))
        .setTargetTable("test")
        .setFilterExpression("column_2 != 'c'")
        .setJoinType("left")

      val outputDf = transformer.transform(inputBuilder.build())
      assertDataFrameEquals(outputDf, expectedOutputDf)
    }

    "provide new joined table with two cols" in {
      val expectedOutputDf = DataFrameBuilder(Seq(Row("2", "b", null), Row("1", "a", "c"), Row("2", "d", "d")),
                                              Seq(StructField("id", StringType),
                                                  StructField("column", StringType),
                                                  StructField("column_2", StringType)
                                              )
      )
        .build()

      val transformer = new JoinById()
        .setInputCols(Array("id", "column"))
        .setTargetTable("test")
        .setJoinType("full_outer")

      val outputDf = transformer.transform(inputBuilder.build())
      assertDataFrameEquals(outputDf.orderBy("id"), expectedOutputDf.orderBy("id"))
    }
  }
}
