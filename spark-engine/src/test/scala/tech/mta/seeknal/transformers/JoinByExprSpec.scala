package tech.mta.seeknal.transformers

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField}
import org.junit.runner.RunWith
import org.scalatest.Outcome
import org.scalatestplus.junit.JUnitRunner
import tech.mta.seeknal.{BaseSparkSpec, DataFrameBuilder}

@RunWith(classOf[JUnitRunner])
class JoinByExprSpec extends BaseSparkSpec {

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

  "JoinByExpr" should {
    val inputBuilder = DataFrameBuilder(Seq(Row("1", "a"), Row("2", "b")),
                                        Seq(StructField("id", StringType), StructField("column", StringType))
    )

    "provide new joined table" in {
      val expectedOutputDf = DataFrameBuilder(Seq(Row("1", "a", "1", "c", "a"), Row("2", "b", "2", "d", "d")),
                                              Seq(StructField("id", StringType),
                                                  StructField("column", StringType),
                                                  StructField("id", StringType),
                                                  StructField("column_2", StringType),
                                                  StructField("column", StringType)
                                              )
      )
        .build()

      val transformer = new JoinByExpr()
        .setJoinExpression("a.id = b.id")
        .setTargetTable("test")
        .setJoinType("left")

      val outputDf = transformer.transform(inputBuilder.build())
      assertDataFrameEquals(outputDf, expectedOutputDf)
    }

    "provide new joined table with broadcast" in {
      val expectedOutputDf = DataFrameBuilder(Seq(Row("1", "a", "1", "c", "a"), Row("2", "b", "2", "d", "d")),
                                              Seq(StructField("id", StringType),
                                                  StructField("column", StringType),
                                                  StructField("id", StringType),
                                                  StructField("column_2", StringType),
                                                  StructField("column", StringType)
                                              )
      )
        .build()

      val transformer = new JoinByExpr()
        .setJoinExpression("a.id = b.id")
        .setTargetTable("test")
        .setJoinType("left")
        .setDoBroadcast(true)

      val outputDf = transformer.transform(inputBuilder.build())
      assertDataFrameEquals(outputDf, expectedOutputDf)
    }

    "provide new joined table with filter" in {
      val expectedOutputDf = DataFrameBuilder(Seq(Row("1", "a", null, null, null), Row("2", "b", "2", "d", "d")),
                                              Seq(StructField("id", StringType),
                                                  StructField("column", StringType),
                                                  StructField("id", StringType),
                                                  StructField("column_2", StringType),
                                                  StructField("column", StringType)
                                              )
      )
        .build()

      val transformer = new JoinByExpr()
        .setJoinExpression("a.id = b.id")
        .setTargetTable("test")
        .setFilterExpression("column_2 != 'c'")
        .setJoinType("left")

      val outputDf = transformer.transform(inputBuilder.build())
      assertDataFrameEquals(outputDf, expectedOutputDf)
    }

    "provide new joined table with two cols" in {
      val expectedOutputDf = DataFrameBuilder(
        Seq(Row("2", "b", null, null, null), Row("1", "a", "1", "c", "a"), Row(null, null, "2", "d", "d")),
        Seq(StructField("id", StringType),
            StructField("column", StringType),
            StructField("id", StringType),
            StructField("column_2", StringType),
            StructField("column", StringType)
        )
      )
        .build()

      val transformer = new JoinByExpr()
        .setInputCols(Array("id", "column"))
        .setJoinExpression("a.id = b.id and a.column = b.column")
        .setTargetTable("test")
        .setJoinType("full_outer")

      val outputDf = transformer.transform(inputBuilder.build())
      assertDataFrameEquals(outputDf.orderBy("column_2"), expectedOutputDf.orderBy("column_2"))
    }

  }
}
