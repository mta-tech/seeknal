package tech.mta.seeknal.transformers

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField}
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatestplus.junit.JUnitRunner
import tech.mta.seeknal.{BaseSparkSpec, DataFrameBuilder}

@RunWith(classOf[JUnitRunner])
class CreateTemporaryTableSpec extends BaseSparkSpec with BeforeAndAfterAll {

  "CreateTemporaryTable" should {
    val input = DataFrameBuilder(Seq(Row("1", "a"), Row("2", "b")),
                                 Seq(StructField("id", StringType), StructField("column", StringType))
    )

    val temp = DataFrameBuilder(Seq(Row("2", "a"), Row("3", "b")),
                                Seq(StructField("id_b", StringType), StructField("column", StringType))
    )

    "create temporary table with SQL statement" in {

      val expected_one = DataFrameBuilder(Seq(Row("2", "a", "1"), Row("3", "b", "1")),
                                          Seq(StructField("id_b", StringType),
                                              StructField("column", StringType),
                                              StructField("hit", StringType)
                                          )
      ).build()

      val expected_two = DataFrameBuilder(Seq(Row("a", "1", "2", "1"), Row("b", "2", "3", "1")),
                                          Seq(StructField("column", StringType),
                                              StructField("id", StringType),
                                              StructField("id_b", StringType),
                                              StructField("hit", StringType)
                                          )
      ).build()

      temp.build().createOrReplaceTempView("temp_one")

      val sqlStatement =
        """
          |SELECT *, '1' as hit FROM temp_one
          |""".stripMargin

      val tempTransformer = new CreateTemporaryTable()
        .setStatement(sqlStatement)
        .setTempTableName("temp_two")

      val sameInput = tempTransformer
        .transform(input.build())

      assertDataFrameEquals(input.build(), sameInput)

      val tempTable = spark.table("temp_two")

      assertDataFrameEquals(tempTable, expected_one)

      val joined = sameInput
        .join(tempTable, Seq("column"))
        .sort("id")

      assertDataFrameEquals(joined, expected_two)
    }
  }
}
