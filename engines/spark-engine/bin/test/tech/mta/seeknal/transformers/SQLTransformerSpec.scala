package tech.mta.seeknal.transformers

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, StringType, StructField}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import tech.mta.seeknal.{BaseSparkSpec, DataFrameBuilder}

@RunWith(classOf[JUnitRunner])
class SQLTransformerSpec extends BaseSparkSpec {

  "SQLTransformer" should {
    val cellTower = DataFrameBuilder(
      Seq(Row("id1", 40.8379525833, -73.70209875, "20190901"), Row("id2", 46.760613794, -76.80234, "20190901")),
      Seq(StructField("id", StringType),
          StructField("lat", DoubleType),
          StructField("lon", DoubleType),
          StructField("date_id", StringType)
      )
    )

    "run SQL statement" in {

      val expectedDF = DataFrameBuilder(Seq(Row("id1", 40.8379525833, -73.70209875, "20190901"),
                                            Row("id2", 46.760613794, -76.80234, "20190901")
                                        ),
                                        Seq(StructField("id", StringType),
                                            StructField("lat", DoubleType),
                                            StructField("lon", DoubleType),
                                            StructField("date_id", StringType)
                                        )
      ).build()

      val sqlStatement =
        s"""
           |SELECT *
           |FROM __THIS__
           |""".stripMargin

      val sqlTransformer = new SQL()
        .setStatement(sqlStatement)

      val result = sqlTransformer.transform(cellTower.build()).orderBy("id")
      assertDataFrameEquals(expectedDF, result)
    }
  }
}
