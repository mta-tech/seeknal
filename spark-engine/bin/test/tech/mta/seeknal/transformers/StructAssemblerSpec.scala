package tech.mta.seeknal.transformers

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import tech.mta.seeknal.{BaseSparkSpec, DataFrameBuilder}

@RunWith(classOf[JUnitRunner])
class StructAssemblerSpec extends BaseSparkSpec {

  "StructAssembler" should {
    val userLoc = DataFrameBuilder(
      Seq(Row("id1", 40.8379525833, -73.70209875, "20190901"), Row("id2", 46.760613794, -76.802345, "20190901")),
      Seq(StructField("id", StringType),
          StructField("lat", DoubleType),
          StructField("lon", DoubleType),
          StructField("date_id", StringType)
      )
    )

    "assemble struct column with given excludeCols" in {

      val expected = DataFrameBuilder(Seq(Row(Row("id1", 40.8379525833, -73.70209875), "20190901"),
                                          Row(Row("id2", 46.760613794, -76.802345), "20190901")
                                      ),
                                      Seq(
                                        StructField("value",
                                                    StructType(
                                                      List(StructField("id", StringType),
                                                           StructField("lat", DoubleType),
                                                           StructField("lon", DoubleType)
                                                      )
                                                    ),
                                                    nullable = false
                                        ),
                                        StructField("date_id", StringType)
                                      )
      ).build()

      val structAssembler = new StructAssembler()
        .setExcludeCols(Array("date_id"))
        .setOutputCol("value")
        .setRemoveInputCols(true)

      val result = structAssembler.transform(userLoc.build())

      assert(result.columns sameElements Array("value", "date_id"))
      assertDataFrameEquals(expected, result)
    }

    "assemble struct column with given inputCols" in {
      val expected = DataFrameBuilder(Seq(Row(Row("id1", 40.8379525833, -73.70209875), "20190901"),
                                          Row(Row("id2", 46.760613794, -76.802345), "20190901")
                                      ),
                                      Seq(
                                        StructField("value",
                                                    StructType(
                                                      List(StructField("id", StringType),
                                                           StructField("lat", DoubleType),
                                                           StructField("lon", DoubleType)
                                                      )
                                                    ),
                                                    nullable = false
                                        ),
                                        StructField("date_id", StringType)
                                      )
      ).build()

      val structAssembler = new StructAssembler()
        .setInputCols(Array("id", "lat", "lon"))
        .setOutputCol("value")
      val result = structAssembler
        .transform(userLoc.build())
        .select("value", "date_id")

      assert(result.columns sameElements Array("value", "date_id"))
      assertDataFrameEquals(expected, result)
    }

    "keeping original column if removeInputCols set false as the default behaviour" in {
      val expected = DataFrameBuilder(
        Seq(Row("id1", 40.8379525833, -73.70209875, "20190901", Row("id1", 40.8379525833, -73.70209875)),
            Row("id2", 46.760613794, -76.802345, "20190901", Row("id2", 46.760613794, -76.802345))
        ),
        Seq(StructField("id", StringType),
            StructField("lat", DoubleType),
            StructField("lon", DoubleType),
            StructField("date_id", StringType),
            StructField(
              "value",
              StructType(
                List(StructField("id", StringType), StructField("lat", DoubleType), StructField("lon", DoubleType))
              ),
              nullable = false
            )
        )
      ).build()

      val structAssembler = new StructAssembler()
        .setInputCols(Array("id", "lat", "lon"))
        .setOutputCol("value")
        .setRemoveInputCols(false)
      val result = structAssembler.transform(userLoc.build())

      assertDataFrameEquals(expected, result)
    }

    "if both excludeCols and includeCols is not defined, " +
      "it will taking all columns merge it as struct column" in {
        val structAssembler = new StructAssembler()
          .setOutputCol("value")
          .setRemoveInputCols(true)
        val result = structAssembler.transform(userLoc.build())

        val expected = DataFrameBuilder(Seq(Row(Row("id1", 40.8379525833, -73.70209875, "20190901")),
                                            Row(Row("id2", 46.760613794, -76.802345, "20190901"))
                                        ),
                                        Seq(
                                          StructField("value",
                                                      StructType(
                                                        List(StructField("id", StringType),
                                                             StructField("lat", DoubleType),
                                                             StructField("lon", DoubleType),
                                                             StructField("date_id", StringType)
                                                        )
                                                      ),
                                                      nullable = false
                                          )
                                        )
        ).build()

        assert(result.columns sameElements Array("value"))
        assertDataFrameEquals(expected, result)
      }
  }
}
