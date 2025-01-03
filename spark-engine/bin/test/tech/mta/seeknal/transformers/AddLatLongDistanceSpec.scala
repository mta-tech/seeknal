package tech.mta.seeknal.transformers

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, StructField}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import org.scalatest.matchers.should.Matchers._
import tech.mta.seeknal.{BaseSparkSpec, DataFrameBuilder}

@RunWith(classOf[JUnitRunner])
class AddLatLongDistanceSpec extends BaseSparkSpec {

  "AddLatLongDistanceSpec" should {
    val inputBuilder = DataFrameBuilder(
      Seq(Row(-6.201, 106.781, -6.192, 106.765),
          Row(-60.201, 106.781, 3.195, -106.765),
          Row(30.781, -6.201, -6.216, -80.765),
          Row(-6.216, -80.765, 30.781, -6.201)
      ),
      Seq(StructField("strLat", DoubleType),
          StructField("strLong", DoubleType),
          StructField("endLat", DoubleType),
          StructField("endLong", DoubleType)
      )
    )

    "provide new distance column from sourcePoint to targetPoint" in {
      val expectedOutputDf = DataFrameBuilder(Seq(Row(-6.201, 106.781, -6.192, 106.765, 2.032),
                                                  Row(-60.201, 106.781, 3.195, -106.765, 13066.546),
                                                  Row(30.781, -6.201, -6.216, -80.765, 8906.827),
                                                  Row(-6.216, -80.765, 30.781, -6.201, 8906.827)
                                              ),
                                              Seq(StructField("strLat", DoubleType),
                                                  StructField("strLong", DoubleType),
                                                  StructField("endLat", DoubleType),
                                                  StructField("endLong", DoubleType),
                                                  StructField("distance", DoubleType)
                                              )
      )
        .build()

      val transformer = new AddLatLongDistance()
        .setSourcePoint("strLat,strLong")
        .setTargetPoint("endLat,endLong")
        .setDecimalOffset(3)
        .setOutputCol("distance")

      val outputDf = transformer
        .transform(inputBuilder.build())
      assertDataFrameEquals(outputDf, expectedOutputDf)
    }

    "convert lat long to 0 when requesting invalid lat long" in {
      val transformer = new AddLatLongDistance()

      transformer.harvesineFun(-95.1, 0, 0, 0) shouldBe 0.0
      transformer.harvesineFun(95.1, 0, 0, 0) shouldBe 0.0
      transformer.harvesineFun(0, 181.1, 0, 0) shouldBe 0.0
      transformer.harvesineFun(0, -181.1, 0, 0) shouldBe 0.0
      transformer.harvesineFun(0, 0, -95.1, 0) shouldBe 0.0
      transformer.harvesineFun(0, 0, 95.1, 0) shouldBe 0.0
      transformer.harvesineFun(0, 0, 0, 181.1) shouldBe 0.0
      transformer.harvesineFun(0, 0, 0, -181.1) shouldBe 0.0
    }
  }
}
