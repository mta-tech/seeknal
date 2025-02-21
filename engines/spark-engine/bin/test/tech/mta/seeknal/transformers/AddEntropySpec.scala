package tech.mta.seeknal.transformers

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import tech.mta.seeknal.{BaseSparkSpec, DataFrameBuilder}

@RunWith(classOf[JUnitRunner])
class AddEntropySpec extends BaseSparkSpec {

  "AddEntropy" should {

    val inputDF = DataFrameBuilder(
      Seq(Row("1234", "2345", "20180611", "Outgoing", 2),
          Row("1234", "2346", "20180611", "Outgoing", 4),
          Row("1234", "2347", "20180611", "Outgoing", 8),
          Row("1234", "5678", "20180715", "Outgoing", 16),
          Row("1234", "5679", "20180715", null, 16),
          Row("1234", "5679", "20180715", "Outgoing", 16),
          Row("5555", "2345", "20180817", "Ingoing", 1)
      ),
      Seq(StructField("id", StringType),
          StructField("b_party", StringType),
          StructField("date_id", StringType),
          StructField("direction_type", StringType),
          StructField("hit", IntegerType)
      )
    )

    "calculate entropy and entropy column" in {

      val expectedDF = DataFrameBuilder(Seq(Row("1234", "2345", "20180611", "Outgoing", 2, 1.3787834934861753),
                                            Row("1234", "2346", "20180611", "Outgoing", 4, 1.3787834934861753),
                                            Row("1234", "2347", "20180611", "Outgoing", 8, 1.3787834934861753),
                                            Row("1234", "5678", "20180715", "Outgoing", 16, 1.0),
                                            Row("1234", "5679", "20180715", null, 16, 0.0),
                                            Row("1234", "5679", "20180715", "Outgoing", 16, 1.0),
                                            Row("5555", "2345", "20180817", "Ingoing", 1, 0.0)
                                        ),
                                        Seq(StructField("id", StringType),
                                            StructField("b_party", StringType),
                                            StructField("date_id", StringType),
                                            StructField("direction_type", StringType),
                                            StructField("hit", IntegerType),
                                            StructField("entropy", DoubleType)
                                        )
      ).build()

      val transformer = new AddEntropy()
        .setIdCol("id")
        .setEntityCol("hit")
        .setEntropyCol("entropy")
        .setFilterExpression("direction_type = 'Outgoing'")
        .setFrequencyCol("date_id")
        .setGroupByCols(Array("b_party"))

      val outputDF = transformer.transform(inputDF.build())
      assertDataFrameApproximateEquals(outputDF, expectedDF, 0.001)
    }
  }

}
