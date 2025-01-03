package tech.mta.seeknal.features

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.SparkSession
import tech.mta.seeknal.pipeline.{FeatureDimension, PipelineConfig, SubFeature}

abstract class FeatureGenerator extends Params {
  val uid: String = Identifiable.randomUID("FeatureGenerator")
  override def copy(extra: ParamMap): FeatureGenerator = defaultCopy(extra)

  def getTransformers(frequency: String, features: Option[Seq[SubFeature]] = None)(implicit
      spark: Option[SparkSession]
  ): Seq[Transformer]

  def configureParams(config: PipelineConfig): FeatureGenerator = {
    // override this if necessary
    this
  }
}
