package tech.mta.seeknal.transformers

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.StructType

abstract class BaseTransformer(override val uid: String) extends Transformer {
  // the UID generation is moved to parent to minimize code duplicate since
  // it's unlikely to get duplicate UUID
  // if it happens that we do get one, this can be moved back to the subclass
  def this() = this(Identifiable.randomUID("BaseTransformer"))

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  // NOTE: don't validate input / output schema for now
  // TODO: this need to be validated to make sure that we can use built-in spark transformer
  // to process output of our custom transformer
  override def transformSchema(schema: StructType): StructType = schema
}

abstract class GroupingTransformer extends BaseTransformer {
  def withAggregators(aggregators: Seq[Column]): GroupingTransformer = this
}
