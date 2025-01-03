package tech.mta.seeknal.params

import io.circe.JsonObject

object Config {

  val SCHEMA_REGISTRY_URL = "schema.registry.url"
  val TOPIC_NAME_STRATEGY = "topicNameStrategy"
  val RECORD_NAME_STRATEGY = "recordNameStrategy"
  val TOPIC_RECORD_NAME_STRATEGY = "topicRecordNameStrategy"
  val SERDE_TEMP_COL = "_parsed"
  val CONFLUENT_AVRO = "confluentAvro"
  val VANILLA_AVRO = "simpleAvro"
  val DUMMY_SCHEMA_REGISTRY_URL = "mock://dummySchemaRegistry"
  val TRIGGER_PROCESSING_TIME = "processingTime"
  val TRIGGER_CONTINUOUS = "continuous"
  val TRIGGER_ONCE = "once"
  val PIPELINE_BATCH = "batch"
  val PIPELINE_STREAMING = "streaming"
  val DEFAULT_AUTHOR = "dr.who"
  val MAX_RESULT = 100
  val STRUCT_COLUMN_FOR_AVRO = "__struct_column"
}

case class SerDeConfig(className: String,
                       params: Option[JsonObject],
                       keepCols: Option[Seq[String]] = None,
                       outputCol: Option[String] = None
)

case class StreamingTrigger(mode: String = "processingTime", time: Option[String] = None)
