package tech.mta.seeknal.connector.serde

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.{DatumReader, Decoder, DecoderFactory}
import org.apache.avro.specific.SpecificDatumReader
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.functions.{col, from_json, udf}
import org.apache.spark.sql.types.StructType
import tech.mta.seeknal.params.{Config, HasAvroBytesReadSerDe}

/** SerDe class for working with avro bytes currently only implement deserialize bytes
  */
class AvroBytesReadSerDe extends BaseSerDe with HasAvroBytesReadSerDe {

  /** deserialize avro bytes by trimming the magic bytes and deserialize with providing schema
    *
    * @param schemaDef
    *   @return
    */
  def avroDeserializationUDF(schemaDef: String): Array[Byte] => String = values => {
    val schema: Schema = new Schema.Parser().parse(schemaDef)
    val message = values.slice(getTrimOffset - 1, values.length)
    val reader: DatumReader[GenericRecord] = new SpecificDatumReader[GenericRecord](schema)
    val decoder: Decoder = DecoderFactory.get().binaryDecoder(message, null)
    reader.read(null, decoder).toString
  }

  /** deserialize data with read the bytes directly
    *
    * @param dataset
    *   @return
    */
  override def transform(dataset: Dataset[_]): DataFrame = {
    val schemaString = schemaReader(dataset.sparkSession)

    val parser = new Schema.Parser()
    val schemaDef = parser.parse(schemaString)
    val schemaStruct = SchemaConverters
      .toSqlType(schemaDef)
      .dataType
      .asInstanceOf[StructType]

    if (!getSerialize) {
      dataset
        .withColumn(Config.SERDE_TEMP_COL, udf(avroDeserializationUDF(schemaString)).apply(col(getInputCol)))
        .withColumn(Config.SERDE_TEMP_COL, from_json(col(Config.SERDE_TEMP_COL), schemaStruct))
    } else {
      throw new NotImplementedError("Serialize method is not implemented")
    }
  }
}
