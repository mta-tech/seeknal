package tech.mta.seeknal.connector.serde

import org.apache.avro.Schema
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.functions.{col, from_json, to_json}
import org.apache.spark.sql.types.StructType
import tech.mta.seeknal.params.{Config, HasJsonSerDe}


/**
  * SerDe class for working with json column
  */
class JsonSerDe extends BaseSerDe with HasJsonSerDe {

  /**
    * serialize/deserialize column to/from json
    *
    * @param dataset
    * @return
    */
  override def transform(dataset: Dataset[_]): DataFrame = {

    if (getSerialize) {
      dataset
        .withColumn(getOutputCol, to_json(col(getInputCol)))
    } else {
     if (isDefined(schemaFile)) {
       val schema = schemaReader(dataset.sparkSession)
       val schemaDef: Schema = new Schema.Parser().parse(schema)
       val schemaStruct = SchemaConverters.toSqlType(schemaDef).dataType.asInstanceOf[StructType]

       dataset
         .withColumn(getInputCol, col(getInputCol).cast("string"))
         .withColumn(Config.SERDE_TEMP_COL, from_json(col(getInputCol), schemaStruct))
     } else if (isDefined(schemaString)) {
         val schemaDef: Schema = new Schema.Parser().parse(getSchemaString)
         val schemaStruct = SchemaConverters.toSqlType(schemaDef).dataType.asInstanceOf[StructType]

         dataset
           .withColumn(getInputCol, col(getInputCol).cast("string"))
           .withColumn(Config.SERDE_TEMP_COL, from_json(col(getInputCol), schemaStruct))
     } else {
       var schema = new StructType()
       getColsDefinition.foreach(x =>
         schema = schema
          .add(x.name, x.dataType)
       )
       dataset
         .withColumn(getInputCol, col(getInputCol).cast("string"))
         .withColumn(Config.SERDE_TEMP_COL, from_json(col(getInputCol), schema))
     }
    }
  }
}
