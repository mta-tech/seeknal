package tech.mta.seeknal.connector

import io.circe.JsonObject
import org.apache.hadoop.fs.Path
import tech.mta.seeknal.params.{ColumnDefinition, SerDeConfig, StreamingTrigger}
import tech.mta.seeknal.pipeline.{Bucketing, DbConfig, PipelineConfig, SourceProperties}

case class SourceConfig(
    // should be unique
    id: Option[String] = None,
    // datasource type, eg. hive, parquet, csv, etc. defaults to hive
    // for now only hive is supported
    source: Option[String] = None,
    connId: Option[String] = None,
    // Hive Params
    // hive database configuration
    db: Option[DbConfig] = None,
    // table name, for hive
    table: Option[String] = None,
    // path  to the dataset, not needed for hive input,
    // needed for hive external table or file output
    path: Option[String] = None,
    partitions: Option[Seq[String]] = None,
    bucketing: Option[Bucketing] = None,
    limit: Option[Int] = None,
    params: Option[JsonObject] = None,
    // serde options
    serde: Option[Seq[SerDeConfig]] = None,
    // if kind is streaming, use for indicate trigger settings
    // of a streaming query.
    trigger: Option[StreamingTrigger] = None,
    // use for define schema corresponding to input-output
    schema: Option[Seq[ColumnDefinition]] = None,
    // set repartition number
    repartition: Option[Int] = None
) {

  val sourceProperties = new SourceProperties()
  PipelineConfig.configureParams(sourceProperties, params.getOrElse(JsonObject.empty))

  /** get the table name of this data source
    *
    *   - table name is derived from `table` key if it exists, else the id will be used
    *   - if table name doesn't include database name, db name will be used if it exists
    *   - else returns the table name as is
    */
  def getTableName: String = {
    val tableName = table.getOrElse(id.get)
    if (!tableName.contains(".") && db.isDefined) {
      s"${db.get.name}.${tableName}"
    } else {
      tableName
    }
  }

  /** get path of where the parquet or file will be stored.
    *
    * If path explicitly specified, then return path. If path not explicitly specified but is specified in params, then
    * return the path specified in params. Otherwise will use path from db definition
    *
    * @return
    */
  def getPath: String = {
    if (path.isDefined) {
      path.get
    } else if (params.isDefined && params.get.contains("path")) {
      params.get("path").get.asString.get
    } else if (db.isDefined && db.get.path.isDefined && table.isDefined) {
      new Path(db.get.path.get, table.get.split('.').last).toString
    } else {
      null
    }
  }
}
