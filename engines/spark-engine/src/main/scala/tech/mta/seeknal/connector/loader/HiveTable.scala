package tech.mta.seeknal.connector.loader

import scala.util.{Failure, Success, Try}

import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger, LogManager}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{StructField, StructType}

/** Utility to update hive metadata of the parquet files used to store the DataFrame
  *
  * TODO: handle table with multiple partitions
  *
  * @param spark
  *   the SparkSession
  * @param name
  *   the hive table name
  * @param location
  *   the parquet file location
  * @param partitionColumn
  *   the partition column
  */
class HiveTable(spark: SparkSession, name: String, location: Path, partitionColumn: Option[Seq[String]]) {

  val logger: Logger = LogManager.getLogger(this.getClass)
  logger.setLevel(Level.INFO)

  /** Update hive table metadata with information from the new DataFrame
    *
    * If table already exists, make sure the schema is the same as the new DataFrame If not, simply create external
    * table Then run MSCK REPAIR TABLE to sync partition info
    *
    * @param df
    *   the newly created DataFrame
    */
  def update(df: DataFrame): Unit = {
    val tableExists = checkTableExists(name)

    if (!tableExists) {
      // table doesn't exists, create the table
      logger.info(s"Table $name doesn't exist")
      create(df)
    } else {
      // table exists
      logger.info(s"Table $name exists")
      val currentTable = spark.table(name)

      if (!schemaCompatibleWith(currentTable.schema, df.schema)) {
        logger.info(s"Schema is not compatible")
        // recreate table if schema is different
        // TODO: do alter table instead
        drop()
        create(df)
      } else {
        refresh()
      }
    }
    if (partitionColumn.isDefined) {
      repair()
    }
  }

  protected def checkTableExists(name: String): Boolean = {
    // in spark 2.1, database name need to be set explicitly
    // scalastyle:off
    // https://stackoverflow.com/questions/46477270/spark-scala-how-can-i-check-if-a-table-exists-in-hive/46477521?noredirect=1#comment87175190_46477521
    // scalastyle:on
    val dbName = if (name.contains('.')) name.split('.').head else "default"
    spark.catalog.tableExists(dbName, name.split('.').last)
  }

  /** Check if current table schema is compatible with the new one from DataFrame.
    *
    * Two schemas are compatible if their partitions and fields are the same
    *
    * @param currentSchema
    *   the schema of current table
    * @param newSchema
    *   the schema of new DataFrame
    */
  private def schemaCompatibleWith(currentSchema: StructType, newSchema: StructType): Boolean = {
    partitionColumn match {
      case Some(hasPartition) =>
        // Compare partition
        val currentPartition = getPartitionColumn
        if (currentPartition != hasPartition.mkString(",")) {
          logger.info(s"Incompatible: $currentPartition != ${hasPartition.mkString(",")}")
          return false
        }
      case None =>
        val currentPartition = Try {
          getPartitionColumn
        } match {
          case Success(value) =>
            value
          case Failure(exception) =>
            ""
        }

        if (currentPartition != "") {
          logger.info(s"Incompatible: $currentPartition != ''")
          return false
        }
    }

    val currentFields = currentSchema.fields
      .map(HiveTable.makeSchemaString)
      .sorted
    val newFields = newSchema.fields
      .map(HiveTable.makeSchemaString)
      .sorted
    // Compare fields
    val addedFields = newFields.toSet.diff(currentFields.toSet)
    val removedFields = currentFields.toSet.diff(newFields.toSet)
    logger.info(s"Current: ${currentFields.length}")
    logger.debug(currentFields.mkString("\n"))
    logger.info(s"New    : ${newFields.length}")
    logger.debug(newFields.mkString("\n"))
    logger.info(s"Added  : ${addedFields.mkString("\n")}")
    logger.info(s"Removed: ${removedFields.mkString("\n")}")
    if (addedFields.nonEmpty || removedFields.nonEmpty) {
      logger.info(s"Incompatible fields")
      return false
    }

    true
  }

  /** Get partition column name
    */
  def getPartitionColumn: String = {
    // SHOW PARTITIONS output is like:
    // | partition |
    // |-----------|
    // | col1=val1 |
    // | col1=val2 |
    // | col2=val3 |
    val partition = spark
      .sql(s"SHOW PARTITIONS $name")
      .collect()
    !partition.isEmpty match {
      case true =>
        partition.head.toString
          .split("/")
          .map(x =>
            x.replaceAll("=.*$", "")
              .replace("[", "")
          )
          .mkString(",")
      case false =>
        ""
    }
  }

  /** Create hive table for a given DataFrame
    *
    * @param df
    *   the DataFrame
    */
  private def create(df: DataFrame): Unit = {
    logger.info(s"Creating table $name")
    val createTableString = HiveTable.makeCreateTableString(name, df.schema, partitionColumn, location)

    logger.info(createTableString)
    val res = spark.sql(createTableString).collect()
    logger.info(s"Table $name created: ${res.mkString(",")}")
  }

  /** Run MSCK REPAIR TABLE to make sure that the hive metadata is up to date with partition info
    */
  private def repair(): Unit = {
    logger.info(s"Repairing table $name")
    val res = spark.sql(s"MSCK REPAIR TABLE $name").collect()
    logger.info(s"Table $name repaired: ${res.mkString(",")}")
  }

  /** DROP the current table
    */
  private def drop(): Unit = {
    logger.info(s"Dropping table $name")
    val res = spark.sql(s"DROP TABLE $name").collect()
    logger.info(s"Table $name dropped: ${res.mkString(",")}")
  }

  /** REFRESH the current table
    */
  protected def refresh(): Unit = {
    Try { spark.catalog.refreshTable(name) } match {
      case Success(value) =>
        logger.info(s"Table $name is refreshed")
      case Failure(exception) =>
        logger.error(exception)
    }
  }
}

object HiveTable {

  /** Construct SQL compatible string for a field, with field name and type eg. some_column STRING
    *
    * @param field
    *   the field
    */
  private def makeSchemaString(field: StructField): String = {
    field.name
      .replaceAll("""^_""", "")
      .concat(" ")
      .concat(field.dataType.sql)
  }

  /** Construct CREATE TABLE string for a given DataFrame
    *
    * @param name
    *   the table name
    * @param schema
    *   the schema of a DataFrame
    * @param partition
    *   the partition column
    * @param location
    *   the location for parquet files
    */
  def makeCreateTableString(name: String,
                            schema: StructType,
                            partition: Option[Seq[String]],
                            location: Path
  ): String = {

    // Get all fields, excluding the partition
    var _schemaString: Array[StructField] = schema.fields
    partition match {
      case Some(hasPartition) =>
        // check whether partition columns are in the schema
        hasPartition.foreach(x => {
          if (!_schemaString.map(x => x.name).contains(x)) {
            throw new NoSuchElementException(s"Partition column: $x is not in the schema")
          }
        })
        val _partitionColumn: Array[StructField] = hasPartition
          .map(x => {
            schema.fields.filter(f => f.name == x)
          })
          .reduce(_ ++ _)
        hasPartition.foreach(x => {
          _schemaString = _schemaString.filter(f => f.name != x)
        })
        val schemaString = _schemaString
          .map(makeSchemaString)
          .mkString(",\n")

        val partitionString = _partitionColumn.map(x => makeSchemaString(x))

        s"""CREATE EXTERNAL TABLE $name ($schemaString)
           |PARTITIONED BY (${partitionString.mkString(", ")})
           |STORED AS PARQUET
           |LOCATION "${location.toString}"""".stripMargin.replaceAll("\n", " ")
      case None =>
        val schemaString = _schemaString
          .map(makeSchemaString)
          .mkString(",\n")
        s"""CREATE EXTERNAL TABLE $name ($schemaString)
           |STORED AS PARQUET
           |LOCATION "${location.toString}"""".stripMargin.replaceAll("\n", " ")
    }
  }
}
