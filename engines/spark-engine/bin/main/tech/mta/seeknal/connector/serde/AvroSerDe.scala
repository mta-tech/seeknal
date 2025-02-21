package tech.mta.seeknal.connector.serde

import org.apache.avro.Schema
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.col
import scala.collection.JavaConverters._
import za.co.absa.abris.avro.functions.{from_avro, to_avro}
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.abris.avro.read.confluent.SchemaManagerFactory
import za.co.absa.abris.avro.registry.{ConfluentMockRegistryClient, NumVersion, SchemaSubject}
import za.co.absa.abris.config.{AbrisConfig, FromAvroConfig, FromSchemaDownloadingConfigFragment, FromStrategyConfigFragment, ToAvroConfig, ToConfluentAvroRegistrationStrategyConfigFragment, ToSchemaDownloadingConfigFragment, ToSchemaRegisteringConfigFragment, ToStrategyConfigFragment}
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
import tech.mta.seeknal.params.{Config, HasAvroSerDeConfig}


/**
  * SerDe class for working with avro column
  * Need avro version >= 1.8
  */
class AvroSerDe extends BaseSerDe with HasAvroSerDeConfig {

  var schemaRegistry = ""
  val mockSchemaRegistryConfig = Map(AbrisConfig.SCHEMA_REGISTRY_URL -> Config.SCHEMA_REGISTRY_URL,
    AbrisConfig.REGISTRY_CLIENT_CLASS -> "za.co.absa.abris.avro.registry.ConfluentMockRegistryClient")
  val logger: Logger = LogManager.getLogger(this.getClass)
  logger.setLevel(Level.INFO)

  /**
    * mocking schema registry
    *
    * @param schema
    * @param topic
    * @return
    */
  def mockingSchemaRegistry(schema: Schema,
                            topic: String): Map[String, String] = {
    val schemaSubject = new SchemaSubject(topic)
    val mockSchemaRegistryClient = SchemaManagerFactory.create(mockSchemaRegistryConfig)
    val returnSchemaId = mockSchemaRegistryClient.register(new SchemaSubject(topic), schema)
    Map(
      "topic" -> topic,
      "id" -> returnSchemaId.toString,
      "version" -> mockSchemaRegistryClient
        .getAllSchemasWithMetadata(schemaSubject).last.getVersion.toString
    )
  }

  /**
    * confluent avro deserialize method
    *
    * @param spark
    * @return
    */
  def confluentAvroDeserialize(spark: SparkSession): FromAvroConfig = {

    val fromConfluent = AbrisConfig.fromConfluentAvro
    isDefined(schemaFile) || isDefined(schemaString) match {
      case true =>
        val schemaFileLoaded: String = if (isDefined(schemaFile)) {
          schemaReader(spark)
        } else {
          getSchemaString
        }

        if (!isDefined(schemaRegistryUrl)) {
          val parser = new Schema.Parser()
          val schema = parser.parse(schemaFileLoaded)
          val registeredSchema = mockingSchemaRegistry(schema, getTopicName)
          fromConfluent
            .provideReaderSchema(schemaFileLoaded)
            .usingSchemaRegistry(mockSchemaRegistryConfig)
        } else {
        fromConfluent
          .provideReaderSchema(schemaFileLoaded)
          .usingSchemaRegistry(getSchemaRegistryUrl)
        }

      case false =>
        schemaRegistry = getSchemaRegistryUrl
        if (isDefined(schemaId)) {
          if (schemaRegistry.startsWith("mock://")) {
            fromConfluent
              .downloadReaderSchemaById(getSchemaId.toInt)
              .usingSchemaRegistry(
                Map(AbrisConfig.SCHEMA_REGISTRY_URL -> schemaRegistry,
                  AbrisConfig.REGISTRY_CLIENT_CLASS -> "za.co.absa.abris.avro.registry.ConfluentMockRegistryClient")
              )
          } else {
            fromConfluent
              .downloadReaderSchemaById(getSchemaId.toInt)
              .usingSchemaRegistry(schemaRegistry)
          }
        } else if (isDefined(schemaVersion)) {
          val fromStrategyConfig = fromConfluent
            .downloadReaderSchemaByVersion(getSchemaVersion.toInt)
          val getStrategy = confluentGetStrategy(fromStrategyConfig)
          useMockOrRealSchemaRegistryFrom(schemaRegistry, getStrategy)
        } else {
          val fromStrategyConfig =
            fromConfluent.downloadReaderSchemaByLatestVersion
          val getStrategy = confluentGetStrategy(fromStrategyConfig)
          useMockOrRealSchemaRegistryFrom(schemaRegistry, getStrategy)
        }
    }
  }

  def useMockOrRealSchemaRegistryFrom(schemaRegistry: String, getStrategy: FromSchemaDownloadingConfigFragment):
    FromAvroConfig = {
    if (schemaRegistry.startsWith("mock://")) {
      AbrisConfig
        .fromConfluentAvro
        .downloadReaderSchemaById(1)
        .usingSchemaRegistry(
          Map(AbrisConfig.SCHEMA_REGISTRY_URL -> schemaRegistry,
            AbrisConfig.REGISTRY_CLIENT_CLASS -> "za.co.absa.abris.avro.registry.ConfluentMockRegistryClient")
        )
    } else {
      getStrategy.usingSchemaRegistry(schemaRegistry)
    }
  }

 def useMockOrRealSchemaRegistryTo(schemaRegistry: String, getStrategy: ToSchemaDownloadingConfigFragment):
    ToAvroConfig = {
      if (schemaRegistry.startsWith("mock://")) {
        getStrategy
          .usingSchemaRegistry(
            Map(AbrisConfig.SCHEMA_REGISTRY_URL -> schemaRegistry,
              AbrisConfig.REGISTRY_CLIENT_CLASS -> "za.co.absa.abris.avro.registry.ConfluentMockRegistryClient")
          )
      } else {
        getStrategy.usingSchemaRegistry(schemaRegistry)
      }
 }

  /**
    * deserialize strategy
    *
    * @param strategyConfig
    * @return
    */
  def confluentGetStrategy(strategyConfig: FromStrategyConfigFragment)
    : FromSchemaDownloadingConfigFragment = {

    if (isDefined(strategyMethod)) {
      getStrategyMethod match {
        case Config.TOPIC_NAME_STRATEGY =>
          defaultConfluentGetStrategy(strategyConfig)
        case Config.RECORD_NAME_STRATEGY =>
          val recordName = getRecordName.split(",")
          strategyConfig
            .andRecordNameStrategy(recordName(0), recordName(1))
        case Config.TOPIC_RECORD_NAME_STRATEGY =>
          val topicRecordName = getRecordName.split(",")
          strategyConfig
            .andTopicRecordNameStrategy(getTopicName,
                                        topicRecordName(0),
                                        topicRecordName(1))
        case _ =>
          defaultConfluentGetStrategy(strategyConfig)
      }
    } else {
      defaultConfluentGetStrategy(strategyConfig)
    }
  }

  /**
    * serialize strategy with specified schema version
    *
    * @param strategyConfig
    * @return
    */
  def confluentVersionToStrategy(strategyConfig: ToStrategyConfigFragment)
    : ToSchemaDownloadingConfigFragment = {

    if (isDefined(strategyMethod)) {
      getStrategyMethod match {
        case Config.TOPIC_NAME_STRATEGY =>
          defaultConfluentVersionToStrategy(strategyConfig)
        case Config.RECORD_NAME_STRATEGY =>
          val recordName = getRecordName.split(",")
          strategyConfig
            .andRecordNameStrategy(recordName(0), recordName(1))
        case Config.TOPIC_RECORD_NAME_STRATEGY =>
          val topicRecordName = getRecordName.split(",")
          strategyConfig
            .andTopicRecordNameStrategy(getTopicName,
                                        topicRecordName(0),
                                        topicRecordName(1))
        case _ =>
          defaultConfluentVersionToStrategy(strategyConfig)
      }
    } else {
      defaultConfluentVersionToStrategy(strategyConfig)
    }
  }

  /**
    * Fill default value given a schema.
    * This requires due to infer avro schema from dataframe cannot set default value
    *
    * @param schema
    * @param defaultValue
    * @return
    */
  def fillFieldDefault(schema: Schema, defaultValue: String = "null"): String = {

    val defaultValueStr = defaultValue match {
      case "null" =>
        """null"""
      case _ =>
        defaultValue
    }
    // @TODO: bad hack for adding default value
    val fieldStrArr = schema.getFields.asScala.map(x => {
      val headField = x.schema.toString().split(",").head.replace("[", "")
      s"""{"name": "${x.name()}","type": ["null", ${headField}],
         |"default": ${defaultValueStr}}""".stripMargin
    }).mkString(",")

      s"""{"type": "${schema.getType.getName}", "name": "${schema.getName}",
         |"fields": [${fieldStrArr}]}""".stripMargin
  }

  /**
    * Given schema, register schema to schema registry
    *
    * @param registryURL schema registry url
    * @param schema schema string
    * @return toAvroConfig
    */
  def provideAndRegisterSchema(registryURL: String, schema: String): ToAvroConfig = {

    val registrationStrategy = AbrisConfig.toConfluentAvro
      .provideAndRegisterSchema(schema)

    confluentToStrategy(registrationStrategy)
      .usingSchemaRegistry(registryURL)
  }

  /**
    * confluent avro serialize method
    *
    * @param spark
    * @return
    */
  def confluentAvroSerialize(inputDF: DataFrame): ToAvroConfig = {
    val toConfluent = AbrisConfig.toConfluentAvro
    val spark = inputDF.sparkSession

    (isDefined(schemaFile) || isDefined(schemaString)) match {
      case true =>
        val schemaFileLoaded = if (isDefined(schemaFile)) {
          schemaReader(spark)
        } else {
          getSchemaString
        }

        if (!isDefined(schemaRegistryUrl)) {
          val parser = new Schema.Parser()
          val schema = parser.parse(schemaFileLoaded)
          val registeredSchema = mockingSchemaRegistry(schema, getTopicName)
          toConfluent
            .provideAndRegisterSchema(schemaFileLoaded)
            .usingTopicNameStrategy(getTopicName)
            .usingSchemaRegistry(mockSchemaRegistryConfig)
        } else {
          provideAndRegisterSchema(getSchemaRegistryUrl, schemaFileLoaded)
        }

      case false =>
        schemaRegistry = getSchemaRegistryUrl

        getInferFromCol match {
          case true =>
            val schemaFromDF = AvroSchemaUtils.toAvroSchema(inputDF.select(s"$getInputCol.*"))
            logger.info("Avro schema inferred from dataframe: " + schemaFromDF.toString)
            if (isDefined(schemaRegistryUrl)) {
              provideAndRegisterSchema(schemaRegistry, schemaFromDF.toString())
            } else {
              val registeredSchema = mockingSchemaRegistry(schemaFromDF, getTopicName)
              toConfluent
                .provideAndRegisterSchema(schemaFromDF.toString())
                .usingTopicNameStrategy(getTopicName)
                .usingSchemaRegistry(mockSchemaRegistryConfig)
            }


          case false =>
            if (isDefined(schemaId)) {
              if (schemaRegistry.startsWith("mock://")) {
                toConfluent
                  .downloadSchemaById(getSchemaId.toInt)
                  .usingSchemaRegistry(
                    Map(AbrisConfig.SCHEMA_REGISTRY_URL -> schemaRegistry,
                      AbrisConfig.REGISTRY_CLIENT_CLASS -> "za.co.absa.abris.avro.registry.ConfluentMockRegistryClient")
                  )
              } else {
                toConfluent
                  .downloadSchemaById(getSchemaId.toInt)
                  .usingSchemaRegistry(schemaRegistry)
              }
            } else if (isDefined(schemaVersion)) {
              val fragment = toConfluent
                .downloadSchemaByVersion(getSchemaVersion.toInt)
              useMockOrRealSchemaRegistryTo(schemaRegistry, confluentVersionToStrategy(fragment))
            } else {
              val fragment = toConfluent
                .downloadSchemaByLatestVersion
              useMockOrRealSchemaRegistryTo(schemaRegistry, confluentVersionToStrategy(fragment))
            }
        }
    }
  }

  /**
    * strategy for serialize
    *
    * @param strategyConfig
    * @return
    */
  def confluentToStrategy(
      strategyConfig: ToConfluentAvroRegistrationStrategyConfigFragment)
    : ToSchemaRegisteringConfigFragment = {
    if (isDefined(strategyMethod)) {
      getStrategyMethod match {
        case Config.TOPIC_NAME_STRATEGY =>
          defaultConfluentToStrategy(strategyConfig)
        case Config.RECORD_NAME_STRATEGY =>
          strategyConfig
            .usingRecordNameStrategy()
        case Config.TOPIC_RECORD_NAME_STRATEGY =>
          strategyConfig
            .usingTopicRecordNameStrategy(getTopicName)
        case _ =>
          defaultConfluentToStrategy(strategyConfig)
      }
    } else {
      defaultConfluentToStrategy(strategyConfig)
    }
  }

  /**
    * default strategy for deserialize
    *
    * @param strategyConfig
    * @return
    */
  def defaultConfluentGetStrategy(strategyConfig: FromStrategyConfigFragment)
    : FromSchemaDownloadingConfigFragment = {
    strategyConfig
      .andTopicNameStrategy(getTopicName, getIsKey)
  }

  /**
    * default strategy for serialize
    *
    * @param strategyConfig
    * @return
    */
  def defaultConfluentToStrategy(
      strategyConfig: ToConfluentAvroRegistrationStrategyConfigFragment)
    : ToSchemaRegisteringConfigFragment = {
    strategyConfig
      .usingTopicNameStrategy(getTopicName, getIsKey)
  }

  /**
    * default strategy for serialize with specified schema version
    *
    * @param strategyConfig
    * @return
    */
  def defaultConfluentVersionToStrategy(
      strategyConfig: ToStrategyConfigFragment)
    : ToSchemaDownloadingConfigFragment = {
    strategyConfig
      .andTopicNameStrategy(getTopicName, getIsKey)
  }

  /**
    * deserialize method for simpleAvro with specified schema registry
    *
    * @return
    */
  def simpleAvroDeserializeSchemaRegistry(): FromAvroConfig = {

    val fromSimple = AbrisConfig.fromSimpleAvro
    val schemaManager = if (getSchemaRegistryUrl.startsWith("mock://")) {
      SchemaManagerFactory.create(Map(AbrisConfig.SCHEMA_REGISTRY_URL -> getSchemaRegistryUrl,
        AbrisConfig.REGISTRY_CLIENT_CLASS -> "za.co.absa.abris.avro.registry.ConfluentMockRegistryClient"))
    } else {
      SchemaManagerFactory.create(Map(AbrisConfig.SCHEMA_REGISTRY_URL -> getSchemaRegistryUrl))
    }
    val schema: String = if (isDefined(schemaId)) {
      schemaManager.getSchemaById(getSchemaId.toInt).toString
    } else if (isDefined(schemaVersion)) {
      schemaManager.getSchemaBySubjectAndVersion(new SchemaSubject(getTopicName),
        NumVersion(getSchemaVersion.toInt)).toString
    } else {
      val schemas = schemaManager.getAllSchemasWithMetadata(new SchemaSubject(getTopicName))
      if (schemas.nonEmpty) {
        schemas.last.getSchema
      } else {
        throw new RestClientException("Schema not found", 404, 40403)
      }
    }
    fromSimple.provideSchema(schema)
  }

  /**
    * serialize method for simpleAvro with specified schema registry
    *
    * @return
    */
  def simpleAvroSerializeSchemaRegistry(): ToAvroConfig = {

    val toSimple = AbrisConfig.toSimpleAvro
    if (isDefined(schemaId)) {
        toSimple
          .downloadSchemaById(getSchemaId.toInt)
          .usingSchemaRegistry(getSchemaRegistryUrl)
    } else if (isDefined(schemaVersion)) {
      val fragment = toSimple
        .downloadSchemaByVersion(getSchemaVersion.toInt)

      confluentVersionToStrategy(fragment)
        .usingSchemaRegistry(getSchemaRegistryUrl)
    } else {
      val fragment = toSimple.downloadSchemaByLatestVersion

      confluentVersionToStrategy(fragment)
        .usingSchemaRegistry(getSchemaRegistryUrl)
    }
  }

  /**
    * Serialze/deserialize the dataset with given variables
    *
    * @param dataset
    * @return
    */
  override def transform(dataset: Dataset[_]): DataFrame = {

    getAvroType match {
      case Config.CONFLUENT_AVRO =>
        if (getSerialize) {
          val toAvroConfig: ToAvroConfig = confluentAvroSerialize(
            dataset.toDF()
          )
          dataset
            .withColumn(getOutputCol,
                        to_avro(col(getInputCol), toAvroConfig))

        } else {
          val fromAvroConfig: FromAvroConfig = confluentAvroDeserialize(
            dataset.sparkSession)
          dataset
            .withColumn(getOutputCol,
                        from_avro(col(getInputCol), fromAvroConfig))
        }
      case Config.VANILLA_AVRO =>
        if (getSerialize) {
          if (isDefined(schemaFile) || isDefined(schemaString) || getInferFromCol) {
            val schemaFileString = if (isDefined(schemaFile)) {
              schemaReader(dataset.sparkSession)
            } else if (isDefined(schemaString)) {
              getSchemaString
            } else if (getInferFromCol) {
              val schemaFromDF = AvroSchemaUtils.toAvroSchema(dataset.select(s"$getInputCol.*"))
              logger.info("Avro schema inferred from dataframe: " + schemaFromDF.toString)
              schemaFromDF.toString
            } else {
              throw new IllegalArgumentException("Cannot find schema")
            }
            if (isDefined(schemaRegistryUrl)) {
              SchemaManagerFactory.create(Map(AbrisConfig.SCHEMA_REGISTRY_URL -> getSchemaRegistryUrl))
                .register(new SchemaSubject(getTopicName), schemaFileString)
            }
            dataset
              .withColumn(getOutputCol,
                to_avro(col(getInputCol), schemaFileString))
          } else {
            val toAvroConfig: ToAvroConfig = simpleAvroSerializeSchemaRegistry()
            dataset
              .withColumn(getOutputCol,
                to_avro(col(getInputCol), toAvroConfig))
          }
        } else {
          if (isDefined(schemaFile) || isDefined(schemaString) || getInferFromCol) {
            val schemaFileString = if (isDefined(schemaFile)) {
              schemaReader(dataset.sparkSession)
            } else if (isDefined(schemaString)) {
              getSchemaString
            } else if (getInferFromCol) {
              val schemaFromDF = AvroSchemaUtils.toAvroSchema(dataset.select(s"$getInputCol.*"))
              logger.info("Avro schema inferred from dataframe: " + schemaFromDF.toString)
              schemaFromDF.toString
            } else {
              throw new IllegalArgumentException("Cannot find schema")
            }

            dataset
              .withColumn(getOutputCol,
                from_avro(col(getInputCol), schemaFileString))
          } else {
            val fromAvroConfig: FromAvroConfig = simpleAvroDeserializeSchemaRegistry()
            dataset
              .withColumn(getOutputCol,
                from_avro(col(getInputCol), fromAvroConfig))
          }
        }
      case _ =>
        throw new NotImplementedError("Avro type is not supported")
    }
  }
}
