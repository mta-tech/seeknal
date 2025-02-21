package tech.mta.seeknal.connector.serde

import org.apache.log4j.{Level, Logger, LogManager}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{coalesce, col, first, lit}
import org.apache.spark.sql.types.{StructField, StructType}
import tech.mta.seeknal.params.{Config, HasFeatureGroupSerDe}
import tech.mta.seeknal.transformers.{AddColumnByExpr, AddWindowFunction, DropCols, SQL, SelectColumns, StructAssembler}

class FeatureStoreSerDe extends BaseSerDe with HasFeatureGroupSerDe {

  val logger: Logger = LogManager.getLogger(this.getClass)
  logger.setLevel(Level.INFO)

  def setNullableStateForAllColumns(df: DataFrame, nullable: Boolean): DataFrame = {
    def set(st: StructType): StructType = {
      StructType(st.map {
        case StructField(name, dataType, _, metadata) =>
          val newDataType = dataType match {
            case t: StructType => set(t)
            case _ => dataType
          }
          StructField(name, newDataType, nullable = nullable, metadata)
      })
    }

    df.sqlContext.createDataFrame(df.rdd, set(df.schema))
  }
  override def transform(dataset: Dataset[_]): DataFrame = {
    if (getSerialize) {
      var data = dataset.toDF()
      val addFSMetadataCol = new SQL()
        .setStatement(s"SELECT *, '$getFeatureGroup' as $FEATURE_GROUP_NAME_COL," +
          s"'$getProject' as $PROJECT_COL, '$getEntity' as $ENTITY_COL FROM __THIS__")
      data = addFSMetadataCol.transform(dataset)
      data = setNullableStateForAllColumns(data, true)

      val createFSEventTime = new SQL()
        .setStatement(s"SELECT *, " +
          s"to_timestamp($getEventTimeCol, '$getDatePattern') as $FS_EVENT_TIME_COL FROM __THIS__")

      val dropSrcDateCol = new DropCols()
        .setInputCols(Array(getEventTimeCol))
      val preparedDataSteps = Array(createFSEventTime, dropSrcDateCol)

      val createFeatureValuesStruct = new StructAssembler()
        .setExcludeCols(getKeyCols ++ Array(FEATURE_GROUP_NAME_COL, FS_EVENT_TIME_COL, PROJECT_COL, ENTITY_COL))
        .setRemoveInputCols(true)
        .setOutputCol(FEATURE_GROUP_VALUE_COL)
      val serializeFeatureValues = new AvroSerDe()
        .setInputCol(FEATURE_GROUP_VALUE_COL)
        .setOutputCol(FEATURE_GROUP_VALUE_COL)
        .setTopicName(s"${getEntity}_${getProject}_${getFeatureGroup}")
        .setInferFromCol(true)
        .setSerialize(true)
        .setAvroType(Config.VANILLA_AVRO)

      if (isDefined(schemaRegistryURL)) {
        serializeFeatureValues.setSchemaRegistryUrl(getSchemaRegistryURL)
      } else if (isDefined(schemaValueString)) {
        serializeFeatureValues.setSchemaString(getSchemaValueString)
      } else {
        serializeFeatureValues.setSchemaRegistryUrl(Config.DUMMY_SCHEMA_REGISTRY_URL)
      }
      val serializeFeatureValuesSteps = Array(createFeatureValuesStruct, serializeFeatureValues)

      // creating record id
      val addExtraUnique = new AddWindowFunction()
        .setWindowFunction("row_number")
        .setPartitionCols(getKeyCols ++ Array(FEATURE_GROUP_NAME_COL, FS_EVENT_TIME_COL))
        .setOrderCols(getKeyCols)
        .setAscending(true)
        .setOutputCol(ROW_NUMBER_COL)

      val addStructId = new StructAssembler()
        .setInputCols(getKeyCols ++ Array(FEATURE_GROUP_NAME_COL, ROW_NUMBER_COL, FS_EVENT_TIME_COL))
        .setOutputCol(STRUCT_ID_COL)

      val createRecordId = new AddColumnByExpr()
        .setOutputCol(FS_ID_COL)
        .setExpression(s"hash($STRUCT_ID_COL)")

      val createRecordIdSteps = if (!data.columns.contains(FS_ID_COL)) {
        Array(addExtraUnique, addStructId, createRecordId)
      } else {
        Array()
      }

      // finalizing data so it can write into storage
      val finalizeSetCols = new SelectColumns()
        .setInputCols(Array(FS_ID_COL, PROJECT_COL, ENTITY_COL) ++ getKeyCols ++ Array(FEATURE_GROUP_VALUE_COL,
          FEATURE_GROUP_NAME_COL, FS_EVENT_TIME_COL))

      val finalizeSteps = Array(finalizeSetCols)

      // applying to the input data
      val offlineFSDF = new Pipeline()
        .setStages(preparedDataSteps ++ serializeFeatureValuesSteps ++  createRecordIdSteps ++ finalizeSteps)
        .fit(data)
        .transform(data)

      offlineFSDF
    } else {

      // deserialize method
      val commonKeys = getKeyCols
      var data = dataset.toDF()
        .filter(col(FEATURE_GROUP_NAME_COL).isin(getFeatureGroups.map(_.name): _*))

      if (isDefined(startEventTime)) {
        if (isDefined(endEventTime)) {
          data = data.filter(col(FS_EVENT_TIME_COL).between(getStartEventTime, getEndEventTime))
        } else {
          data = data.filter(col(FS_EVENT_TIME_COL) >= getStartEventTime)
        }
      } else if (isDefined(endEventTime)) {
        data = data.filter(col(FS_EVENT_TIME_COL) <= getEndEventTime)
      }

      var masterbase = data
          .groupBy(commonKeys :+ FS_EVENT_TIME_COL map col: _*)
          .pivot(FEATURE_GROUP_NAME_COL)
          .agg(coalesce(first(FEATURE_GROUP_VALUE_COL), lit(null)))

      val joinKeys = commonKeys :+ FS_EVENT_TIME_COL
      var implemetedCols = Set[String]()

      var nameAndFeatures: Array[(String, Array[String])] = Array[(String, Array[String])]()

      getFeatureGroups.zipWithIndex.foreach(x => {
        val avroSerde = new AvroSerDe()
          .setInputCol(x._1.name)
          .setOutputCol(Config.SERDE_TEMP_COL)
          .setSerialize(false)
          .setAvroType(Config.VANILLA_AVRO)

        if (x._1.schemaValueString.isDefined) {
          avroSerde.setSchemaString(x._1.schemaValueString.get)
        } else if (isDefined(schemaRegistryURL)) {
          avroSerde.setSchemaRegistryUrl(getSchemaRegistryURL)
        } else {
          throw new IllegalArgumentException("Schema registry url or schema value string must be defined")
        }

        if (x._1.schemaId.isDefined) {
          avroSerde.setSchemaId(x._1.schemaId.get)
        } else if (x._1.schemaVersion.isDefined) {
          avroSerde
            .setTopicName(s"${getEntity}_${getProject}_${x._1.name}")
            .setSchemaVersion(x._1.schemaVersion.get)
        } else {
          avroSerde
            .setTopicName(s"${getEntity}_${getProject}_${x._1.name}")
        }

        masterbase = avroSerde.transform(masterbase)

        val currentFeatureCols = masterbase.schema
          .filter(c => c.name == SERDE_TEMP_COL)
          .flatMap(_.dataType.asInstanceOf[StructType].fields)
          .map(_.name)
          .toBuffer
        nameAndFeatures = nameAndFeatures :+ (x._1.name, currentFeatureCols.toArray)

        val intersectCols = currentFeatureCols.toSet.intersect(implemetedCols)
        implemetedCols = currentFeatureCols.toSet ++ implemetedCols

        // handle duplicate feature columns
        // check whether current feature sets have same name with previous feature sets
        // if duplicate found, it will rename the intersect feature columns with prefix of feature group name
        if (x._2 > 0) {

          if (intersectCols.nonEmpty) {
            val intersectFgAndCol = intersectCols.map(y => {
              val name = nameAndFeatures.filter(x => x._2.contains(y)).head._1
              (name, y)
            })
            logger.info("Duplicate columns are found, will rename intersect columns with prefix feature group name")
            intersectFgAndCol.foreach(y => masterbase = masterbase.withColumnRenamed(y._2, s"${y._1}_${y._2}"))
          }
        }
        // if exclude features are defined then will remove particular features on the set
        if (x._1.excludeFeatures.isDefined) {
          for (i <- x._1.excludeFeatures.get) {
            currentFeatureCols -= i
          }
        }

        // populate feature values into columns
        val currentFeatureColStr = currentFeatureCols.map(k => s"${SERDE_TEMP_COL}.$k").mkString(",")
        val populateCols = new SQL()
          .setStatement(s"SELECT *, ${currentFeatureColStr} FROM __THIS__")

        val dropTempCols = new DropCols()
          .setInputCols(Array(SERDE_TEMP_COL, x._1.name))

        val pipeline = Array(populateCols, dropTempCols)
        masterbase = new Pipeline().setStages(pipeline).fit(masterbase).transform(masterbase)

        if (intersectCols.nonEmpty) {
          intersectCols.foreach(y => masterbase = masterbase.withColumnRenamed(y, s"${x._1.name}_$y"))
        }
      })

      // if `features` are set then will select features accordingly
      val features = getFeatureGroups.foldLeft(Seq[String]())(op = (list, feat) => {
        feat.features match {
          case Some(feat) =>
            list ++ feat
          case None =>
            list
        }
      })

      if (features.nonEmpty) {
        masterbase = new SelectColumns()
          .setInputCols(joinKeys ++ features)
          .transform(masterbase)
      }

      // handle null values with specified `fillNull` parameters
      if (isDefined(fillNull)) {
        getFillNull.foreach(k => {
          val columnsToApply = k.columns match {
            case Some(cols) =>
              cols.toBuffer
            case None =>
              masterbase.columns.toBuffer --= joinKeys -= FS_EVENT_TIME_COL
          }
          k.dataType match {
            case "double" =>
              masterbase = masterbase.na.fill(k.value.toDouble, columnsToApply)
            case "int" =>
              masterbase = masterbase.na.fill(k.value.toInt, columnsToApply)
            case "long" =>
              masterbase = masterbase.na.fill(k.value.toLong, columnsToApply)
            case "string" =>
              masterbase = masterbase.na.fill(k.value, columnsToApply)
          }
        })

      }

      // if dropEventTime is true then will remove event_time column
      if (getDropEventTime) {
        masterbase = masterbase.drop(FS_EVENT_TIME_COL)
      }
      masterbase
    }
  }
}
