package tech.mta.seeknal.params

import java.time.DayOfWeek

import scala.collection.JavaConverters._

import org.apache.spark.ml.param._
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.{compact, parse}
import org.json4s.jackson.Serialization.write

object ParamValidators {
  def alwaysTrue[T]: T => Boolean = (_: T) => true

  // checks if a given string is a valid day name
  def validateDayOfWeek(day: String): Boolean = {
    try {
      DayOfWeek.valueOf(day)
      true
    } catch {
      case _: Exception => false
    }
  }

  def validateDayOfWeekArray(day: Array[String]): Boolean = {
    day.forall(validateDayOfWeek)
  }
}

trait HasInputCols extends Params {
  final val inputCols: StringArrayParam =
    new StringArrayParam(this, "inputCols", "input column names")
  final def getInputCols: Array[String] = $(inputCols)
  final def setInputCols(value: Array[String]): this.type =
    set(inputCols, value)

  final val removeInputCols: BooleanParam = new BooleanParam(
    this,
    "removeInputCol",
    "If set to true, will also remove input column")
  final def getRemoveInputCols: Boolean = $(removeInputCols)
  final def setRemoveInputCols(value: Boolean): this.type =
    set(removeInputCols, value)
  setDefault(removeInputCols, false)
}

trait HasInputCol extends Params {
  final val inputCol: Param[String] =
    new Param[String](this, "inputCol", "input column name")
  final def getInputCol: String = $(inputCol)
  final def setInputCol(value: String): this.type = set(inputCol, value)

  final val removeInputCol: BooleanParam = new BooleanParam(
    this,
    "removeInputCol",
    "If set to true, will also remove input column")
  final def getRemoveInputCol: Boolean = $(removeInputCol)
  final def setRemoveInputCol(value: Boolean): this.type =
    set(removeInputCol, value)
  setDefault(removeInputCol, false)
}

trait HasOutputCol extends Params {
  final val outputCol: Param[String] =
    new Param[String](this, "outputCol", "output column name")
  final def getOutputCol: String = $(outputCol)
  final def setOutputCol(value: String): this.type = set(outputCol, value)
}

trait HasInputPattern extends Params {
  final val inputPattern: Param[String] =
    new Param[String](this, "inputPattern", "the pattern of the input date")
  final def getInputPattern: String = $(inputPattern)
  final def setInputPattern(value: String): this.type = set(inputPattern, value)
}

trait HasOutputPattern extends Params {
  final val outputPattern: Param[String] =
    new Param[String](this, "outputPattern", "the pattern of the output date")
  final def getOutputPattern: String = $(outputPattern)
  final def setOutputPattern(value: String): this.type =
    set(outputPattern, value)
}

trait HasNullFilter extends Params {
  final val nullFilter: BooleanParam = new BooleanParam(
    this,
    "nullFilter",
    "true if filter null rows in transformer")
  final def getNullFilter: Boolean = $(nullFilter)
  final def setNullFilter(value: Boolean): this.type = set(nullFilter, value)
  setDefault(nullFilter, true)
}

trait HasRenamedCols extends Params {
  final val renamedCols: RenameColumnArrayParam =
    new RenameColumnArrayParam(this, "renamedCols", "columns to be renamed")
  final def getRenamedCols: Array[RenameColumn] = $(renamedCols)
  final def setRenamedCols(value: Array[RenameColumn]): this.type =
    set(renamedCols, value)
}

case class RenameColumn(name: String, newName: String)

class RenameColumnArrayParam(parent: Params,
                             name: String,
                             doc: String,
                             isValid: Array[RenameColumn] => Boolean)
    extends Param[Array[RenameColumn]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  /** Creates a param pair with a `java.util.List` of values (for Java and Python). */
  def w(value: java.util.List[RenameColumn]): ParamPair[Array[RenameColumn]] =
    w(value.asScala.toArray)

  override def jsonEncode(value: Array[RenameColumn]): String = {
    import org.json4s.JsonDSL._
    implicit val formats: DefaultFormats = DefaultFormats
    compact(write(value.toSeq))
  }

  override def jsonDecode(json: String): Array[RenameColumn] = {
    implicit val formats: DefaultFormats = DefaultFormats
    parse(json).extract[Seq[RenameColumn]].toArray
  }
}

trait HasValueMappings extends Params {
  final val valueMappings: ValueMappingArrayParam = new ValueMappingArrayParam(
    this,
    "valueMappings",
    "list of mappings of one value to another value")
  final def getValueMappings: Array[ValueMapping] = $(valueMappings)
  final def setValueMappings(value: Array[ValueMapping]): this.type =
    set(valueMappings, value)
}

case class ValueMapping(fromValue: String, toValue: String)

class ValueMappingArrayParam(parent: Params,
                             name: String,
                             doc: String,
                             isValid: Array[ValueMapping] => Boolean)
    extends Param[Array[ValueMapping]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  /** Creates a param pair with a `java.util.List` of values (for Java and Python). */
  def w(value: java.util.List[ValueMapping]): ParamPair[Array[ValueMapping]] =
    w(value.asScala.toArray)

  override def jsonEncode(value: Array[ValueMapping]): String = {
    import org.json4s.JsonDSL._
    implicit val formats: DefaultFormats = DefaultFormats
    compact(write(value.toSeq))
  }

  override def jsonDecode(json: String): Array[ValueMapping] = {
    implicit val formats: DefaultFormats = DefaultFormats
    parse(json).extract[Seq[ValueMapping]].toArray
  }
}

trait HasColsByExpression extends Params {
  final val colsByExpression: ColumnExpressionArrayParam =
    new ColumnExpressionArrayParam(
      this,
      "colsByExpression",
      "columns to be added as specified by the given expressions")
  final def getColsByExpression: Array[ColumnExpression] = $(colsByExpression)
  final def setColsByExpression(value: Array[ColumnExpression]): this.type =
    set(colsByExpression, value)

  final val expression: Param[String] =
    new Param[String](this, "expression", "spark sql expression")
  final def getExpression: String = $(expression)
  final def setExpression(value: String): this.type = set(expression, value)
}

case class ColumnExpression(newName: String, expression: String)

class ColumnExpressionArrayParam(parent: Params,
                                 name: String,
                                 doc: String,
                                 isValid: Array[ColumnExpression] => Boolean)
    extends Param[Array[ColumnExpression]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  /** Creates a param pair with a `java.util.List` of values (for Java and Python). */
  def w(value: java.util.List[ColumnExpression])
    : ParamPair[Array[ColumnExpression]] =
    w(value.asScala.toArray)

  override def jsonEncode(value: Array[ColumnExpression]): String = {
    import org.json4s.JsonDSL._
    implicit val formats: DefaultFormats = DefaultFormats
    compact(write(value.toSeq))
  }

  override def jsonDecode(json: String): Array[ColumnExpression] = {
    implicit val formats: DefaultFormats = DefaultFormats
    parse(json).extract[Seq[ColumnExpression]].toArray
  }
}

class DayOfWeekParam(parent: Params,
                     name: String,
                     doc: String,
                     isValid: String => Boolean)
    extends Param[String](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.validateDayOfWeek)
}

class DayOfWeekArrayParam(parent: Params,
                          name: String,
                          doc: String,
                          isValid: Array[String] => Boolean)
    extends StringArrayParam(parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.validateDayOfWeekArray)
}

trait HasIdCol extends Params {
  final val idCol: Param[String] =
    new Param[String](this, "idCol", "id column name")
  final def getIdCol: String = $(idCol)
  final def setIdCol(value: String): this.type = set(idCol, value)
}

trait HasEntityCol extends Params {
  final val entityCol: Param[String] =
    new Param[String](this, "entityCol", "entity column name")
  final def getEntityCol: String = $(entityCol)
  final def setEntityCol(value: String): this.type = set(entityCol, value)
}

trait HasLocationCol extends Params {
  final val refNdbTable: Param[String] = new Param[String](
    this,
    "refNdbTable",
    "Reference cellsite table, containing lat / long for cell towers")
  final def setRefNdbTable(value: String): this.type = set(refNdbTable, value)
  final def getRefNdbTable: String = $(refNdbTable)

  final val latitudeCol: Param[String] =
    new Param[String](this, "latitudeCol", "latitude column for cell tower")
  final def setLatitudeCol(value: String): this.type = set(latitudeCol, value)
  final def getLatitudeCol: String = $(latitudeCol)
  setDefault(latitudeCol, "lat")

  final val longitudeCol: Param[String] =
    new Param[String](this, "longitudeCol", "longitude column for cell tower")
  final def setLongitudeCol(value: String): this.type = set(longitudeCol, value)
  final def getLongitudeCol: String = $(longitudeCol)
  setDefault(longitudeCol, "long")

  final val decimalOffset: Param[String] = new Param[String](
    this,
    "decimalOffset",
    "Number of digits on right side of dot")
  final def setDecimalOffset(value: String): this.type =
    set(decimalOffset, value)
  final def getDecimalOffset: String = $(decimalOffset)
  setDefault(decimalOffset, "2")
}

trait HasDateCol extends Params {
  final val dateCol: Param[String] =
    new Param[String](this, "dateCol", "date column name")
  final def getDateCol: String = $(dateCol)
  final def setDateCol(value: String): this.type = set(dateCol, value)

  final val monthCol: Param[String] =
    new Param[String](this, "monthCol", "month column name")
  final def getMonthCol: String = $(monthCol)
  final def setMonthCol(value: String): this.type = set(monthCol, value)

  final val inputDatePattern: Param[String] =
    new Param[String](this, "inputDatePattern", "date pattern in dateCol")
  final def getInputDatePattern: String = $(inputDatePattern)
  final def setInputDatePattern(value: String): this.type =
    set(inputDatePattern, value)
  setDefault(inputDatePattern, "yyyyMMdd")

  final val inputMonthPattern: Param[String] =
    new Param[String](this, "inputMonthPattern", "month pattern in dateCol")
  final def getInputMonthPattern: String = $(inputMonthPattern)
  final def setInputMonthPattern(value: String): this.type =
    set(inputMonthPattern, value)
  setDefault(inputMonthPattern, "yyyyMM")

  final val outputDatePattern: Param[String] = new Param[String](
    this,
    "outputDatePattern",
    "desired date pattern after formatting")
  final def getOutputDatePattern: String = $(outputDatePattern)
  final def setOutputDatePattern(value: String): this.type =
    set(outputDatePattern, value)
  setDefault(outputDatePattern, "yyyy-MM-dd")

  final val outputMonthPattern: Param[String] = new Param[String](
    this,
    "outputMonthPattern",
    "desired date pattern after formatting")
  final def getOutputMonthPattern: String = $(outputMonthPattern)
  final def setOutputMonthPattern(value: String): this.type =
    set(outputMonthPattern, value)
  setDefault(outputMonthPattern, "yyyy-MM-01")

  final val weekends: DayOfWeekArrayParam =
    new DayOfWeekArrayParam(this,
                            "weekends",
                            "List of weekends (in capital letters)")
  final def getWeekends: Array[String] = $(weekends)
  final def setWeekends(value: Array[String]): this.type = set(weekends, value)
  setDefault(weekends, Array("SATURDAY", "SUNDAY"))
}

trait HasH3Col extends Params {
  final val resolution: IntParam = new IntParam(
    this,
    "resolution",
    "H3 resolution"
  )

  final def getResolution: Int = $(resolution)
  final def setResolution(value: Int): this.type = set(resolution, value)

  final val resolutionString: Param[String] = new Param[String](
    this,
    "resolutionString",
    "H3 resolution"
  )

  final def getResolutionString: String = $(resolutionString)
  final def setResolutionString(value: String): this.type =
    set(resolutionString, value)

  final val firstIndex: IntParam = new IntParam(
    this,
    "firstIndex",
    "first index of chosen H3 resolution"
  )
  final def getFirstIndex: Int = $ { firstIndex }
  final def setFirstIndex(value: Int): this.type = set(firstIndex, value)
  setDefault(firstIndex, 0)

  final val lastIndex: IntParam = new IntParam(
    this,
    "lastIndex",
    "last index of chosen H3 resolution"
  )
  final def getLastIndex: Int = $ { lastIndex }
  final def setLastIndex(value: Int): this.type = set(lastIndex, value)
  setDefault(lastIndex, 14)

  final val kRing: IntParam = new IntParam(
    this,
    "kRing",
    "k-rings produces indices within k distance of the origin index"
  )
  final def getKRing: Int = $(kRing)
  final def setKRing(value: Int): this.type = set(kRing, value)

  final val h3AddressCol: Param[String] = new Param[String](
    this,
    "h3AddressCol",
    "column indicate H3 index address"
  )

  final def getH3AddressCol: String = $(h3AddressCol)
  final def setH3AddressCol(value: String): this.type = set(h3AddressCol, value)

  final val parentResolution: IntParam = new IntParam(
    this,
    "parentResolution",
    "target parent (coarser) resolution"
  )

  final def getParentResolution: Int = $(parentResolution)
  final def setParentResolution(value: Int): this.type = set(parentResolution, value)
}

case class GeoCoord(lat: Double, lon: Double)

trait HasSerDeConfig extends Params {
  final val schemaFile: SchemaFileParam = new SchemaFileParam(
    this,
    "schemaFile",
    "schema file path"
  )
  final def getSchemaFile: SchemaFile = $ { schemaFile }
  final def setSchemaFile(value: SchemaFile): this.type = set(schemaFile, value)

  final val schemaString: Param[String] = new Param[String](
    this,
    "schemaString",
    "Avro schema as string"
  )
  final def getSchemaString: String = $ { schemaString }
  final def setSchemaString(value: String): this.type = set(schemaString, value)

  final val inferFromCol: BooleanParam = new BooleanParam(
    this,
    "inferFromCol",
    "is infer schema from input col"
  )
  final def getInferFromCol: Boolean = $ { inferFromCol }
  final def setInferFromCol(value: Boolean): this.type = set(inferFromCol, value)
  setDefault(inferFromCol, false)

  final val serialize: BooleanParam = new BooleanParam(
    this,
    "serialize",
    "is serialize input col"
  )
  final def getSerialize: Boolean = $ { serialize }
  final def setSerialize(value: Boolean): this.type = set(serialize, value)
  setDefault(serialize, false)

}

case class SchemaFile(path: String, store: String)

class SchemaFileParam(parent: Params,
                      name: String,
                      doc: String,
                      isValid: SchemaFile => Boolean)
    extends Param[SchemaFile](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  /** Creates a param pair with a `java.util.List` of values (for Java and Python). */
  override def w(value: SchemaFile): ParamPair[SchemaFile] = w(value)

  override def jsonEncode(value: SchemaFile): String = {
    import org.json4s.JsonDSL._
    implicit val formats: DefaultFormats = DefaultFormats
    compact(write(value))
  }

  override def jsonDecode(json: String): SchemaFile = {
    implicit val formats: DefaultFormats = DefaultFormats
    parse(json).extract[SchemaFile]
  }
}

trait HasAvroSerDeConfig extends Params {
  final val schemaRegistryUrl: Param[String] = new Param[String](
    this,
    "schemaRegistryUrl",
    "Schema registry url"
  )
  final def getSchemaRegistryUrl: String = $ { schemaRegistryUrl }
  final def setSchemaRegistryUrl(value: String): this.type =
    set(schemaRegistryUrl, value)

  final val schemaId: Param[String] = new Param[String](
    this,
    "schemaId",
    "Schema id"
  )
  final def getSchemaId: String = $ { schemaId }
  final def setSchemaId(value: String): this.type = set(schemaId, value)

  final val schemaVersion: Param[String] = new Param[String](
    this,
    "schemaVersion",
    "Schema version"
  )
  final def getSchemaVersion: String = $ { schemaVersion }
  final def setSchemaVersion(value: String): this.type =
    set(schemaVersion, value)

  final val strategyMethod: Param[String] = new Param[String](
    this,
    "strategyMethod",
    "Strategy method"
  )
  final def getStrategyMethod: String = $ { strategyMethod }
  final def setStrategyMethod(value: String): this.type =
    set(strategyMethod, value)

  final val topicName: Param[String] = new Param[String](
    this,
    "topicName",
    "Topic name"
  )
  final def getTopicName: String = $ { topicName }
  final def setTopicName(value: String): this.type = set(topicName, value)

  final val recordName: Param[String] = new Param[String](
    this,
    "recordName",
    "Record name"
  )
  final def getRecordName: String = $ { recordName }
  final def setRecordName(value: String): this.type = set(recordName, value)

  final val isKey: BooleanParam = new BooleanParam(
    this,
    "isKey",
    "is key"
  )
  final def getIsKey: Boolean = $ { isKey }
  final def setIsKey(value: Boolean): this.type = set(isKey, value)
  setDefault(isKey, false)

  final val avroType: Param[String] = new Param[String](
    this,
    "avroType",
    "avro type"
  )
  final def getAvroType: String = $ { avroType }
  final def setAvroType(value: String): this.type = set(avroType, value)
  setDefault(avroType, Config.CONFLUENT_AVRO)
}

trait HasAvroBytesReadSerDe extends Params {

  final val trimOffset: IntParam = new IntParam(
    this,
    "trimOffset",
    "Trim bytes offset"
  )

  final def getTrimOffset: Int = $ { trimOffset }
  final def setTrimOffset(value: Int): this.type = set(trimOffset, value)
  setDefault(trimOffset, 28)
}

trait HasJsonSerDe extends Params {
  final val colsDefinition: ColumnDefinitionArrayParam =
    new ColumnDefinitionArrayParam(
      this,
      "colsDefinition",
      "pair of column name and data type")
  final def getColsDefinition: Array[ColumnDefinition] = ${colsDefinition}
  final def setColsDefinition(value: Array[ColumnDefinition]): this.type =
    set(colsDefinition, value)
}

case class ColumnDefinition(name: String, dataType: String)

class ColumnDefinitionArrayParam(
    parent: Params,
    name: String,
    doc: String,
    isValid: Array[ColumnDefinition] => Boolean)
  extends Param[Array[ColumnDefinition]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  /** Creates a param pair with a `java.util.List` of values (for Java and Python). */
  def w(value: java.util.List[ColumnDefinition])
  : ParamPair[Array[ColumnDefinition]] =
    w(value.asScala.toArray)

  override def jsonEncode(value: Array[ColumnDefinition]): String = {
    import org.json4s.JsonDSL._
    implicit val formats: DefaultFormats = DefaultFormats
    compact(write(value.toSeq))
  }

  override def jsonDecode(json: String): Array[ColumnDefinition] = {
    implicit val formats: DefaultFormats = DefaultFormats
    parse(json).extract[Seq[ColumnDefinition]].toArray
  }
}

case class FeatureGroup(name: String,
                         features: Option[Array[String]] = None,
                         excludeFeatures: Option[Array[String]] = None,
                         schemaId: Option[String] = None,
                         schemaVersion: Option[String] = None,
                         schemaValueString: Option[String] = None
                        )

class FeatureGroupsArrayParam(parent: Params,
                              name: String,
                              doc: String,
                              isValid: Array[FeatureGroup] => Boolean)
  extends Param[Array[FeatureGroup]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  /** Creates a param pair with a `java.util.List` of values (for Java and Python). */
  def w(value: java.util.List[FeatureGroup])
  : ParamPair[Array[FeatureGroup]] =
    w(value.asScala.toArray)

  override def jsonEncode(value: Array[FeatureGroup]): String = {
    import org.json4s.JsonDSL._
    implicit val formats: DefaultFormats = DefaultFormats
    compact(write(value.toSeq))
  }

  override def jsonDecode(json: String): Array[FeatureGroup] = {
    implicit val formats: DefaultFormats = DefaultFormats
    parse(json).extract[Seq[FeatureGroup]].toArray
  }
}

case class FillNull(value: String,
                    dataType: String,
                    columns: Option[Array[String]] = None)
class FillNullArrayParam(parent: Params,
                         name: String,
                         doc: String,
                         isValid: Array[FillNull] => Boolean)
  extends Param[Array[FillNull]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  /** Creates a param pair with a `java.util.List` of values (for Java and Python). */
  def w(value: java.util.List[FillNull])
  : ParamPair[Array[FillNull]] =
    w(value.asScala.toArray)

  override def jsonEncode(value: Array[FillNull]): String = {
    import org.json4s.JsonDSL._
    implicit val formats: DefaultFormats = DefaultFormats
    compact(write(value.toSeq))
  }

  override def jsonDecode(json: String): Array[FillNull] = {
    implicit val formats: DefaultFormats = DefaultFormats
    parse(json).extract[Seq[FillNull]].toArray
  }
}

trait HasFeatureGroupSerDe extends Params {
  final val FEATURE_GROUP_NAME_COL = "name"
  final val PROJECT_COL = "project"
  final val ENTITY_COL = "entity"
  final val FS_EVENT_TIME_COL = "event_time"
  final val FS_DATE_PATTERN = "yyyy-MM-dd"
  final val FEATURE_GROUP_VALUE_COL = "features"
  final val ROW_NUMBER_COL = "_row_number"
  final val STRUCT_ID_COL = "_struct_id"
  final val FS_ID_COL = "hashid"
  final val FS_ID_SERIALIZED_COL = "serializedId"
  final val SERDE_TEMP_COL = "_parsed"
  final val avroType: Param[String] = new Param[String](
    this,
    "avroType",
    "avro type"
  )

  final def getAvroType: String = $ {
    avroType
  }

  final def setAvroType(value: String): this.type = set(avroType, value)

  setDefault(avroType, Config.CONFLUENT_AVRO)

  final val featureGroup: Param[String] = new Param[String](
    this,
    "featureGroup",
    "Feature Group Name"
  )
  final def getFeatureGroup: String = $ { featureGroup }
  final def setFeatureGroup(value: String): this.type = set(featureGroup, value)

  final val project: Param[String] = new Param[String](
    this,
    "project",
    "Project Name or id"
  )
  final def getProject: String = $ { project }
  final def setProject(value: String): this.type = set(project, value)

  final val entity: Param[String] = new Param[String](
    this,
    "entity",
    "Entity Name"
  )
  final def getEntity: String = $ { entity }
  final def setEntity(value: String): this.type = set(entity, value)

  final val eventTimeCol: Param[String] = new Param[String](
    this,
    "eventTimeCol",
    "Event time column name"
  )
  final def getEventTimeCol: String = $ { eventTimeCol }
  final def setEventTimeCol(value: String): this.type = set(eventTimeCol, value)

  final val datePattern: Param[String] = new Param[String](
      this,
      "datePattern",
      "Date pattern"
  )

  final def getDatePattern: String = $ { datePattern }
  final def setDatePattern(value: String): this.type = set(datePattern, value)

  final val keyCols: StringArrayParam = new StringArrayParam(
    this,
    "keyCols",
    "Key columns"
  )
  final def getKeyCols: Array[String] = $ { keyCols }
  final def setKeyCols(value: Array[String]): this.type = set(keyCols, value)

  final val featureGroups: FeatureGroupsArrayParam = new FeatureGroupsArrayParam(
    this,
    "featureGroups",
    "Feature groups"
  )

  final def getFeatureGroups: Array[FeatureGroup] = $ { featureGroups }
  final def setFeatureGroups(value: Array[FeatureGroup]): this.type = set(featureGroups, value)

  final val schemaRegistryURL: Param[String] = new Param[String] (
    this,
    "schemaRegistryURL",
    "schema registry url"
  )

  final def getSchemaRegistryURL: String = $(schemaRegistryURL)
  final def setSchemaRegistryURL(value: String): this.type = set(schemaRegistryURL, value)

  final val schemaValueString: Param[String] = new Param[String](
    this,
    "schemaValueString",
    "schema value string"
  )

  final def getSchemaValueString: String = $(schemaValueString)

  final def setSchemaValueString(value: String): this.type = set(schemaValueString, value)

  final val schemaKeyString: Param[Option[String]] = new Param[Option[String]](
    this,
    "schemaKeyString",
    "schema Key string"
  )

  final def getSchemaKeyString: Option[String] = $(schemaKeyString)

  final def setSchemaKeyString(value: Option[String]): this.type = set(schemaKeyString, value)
  setDefault(schemaKeyString, None)

  final val startEventTime: Param[String] = new Param[String](
    this,
    "startEventTime",
    "start event time"
  )

  final def getStartEventTime: String = $ {
    startEventTime
  }

  final def setStartEventTime(value: String): this.type = set(startEventTime, value)

  final val endEventTime: Param[String] = new Param[String](
    this,
    "endEventTime",
    "end event time"
  )

  final def getEndEventTime: String = $ {
    endEventTime
  }

  final def setEndEventTime(value: String): this.type = set(endEventTime, value)

  final val fillNull: FillNullArrayParam = new FillNullArrayParam(
    this,
    "fillNull",
    "Fill null with value"
  )

  final def getFillNull: Array[FillNull] = $ {
    fillNull
  }

  final def setFillNull(value: Array[FillNull]): this.type = set(fillNull, value)

  final val dropEventTime: BooleanParam = new BooleanParam(
    this,
    "dropEventTime",
    "drop event time column"
  )

  final def getDropEventTime: Boolean = $ {
    dropEventTime
  }
  final def setDropEventTime(value: Boolean): this.type = set(dropEventTime, value)
  setDefault(dropEventTime, false)
}
