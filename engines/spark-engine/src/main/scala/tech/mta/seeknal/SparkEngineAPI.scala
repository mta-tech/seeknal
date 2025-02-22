package tech.mta.seeknal

import scala.collection.JavaConverters._

import org.apache.spark.sql.DataFrame
import org.json4s._
import org.json4s.jackson.Serialization

class SparkEngineAPI {

  var jobSpec = JobSpec(None, None)
  var commonObject = CommonObject(None, None)
  var datePattern: String = "yyyyMMdd"
  var inlineParam: Option[Map[String, String]] = None
  var date: Seq[String] = Seq()
  var filter: Option[Map[String, String]] = None
  var inputSourceId = "base_input"
  var outputSourceId = "base_output"

  def setJobSpec(file: String = null, str: String = null): this.type = {
    if (file != null) {
      this.jobSpec = JobSpec(Some(file), None)
    } else if (str != null) {
      this.jobSpec = JobSpec(None, Some(str))
    }
    this
  }

  def setCommonObject(file: String = null, str: String = null): this.type = {
    if (file != null) {
      this.commonObject = CommonObject(Some(file), None)
    } else if (str != null) {
      this.commonObject = CommonObject(None, Some(str))
    }
    this
  }

  def setInlineParam(inlineParam: java.util.HashMap[String, String]): this.type = {
    if (inlineParam != null) {
      this.inlineParam = Some(inlineParam.asScala.toMap)
    }
    this
  }

  def setFilter(predicate: java.util.HashMap[String, String] = null): this.type = {
    if (predicate != null) {
      this.filter = Some(predicate.asScala.toMap)
    }
    this
  }

  def setDate(date: java.util.ArrayList[String] = null): this.type = {
    if (date != null) {
      this.date = date.asScala.toSeq
    }
    this
  }

  def setInputSourceId(inputSourceId: String): this.type = {
    this.inputSourceId = inputSourceId
    this
  }

  def setOutputSourceId(outputSourceId: String): this.type = {
    this.outputSourceId = outputSourceId
    this
  }

  def setDatePattern(datePattern: String): this.type = {
    this.datePattern = datePattern
    this
  }

  def transform(inputDF: DataFrame, dateCol: String = "day"): DataFrame = {
    val sparkEngine = SparkEngine(
      jobSpec = Seq(this.jobSpec),
      commonObject = this.commonObject,
      filter = this.filter,
      maxResults = 0,
      range = this.date,
      datePattern = this.datePattern,
      inlineParam = this.inlineParam,
      inputSourceId = Some(this.inputSourceId),
      outputSourceId = Some(this.outputSourceId)
    )
    val res = sparkEngine.transform(Some(inputDF), dateCol)
    res.get().head
  }

  def transform(): DataFrame = {
    val sparkEngine = SparkEngine(
      jobSpec = Seq(this.jobSpec),
      commonObject = this.commonObject,
      filter = this.filter,
      maxResults = 0,
      range = this.date,
      datePattern = this.datePattern,
      inlineParam = this.inlineParam,
      inputSourceId = Some(this.inputSourceId),
      outputSourceId = Some(this.outputSourceId)
    )
    val res = sparkEngine.transform()
    res.get().head
  }

  def run(): Unit = {
    val sparkEngine = SparkEngine(
      jobSpec = Seq(this.jobSpec),
      commonObject = this.commonObject,
      filter = this.filter,
      maxResults = 0,
      range = this.date,
      datePattern = this.datePattern,
      inlineParam = this.inlineParam,
      inputSourceId = Some(this.inputSourceId),
      outputSourceId = Some(this.outputSourceId)
    )
    sparkEngine.run()
  }

  def materialize(inputDF: DataFrame, date: String = ""): Unit = {
    val sparkEngine = SparkEngine(
      jobSpec = Seq(this.jobSpec),
      commonObject = this.commonObject,
      filter = this.filter,
      maxResults = 0,
      range = this.date,
      datePattern = this.datePattern,
      inlineParam = this.inlineParam,
      inputSourceId = Some(this.inputSourceId),
      outputSourceId = Some(this.outputSourceId)
    )
    sparkEngine.materialize(Map(date -> Seq(inputDF)))
  }

  def getAvroSchema(inputDF: DataFrame): String = {
    val sparkEngine = SparkEngine(
      jobSpec = Seq(this.jobSpec),
      commonObject = this.commonObject,
      filter = this.filter,
      maxResults = 0,
      range = this.date,
      datePattern = this.datePattern,
      inlineParam = this.inlineParam,
      inputSourceId = Some(this.inputSourceId),
      outputSourceId = Some(this.outputSourceId)
    )
    val schema = sparkEngine.getAvroSchema()
    implicit val formats = Serialization.formats(NoTypeHints)
    val jsonRes = Serialization.write(schema.asInstanceOf[Map[String, Any]])
    jsonRes
  }
}
