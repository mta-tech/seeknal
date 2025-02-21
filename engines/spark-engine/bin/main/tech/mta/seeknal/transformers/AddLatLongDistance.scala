package tech.mta.seeknal.transformers

import org.apache.spark.ml.param.{IntParam, Param}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, udf}
import tech.mta.seeknal.params.HasOutputCol

/** Calculate distance (in default km) source lat long point to target lat long point. Valid latitudes are -85.05 to
  * 85.05 degrees, valid longitudes are -180 to 180 degrees
  */
class AddLatLongDistance extends BaseTransformer with HasOutputCol {

  // TODO: change this from string to case class
  final val sourcePoint: Param[String] =
    new Param[String](this, "sourcePoint", "The columns for source point (lat, long), separated by comma")
  final def getSourcePoint: String = $(sourcePoint)
  final def setSourcePoint(value: String): this.type = set(sourcePoint, value)

  // TODO: change this from string to case class
  final val targetPoint: Param[String] =
    new Param[String](this, "targetPoint", "The columns for target point (lat, long), separated by comma")
  final def getTargetPoint: String = $(targetPoint)
  final def setTargetPoint(value: String): this.type = set(targetPoint, value)

  final val decimalOffset: IntParam = new IntParam(this, "decimalOffset", "The target point column")
  final def getDecimalOffset: Int = $(decimalOffset)
  final def setDecimalOffset(value: Int): this.type = set(decimalOffset, value)

  setDefault(decimalOffset, 3)
  setDefault(outputCol, "distance")

  def harvesineFun: (Double, Double, Double, Double) => Double = (cLat, cLong, lat, long) => {
    var clat = cLat
    var tlat = lat
    var clong = cLong
    var tlong = long

    if (cLat > 85.05 || cLat < -85.05 || lat > 85.05 || lat < -85.05) {
      // TODO: Add log to inform this clat is invalid
      clat = 0.0
      tlat = 0.0
      clong = 0.0
      tlong = 0.0
    }

    if (cLong > 180.0 || cLong < -180.0 || long > 180.0 || long < -180.0) {
      // TODO: Add log to inform this clong is invalid
      clong = 0.0
      tlong = 0.0
      clat = 0.0
      tlat = 0.0
    }

    val _clat = BigDecimal(clat).setScale(getDecimalOffset, BigDecimal.RoundingMode.DOWN).toDouble
    val _clong = BigDecimal(clong).setScale(getDecimalOffset, BigDecimal.RoundingMode.DOWN).toDouble
    val _lat = BigDecimal(tlat).setScale(getDecimalOffset, BigDecimal.RoundingMode.DOWN).toDouble
    val _long = BigDecimal(tlong).setScale(getDecimalOffset, BigDecimal.RoundingMode.DOWN).toDouble
    val earthRadiusMeter = 6371

    val latDiff = math.toRadians(_clat - _lat)
    val longDiff = math.toRadians(_clong - _long)
    val a = math.pow(math.sin(latDiff / 2), 2) +
      (math.cos(math.toRadians(_clat)) * math.cos(math.toRadians(_lat)) *
        math.pow(math.sin(longDiff / 2), 2))
    val c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    val res = earthRadiusMeter * c

    BigDecimal(res).setScale(getDecimalOffset, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

  override def transform(df: Dataset[_]): DataFrame = {
    val srclatlong = getSourcePoint.replaceAll("\\s+", "").split(",")
    val targetlatlong = getTargetPoint.replaceAll("\\s+", "").split(",")

    df.withColumn(
      getOutputCol,
      udf(harvesineFun).apply(col(srclatlong(0)), col(srclatlong(1)), col(targetlatlong(0)), col(targetlatlong(1)))
    )
  }
}
