package tech.mta.seeknal.utils

import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.temporal.ChronoField

import com.typesafe.config.Config
import org.apache.log4j.{Level, Logger, LogManager}
import tech.mta.seeknal.params.{Config => myConfig}

object Utils {

  val logger: Logger = LogManager.getLogger(this.getClass)
  logger.setLevel(Level.INFO)

  /** Reformat date pattern into other format pattern
    *
    * @param date
    *   string
    * @param inputPattern
    *   existing date format
    * @param outputPattern
    *   target date format
    */
  def formatDate(date: String, inputPattern: String, outputPattern: String): String = {
    val result =
      try {
        val parser = new DateTimeFormatterBuilder()
          .parseCaseInsensitive()
          .appendPattern(inputPattern)
          .parseDefaulting(ChronoField.MONTH_OF_YEAR, 1)
          .parseDefaulting(ChronoField.DAY_OF_MONTH, 1)
          .toFormatter()
        val formatter = DateTimeFormatter.ofPattern(outputPattern)

        formatter.format(parser.parse(date))
      } catch {
        case e: Throwable =>
          print(e)
          assert(false)
          ""
      }
    result
  }

  /** collect elapsed time for block of code code from:
    * http://biercoff.com/easily-measuring-code-execution-time-in-scala/
    *
    * @param block
    *   block of code
    */
  def time[T](block: => T): T = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    val seconds = (t1 - t0) / 1000000000.0
    logger.info("Elapsed time: " + seconds + " s") // scalastyle:off
    result
  }

  /** parse 'startDate endDate' to range of dates
    *
    * @param rangeDates
    *   range of date as string with delimiter to identify the start date and end date. eg: '20190101 20190105'
    * @param inputPattern
    *   date pattern use for parse the date
    * @param sep
    *   delimeter used for separate start date and end date
    * @param outPattern
    *   intermediate date pattern for create the range of dates
    */
  def parseRangeDate(rangeDates: String,
                     inputPattern: String,
                     sep: String = " ",
                     outPattern: String = "yyyyMMdd"
  ): Seq[String] = {
    val twoDates = rangeDates.trim
      .stripPrefix("'")
      .stripSuffix("'")
      .stripPrefix("\"")
      .stripSuffix("\"")
      .split(sep)

    if (twoDates.size == 2) {
      val trimOutPattern = trimDatePattern(twoDates.head, outPattern)
      val startDate = formatDate(twoDates.head, inputPattern, trimOutPattern)
      val endDate = formatDate(twoDates(1), inputPattern, trimOutPattern)
      val numbers = (startDate.toInt to endDate.toInt).toArray
      numbers.toSeq.map(date => {
        formatDate(date.toString, trimOutPattern, inputPattern)
      })
    } else {
      Seq(twoDates.head)
    }
  }

  /** Remove alphanumeric and trim date string to make correct date pattern reformat
    *
    * @param date
    *   string
    * @param outPattern
    *   target date format
    * @param threshold
    *   minimum size of string to make trim string
    */
  def trimDatePattern(date: String, outPattern: String, threshold: Int = 8): String = {
    val noAlphaNumeric = date.replaceAll("[^a-zA-Z0-9]", "")
    val noAlphaNumericSize = noAlphaNumeric.size
    if (noAlphaNumericSize < threshold) {
      outPattern.substring(0, noAlphaNumericSize)
    } else {
      outPattern
    }
  }

  def replaceStringParams(inlineParams: Map[String, Either[String, Map[Boolean, Seq[String]]]],
                          config: String
  ): String = {

    val keyVal: Map[String, String] = inlineParams.map(x => {
      val myVal: String = x._2 match {
        case Left(value) =>
          value
        case Right(value) =>
          val string: Seq[String] =
            value.foldLeft(Seq[String]())(op = (emptyString, x) => {
              val seqString: String = if (x._1 == true) {
                x._2.map(x => "'" + x + "'").mkString(",")
              } else {
                x._2.mkString(",")
              }
              emptyString :+ seqString
            })
          string.mkString(",")
      }
      (x._1, myVal)
    })

    keyVal.foldLeft(config)(op = (origString, x) => {
      val key = "{" + x._1 + "}"
      origString.replace(key, x._2)
    })
  }

  def getAuthorFromConfig(config: Config, path: String): String = {
    if (config.hasPath(path)) {
      config.getString(path)
    } else {
      myConfig.DEFAULT_AUTHOR
    }
  }

}
