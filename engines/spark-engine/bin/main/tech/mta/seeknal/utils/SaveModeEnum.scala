package tech.mta.seeknal.utils

object SaveModeEnum extends Enumeration {

  type saveMode = Value

  val Append = Value("Append")
  val ErrorIfExists = Value("ErrorIfExists")
  val Ignore = Value("Ignore")
  val Overwrite = Value("Overwrite")
  val Upsert = Value("Upsert")

  val allSaveModes = Seq(Append, ErrorIfExists, Ignore, Overwrite, Upsert)
}
