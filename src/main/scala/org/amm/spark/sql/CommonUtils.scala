package org.amm.spark.sql.report

object CommonUtils {
  def split(str: String) : Seq[String] = return if (str == "") return Seq() else str.split(",")
}
