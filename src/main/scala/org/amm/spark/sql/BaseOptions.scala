package org.amm.spark.sql.report

import com.beust.jcommander.{JCommander, Parameter}

class BaseOptions {
  @Parameter(names = Array("-d", "--database" ), description = "database", required=true)
  var database: String = _

  @Parameter(names = Array("--tables" ), description = "tables", required=false)
  var tables = ""

  def tableList = split(tables)

  def split(str: String) : Seq[String] = return if (str == "") return Seq() else str.split(",")

  def display() {
    System.err.println("Options:")
    System.err.println(s"  database: ${database}")
    System.err.println(s"  tables: ${tables}")
  }
}
