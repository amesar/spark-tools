package org.amm.spark.sql.report

import org.apache.spark.sql.SparkSession

object CommonUtils {
  def split(str: String) : Seq[String] = return if (str == "") return Seq() else str.split(",")

  def getTableNames(spark: SparkSession,database: String) : Seq[String] = {
    val df = spark.catalog.listTables(database)
    df.select("name").collect.map(_.getString(0)).toList
  }

  def getTables(spark: SparkSession, database: String, tables: Seq[String]) : Seq[String] = {
    val df = spark.catalog.listTables(database)
    val allTables = df.select("name").collect.map(_.getString(0)).toList
    val allTablesSet = allTables.toSet
    val tablesSet = tables.toSet
    val diff = tablesSet.diff(allTablesSet)
    if (diff.size > 0)
      throw new Exception("Tables do not exist: "+diff.mkString(","))
    if (tables.size > 0) tables else allTables
  }
}
