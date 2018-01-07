package org.amm.spark.sql.report

import org.apache.spark.sql.SparkSession
import java.io.{File,PrintStream,FileOutputStream,PrintWriter}
import com.beust.jcommander.{JCommander, Parameter}

/**
 * Generate Scala file to create database tables from "show create table".
 */
object ShowCreateTableAsScala {
  def main(args: Array[String]) {
    val opts = new Options()
    new JCommander(opts, args.toArray: _*)
    display(opts)
    val spark = SparkSession.builder().appName("ShowCreateTableAsScala").enableHiveSupport().getOrCreate()
    process(spark, opts.database, opts.tableList, opts.outputFile, opts.dropTable, opts.pkg)
  }

  def process(spark: SparkSession, database: String, desiredTables: Seq[String] = Seq.empty, outputFile: String = null, dropTable: Boolean = false, pkg: String = null) {
    val out = if (outputFile == null) System.out else new FileOutputStream(outputFile)
    val className = if (outputFile == null) s"$database.scala" else (new File(outputFile)).getName.replace(".scala","")
    val tables = if (desiredTables.size > 0) desiredTables else CommonUtils.getTableNames(spark,database)
    new PrintWriter(out) {
      if (pkg != null)
        println(s"package ${pkg}\n")
      println("import org.apache.spark.sql.SparkSession")
      println(s"\nobject ${className} {")
      println("  def generateDDL(spark: SparkSession) {")

      for (table <- tables) {
	val tableName = s"${database}.$table"
        System.err.println(s"Processing $tableName")
        val df = spark.sql(s"show create table $tableName")
        val ddl = df.collect.map(_.getString(0))
        for (line <- ddl) {
          if (dropTable) println("    "+mkSql(s"drop table if exists $tableName"))
          println("    "+mkSql(line.replace("\n"," ")))
          if (dropTable) println()
        }
      }
      println("  }\n}")
      flush()
    }
  }

  def mkSql(cmd: String) = "spark.sql(\"" + cmd + "\")"

  class Options extends BaseOptions {
    @Parameter(names = Array("-o", "--outputFile" ), description = "outputFile", required=false)
    var outputFile : String = _

    @Parameter(names = Array("--dropTable" ), description = "dropTable", required=false)
    var dropTable = false

    @Parameter(names = Array("--package" ), description = "package", required=false)
    var pkg: String = _
  }

  def display(opts: Options) {
    opts.display()
    System.err.println(s"  outputFile: ${opts.outputFile}")
    System.err.println(s"  dropTable: ${opts.dropTable}")
    System.err.println(s"  package: ${opts.pkg}")
  }
}
