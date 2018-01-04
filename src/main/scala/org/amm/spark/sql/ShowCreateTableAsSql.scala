package org.amm.spark.sql.report

import org.apache.spark.sql.SparkSession
import java.io.{PrintStream,FileOutputStream,PrintWriter}
import com.beust.jcommander.{JCommander, Parameter}

object ShowCreateTableAsSql {
  def main(args: Array[String]) {
    val opts = new Options()
    new JCommander(opts, args.toArray: _*)
    display(opts)
    val spark = SparkSession.builder().appName("ShowCreateTableAsSql").enableHiveSupport().getOrCreate()
    process(spark, opts.database, opts.tableList, opts.outputFile, opts.manyLines, opts.dropTable)
  }

  def process(spark: SparkSession, database: String, desiredTables: Seq[String] = Seq.empty, outputFile: String = null, manyLines: Boolean = false, dropTable: Boolean = false) {
    val out = if (outputFile == null) System.out else new FileOutputStream(outputFile)

    new PrintWriter(out) {
      for (table <- desiredTables) {
	val tableName = s"${database}.$table"
        System.err.println(s"Processing $tableName")
        if (dropTable) println(s"DROP TABLE IF EXISTS ${tableName};")
        val df = spark.sql(s"show create table $tableName")
        val ddl = df.collect.map(_.getString(0))
        for (line <- ddl) {
          val content = if (manyLines) line else line.replace("\n"," ")
          println(content+";")
        }
        if (dropTable) println()
      }
      flush()
    }
  }

  class Options extends BaseOptions {
    @Parameter(names = Array("-o", "--outputFile" ), description = "outputFile", required=false)
    var outputFile : String = _

    @Parameter(names = Array("--dropTable" ), description = "dropTable", required=false)
    var dropTable = false

    @Parameter(names = Array("--manyLines" ), description = "manyLines", required=false)
    var manyLines = false
  }

  def display(opts: Options) {
    opts.display()
    System.err.println(s"  outputFile: ${opts.outputFile}")
    System.err.println(s"  dropTable: ${opts.dropTable}")
    System.err.println(s"  manyLines: ${opts.manyLines}")
  }
}
