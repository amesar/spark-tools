package org.amm.spark.sql.report

import org.apache.spark.sql.SparkSession
import java.io.{PrintStream,FileOutputStream,PrintWriter}
import com.beust.jcommander.{JCommander, Parameter}

object ListTableColumns {
  def main(args: Array[String]) {
    val opts = new BaseOptions()
    new JCommander(opts, args.toArray: _*)
    opts.display()
    val spark = SparkSession.builder().appName("ShowCreateTableAsSql").enableHiveSupport().getOrCreate()
    process(spark, opts.database, opts.tableList)
  }

  def process(spark: SparkSession, database: String, desiredTables: Seq[String] = Seq.empty) {
    val tables = if (desiredTables.size > 0) desiredTables else CommonUtils.getTableNames(spark,database)

    for (table <- tables) {
      val tableName = s"${database}.$table"
      val df = spark.catalog.listColumns(tableName)
      println(s"Table $tableName")
      df.show(1000,false)
    }
  }
}
