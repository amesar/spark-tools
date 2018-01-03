package org.amm.spark.sql.report

import org.apache.spark.sql.SparkSession
import com.beust.jcommander.{JCommander, Parameter}

object DatabaseReport {
  def main(args: Array[String]) {
    val opts = new Options()
    new JCommander(opts, args.toArray: _*)
    display(opts)
    val spark = SparkSession.builder().appName("DatabaseReport").enableHiveSupport().getOrCreate()
    val databases = CommonUtils.split(opts.databases)
    process(spark, databases)
  }

  def process(spark: SparkSession, desiredDatabases: Seq[String]) {
    println("SparkConfig")
    for ((k,v) <- spark.conf.getAll) println(s"  $k: $v")

    println()
    println("Databases")
    val df = spark.catalog.listDatabases()
    df.show(1000,false)
    for (database <- df.select("name").collect.map(_.getString(0))) {
       if (desiredDatabases.size == 0 || desiredDatabases.contains(database)) {
         println(s"Database $database")
         val df = spark.catalog.listTables(database)
         df.show(1000,false)
      }
    }
  }

  class Options {
    @Parameter(names = Array("-d", "--databases" ), description = "database", required=false)
    var databases = ""
  }

  def display(opts: Options) {
    println("Options:")
    println(s"  databases: ${opts.databases}")
  }
}
