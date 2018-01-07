package org.amm.spark.sql.report

import org.apache.spark.sql.SparkSession
import com.beust.jcommander.{JCommander, Parameter}

object ShowPartitions {

  def main(args: Array[String]) {
    val opts = new BaseOptions()
    new JCommander(opts, args.toArray: _*)
    display(opts)
    val spark = SparkSession.builder().appName("DescribeTable").enableHiveSupport().getOrCreate()
    process(spark, opts.database, opts.tableList)
  }

  def process(spark: SparkSession, database: String, desiredTables: Seq[String] = Seq.empty) {
    val fileName = s"create_${database}"
    val tables = CommonUtils.getTables(spark, database, desiredTables)

    for (table <- tables) {
      val tableName = s"$database.$table"
      //println(s"Table $tableName")
      showTablePartitions(spark,database,table)
      }
  }

  def showTablePartitions(spark: SparkSession, databaseName: String, tableName: String ) {
    try {
      import org.apache.spark.sql.catalyst.TableIdentifier
      val parts = spark.sessionState.catalog.listPartitions(TableIdentifier(tableName, Some(databaseName)))
      val part = parts(0)
      println(s"$tableName,${parts.size},${part.spec.keys.mkString("|")}")
    } catch {
      case e: Exception => { 
        println(s"ERROR: $tableName: $e")
      }
    }
  }

/*
  def showDatabasePartitions(spark: SparkSession, databaseName: String) {
    val tables = spark.catalog.listTables(databaseName).select("name").map(x => x(0).toString()).collect()
    for (table <- tables) {
     showTablePartitions(databaseName,table)
    }
  }
*/

  def display(opts: BaseOptions) {
    opts.display()
  }
}
