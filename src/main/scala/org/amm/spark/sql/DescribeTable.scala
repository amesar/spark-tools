package org.amm.spark.sql.report

import org.apache.spark.sql.SparkSession
import com.beust.jcommander.{JCommander, Parameter}

object DescribeTable {

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("DescribeTable").enableHiveSupport().getOrCreate()
    main2(spark, args)
  }

  def main2(spark: SparkSession, args: Array[String]) {
    val opts = new BaseOptions()
    new JCommander(opts, args.toArray: _*)
    display(opts)
    process(spark, opts.database, opts.tableList)
  }

  def process(spark: SparkSession, database: String, desiredTables: Seq[String] = Seq.empty) {
    val fileName = s"create_${database}"
    val tables = CommonUtils.getTables(spark, database, desiredTables)

    for (table <- tables) {
      val tableName = s"$database.$table"
      println(s"Table $tableName")
      val df = spark.sql(s"describe formatted $tableName")
      df.show(10000,false)
      }
  }

  def display(opts: BaseOptions) {
    opts.display()
  }
}
