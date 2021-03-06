# spark-tools

Useful Spark tools.

## Summary

* DatabaseReport - Executes "spark.catalog.listTables($database)"
* DescribeTable - Executes Spark SQL "describe extended $table" for specified tables.
* ShowCreateTableAsSql - Execute Spark SQL "show create $table" for specified tables and output SQL/DDL code.
* ShowCreateTableAsScala - Execute Spark SQL "show create $table" for specified tables and output Scala code.
* ListTableColumns - Executes spark.catalog.listColumns($table)

## Details

### DatabaseReport

#### Example
```
spark-submit --class org.amm.spark.sql.report.DatabaseReport --master local[2] \
  target/amm-spark-tools-1.0-SNAPSHOT.jar \
  --databases tpcds \
  --showTables

Databases
+-------+---------------------+---------------------------------------------------------+
|name   |description          |locationUri                                              |
+-------+---------------------+---------------------------------------------------------+
|tpcds  |                     |file:/opt/spark/spark-warehouse/tpcds.db                 |
+-------+---------------------+---------------------------------------------------------+

Database tpcds
+---------------------------------+--------+-----------+---------+-----------+
|name                             |database|description|tableType|isTemporary|
+---------------------------------+--------+-----------+---------+-----------+
|customer                         |tpcds   |null       |EXTERNAL |false      |
|store                            |tpcds   |null       |EXTERNAL |false      |
+---------------------------------+--------+-----------+---------+-----------+
```

#### Options

| Property        | Description  | Required | Default| Sample Value |
|-----------------|--------|----------|----|---|
| databases | List of comma-separated databases. If not specified all databases will be generated. | No | | tpcds,tpch |
| showTables | Show tables for each database | No | false | <br> |
| showSparkConfig | Show Spark Config | No | false | <br> |

### DescribeTable

#### Example
```
spark-submit --class org.amm.spark.sql.report.DescribeTable --master local[2] \
  target/amm-spark-tools-1.0-SNAPSHOT.jar \
  --database tpcds \
  --tables customer,store

Table tpcds.customer
+----------------------------+---------------------------------------------------------+-------+
|col_name                    |data_type                                                |comment|
+----------------------------+---------------------------------------------------------+-------+
|c_customer_sk               |bigint                                                   |null   |
|c_customer_id               |string                                                   |null   |
|c_current_cdemo_sk          |bigint                                                   |null   |
|c_current_hdemo_sk          |bigint                                                   |null   |
|c_current_addr_sk           |bigint                                                   |null   |
|c_first_shipto_date_sk      |bigint                                                   |null   |
|c_first_sales_date_sk       |bigint                                                   |null   |
|c_salutation                |string                                                   |null   |
|c_first_name                |string                                                   |null   |
|c_last_name                 |string                                                   |null   |
|c_preferred_cust_flag       |string                                                   |null   |
|c_birth_day                 |int                                                      |null   |
|c_birth_month               |int                                                      |null   |
|c_birth_year                |int                                                      |null   |
|c_birth_country             |string                                                   |null   |
|c_login                     |string                                                   |null   |
|c_email_address             |string                                                   |null   |
|c_last_review_date          |string                                                   |null   |
|                            |                                                         |       |
|# Detailed Table Information|                                                         |       |
|Database                    |tpcds                                                    |       |
|Table                       |customer                                                 |       |
|Owner                       |bond                                                     |       |
|Created                     |Thu Dec 28 12:14:35 EST 2017                             |       |
|Last Access                 |Wed Dec 31 19:00:00 EST 1969                             |       |
|Type                        |EXTERNAL                                                 |       |
|Provider                    |CSV                                                      |       |
|Table Properties            |[transient_lastDdlTime=1514481275]                       |       |
|Location                    |file:/opt/spark/tables/tpcds/customer                    |       |
|Serde Library               |org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe       |       |
|InputFormat                 |org.apache.hadoop.mapred.SequenceFileInputFormat         |       |
|OutputFormat                |org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat|       |
|Storage Properties          |[delimiter=|, header=false, serialization.format=1]      |       |
+----------------------------+---------------------------------------------------------+-------+

Table tpcds.store
. . .
```

#### Options

| Property        | Description  | Required | Default| Sample Value |
|-----------------|--------|----------|----|---|
| database | Database name | Yes | | tpcds |
| tables | List of comma-separated tables. If not specified all tables will be generated. | No | | customer,store |



### ShowCreateTableAsSql

#### Example
```
spark-submit --class org.amm.spark.sql.report.ShowCreateTableAsSql --master local[2] \
  target/amm-spark-tools-1.0-SNAPSHOT.jar \
  --database tpcds \
  --tables customer \
  --dropTable

DROP TABLE IF EXISTS tpcds.customer;
CREATE TABLE `customer` (`c_customer_sk` BIGINT, `c_customer_id` STRING, `c_current_cdemo_sk` BIGINT, `c_current_hdemo_sk` BIGINT, `c_current_addr_sk` BIGINT, `c_first_shipto_date_sk` BIGINT, `c_first_sales_date_sk` BIGINT, `c_salutation` STRING, `c_first_name` STRING, `c_last_name` STRING, `c_preferred_cust_flag` STRING, `c_birth_day` INT, `c_birth_month` INT, `c_birth_year` INT, `c_birth_country` STRING, `c_login` STRING, `c_email_address` STRING, `c_last_review_date` STRING)
USING CSV
OPTIONS (
  `delimiter` '|',
  `header` 'false',
  `serialization.format` '1',
  path 'file:/opt/spark/tables/tpcds/customer'
) ;
```

#### Options

| Property        | Description  | Required | Default| Sample Value |
|-----------------|--------|----------|----|---|
| database | Database name | Yes | | tpcds |
| tables | List of comma-separated tables. If not specified all tables will be generated. | No | | customer,store |
| outputFile | Send output to file instead of stdout | No | | create_tpcds.ddl |
| dropTable | Generate drop table statement | No | false | drop table if exists tpcds.customer |
| oneLine | Output is one line | No | false | |

### ShowCreateTableAsScala

#### Example
```
spark-submit --class org.amm.spark.sql.report.ShowCreateTableAsScala --master local[2] \
  target/amm-spark-tools-1.0-SNAPSHOT.jar \
  --database tpcds \
  --tables customer \
  --outputFile gen_src/org/myorg/spark/CreateTpcds.scala \
  --dropTable \
  --package org.myorg.spark

cat gen_src/org/myorg/spark/CreateTpcds.scala 

package org.myorg.spark
import org.apache.spark.sql.SparkSession

object CreateTpcds {
  def generateDDL(spark: SparkSession) {
    spark.sql("drop table if exists tpcds.customer")
    spark.sql("CREATE TABLE `customer` (`c_customer_sk` BIGINT, `c_customer_id` STRING, `c_current_cdemo_sk` BIGINT, `c_current_hdemo_sk` BIGINT, `c_current_addr_sk` BIGINT, `c_first_shipto_date_sk` BIGINT, `c_first_sales_date_sk` BIGINT, `c_salutation` STRING, `c_first_name` STRING, `c_last_name` STRING, `c_preferred_cust_flag` STRING, `c_birth_day` INT, `c_birth_month` INT, `c_birth_year` INT, `c_birth_country` STRING, `c_login` STRING, `c_email_address` STRING, `c_last_review_date` STRING) USING CSV OPTIONS (   `delimiter` '|',   `header` 'false',   `serialization.format` '1',   path 'file:/opt/tables/tpcds/customer' ) ")
}

```

#### Options

| Property        | Description  | Required | Default| Sample Value |
|-----------------|--------|----------|----|---|
| database | Database name | Yes | | tpcds |
| tables | List of comma-separated tables. If not specified all tables will be generated. | No | | customer,store |
| outputFile | Send output to file instead of stdout | No | | gen/CreateTpcds.scala |
| dropTable | Generate drop table statement | No | false | spark.sql("drop table if exists tpcds.customer") |
| package | Package name| No | | org.myorg.spark |

### ListTableColumns

#### Example
```
spark-submit --class org.amm.spark.sql.report.ListTableColumns --master local[2] \
  target/amm-spark-tools-1.0-SNAPSHOT.jar \
  --database tpcds \
  --tables customer_part

Table tpcds.customer_part
+----------------------+-----------+--------+--------+-----------+--------+
|name                  |description|dataType|nullable|isPartition|isBucket|
+----------------------+-----------+--------+--------+-----------+--------+
|c_customer_sk         |null       |bigint  |true    |false      |false   |
|c_customer_id         |null       |string  |true    |false      |false   |
|c_current_cdemo_sk    |null       |bigint  |true    |false      |false   |
|c_current_hdemo_sk    |null       |bigint  |true    |false      |false   |
|c_current_addr_sk     |null       |bigint  |true    |false      |false   |
|c_first_shipto_date_sk|null       |bigint  |true    |false      |false   |
|c_first_sales_date_sk |null       |bigint  |true    |false      |false   |
|c_salutation          |null       |string  |true    |false      |false   |
|c_first_name          |null       |string  |true    |false      |false   |
|c_last_name           |null       |string  |true    |false      |false   |
|c_preferred_cust_flag |null       |string  |true    |false      |false   |
|c_birth_day           |null       |int     |true    |false      |false   |
|c_birth_year          |null       |int     |true    |false      |false   |
|c_birth_country       |null       |string  |true    |false      |false   |
|c_login               |null       |string  |true    |false      |false   |
|c_email_address       |null       |string  |true    |false      |false   |
|c_last_review_date    |null       |string  |true    |false      |false   |
|c_birth_month         |null       |int     |true    |true       |false   |
+----------------------+-----------+--------+--------+-----------+--------+
```

#### Options

| Property        | Description  | Required | Default| Sample Value |
|-----------------|--------|----------|----|---|
| database | Database name | Yes | | tpcds |
| tables | List of comma-separated tables. If not specified all tables will be generated. | No | | customer,store |
