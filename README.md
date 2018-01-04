# spark-tools

Useful Spark tools.

## Summary

Tools:

* org.amm.spark.sql.report.DatabaseReport - List database details
* org.amm.spark.sql.report.DescribeTable - Executes 'Describe extended $table'

## Details

### org.amm.spark.sql.report.DatabaseReport`

#### Example
```
spark-submit --class org.amm.spark.sql.report.DatabaseReport --master local[2] \
  target/amm-spark-tools-1.0-SNAPSHOT.jar \
  --databases tpcds

Databases
+-------+---------------------+---------------------------------------------------------+
|name   |description          |locationUri                                              |
+-------+---------------------+---------------------------------------------------------+
|default|Default Hive database|file:/Users/amm/projects/spark/spark-warehouse           |
|tpcds  |                     |file:/Users/amm/projects/spark/spark-warehouse/tpcds.db  |
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

| Property        | Description  | Required | Default Value | Sample Value |
|-----------------|--------|----------|----|---|
| databases | List of comma-separated databases. If not specified all databases will be generated. | No | | tpcds,tpch |
| showSparkConfig | Display Spark Config | No | false | <br> |

### org.amm.spark.sql.report.DescribeTable`

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
|Owner                       |ander                                                    |       |
|Created                     |Thu Dec 28 12:14:35 EST 2017                             |       |
|Last Access                 |Wed Dec 31 19:00:00 EST 1969                             |       |
|Type                        |EXTERNAL                                                 |       |
|Provider                    |CSV                                                      |       |
|Table Properties            |[transient_lastDdlTime=1514481275]                       |       |
|Location                    |file:/Users/amm/tables/tpcds/customer                    |       |
|Serde Library               |org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe       |       |
|InputFormat                 |org.apache.hadoop.mapred.SequenceFileInputFormat         |       |
|OutputFormat                |org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat|       |
|Storage Properties          |[delimiter=|, header=false, serialization.format=1]      |       |
+----------------------------+---------------------------------------------------------+-------+

Table tpcds.store
. . .
```

#### Options

| Property        | Description  | Required | Default Value | Sample Value |
|-----------------|--------|----------|----|---|
| database | Database name | Yes | | tpcds |
| tables | List of comma-separated tables. If not specified all tables will be generated. | No | | customer,store |
