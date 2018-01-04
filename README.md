# spark-tools

Useful Spark tools.

## Summary

Scala tools:

* org.amm.spark.sql.report.DatabaseReport - List database details

## Details

### org.amm.spark.sql.report.DatabaseReport`

#### Example
```
spark-submit --class org.amm.spark.sql.report.DatabaseReport \
  --master local[2] \
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
| databases | List of comma-separated tables. If not specified all tables will be generated. | No | | tpcds,tpch |
| showSparkConfig | Display Spark Config | No | false | <br> |

