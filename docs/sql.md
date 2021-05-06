# InfluxDB SQL Repl Tips and Tricks

InfluxDB IOx contains a built in SQL client you can use to interact with the server using SQL

## Quick Start

```shell
influxdb_iox --host http://localhost:8082  sql

Connected to IOx Server at http://localhost:8082
Ready for commands. (Hint: try 'help;')
>

```

## Remote Mode
In this mode queries are run on the remote server in the context of a database. To use this mode, specify a database and then run queries as normal

```
> show databases;
+-----------------------------------+
| db_name                           |
+-----------------------------------+
| 084d710ebdd819a5_apps             |
| 7f4b06e9112d7bc8_system%255Fusage |
| 7f4b06e9112d7bc8_system%5Fusage   |
| 844910ece80be8bc_057abada752bc8ce |
| 844910ece80be8bc_05a7a51565539000 |
+-----------------------------------+
```

Then run the `USE DATABASE` command

```
> use database my_db;
You are now in remote mode, querying database my_db
my_db>
```

Now, all queries will be run against the specified database (`my_db`) in this example
```
my_db> show tables;
+---------------+--------------------+-------------+------------+
| table_catalog | table_schema       | table_name  | table_type |
+---------------+--------------------+-------------+------------+
| public        | iox                | query_count | BASE TABLE |
| public        | system             | chunks      | BASE TABLE |
| public        | system             | columns     | BASE TABLE |
| public        | system             | operations  | BASE TABLE |
| public        | information_schema | tables      | VIEW       |
| public        | information_schema | columns     | VIEW       |
+---------------+--------------------+-------------+------------+

Query execution complete in 32.034867ms
my_db> select count(*) from query_count;
+-----------------+
| COUNT(UInt8(1)) |
+-----------------+
| 2               |
+-----------------+

Query execution complete in 46.852934ms
```


## Observer
In this mode queries are run *locally* against a cached unified view of the remote system tables

```
my_db> observer;
Preparing local views of remote system tables
Loading system tables from 49 databases
...................................................................................................................................................
 Completed in 4.048318429s
You are now in Observer mode.

SQL commands in this mode run against a cached unified view of
remote system tables in all remote databases.

To see the unified tables available to you, try running
SHOW TABLES;

To reload the most recent version of the database system tables, run
OBSERVER;

Example SQL to show the total estimated storage size by database:

SELECT database_name, storage, count(*) as num_chunks,
  sum(estimated_bytes)/1024/1024 as estimated_mb
FROM chunks
GROUP BY database_name, storage
ORDER BY estimated_mb desc;

OBSERVER>

```



# Query Cookbook

This section contains some common and useful queries against IOx system tables


## Explore Schema


The `SHOW TABLES` command will show all tables available to you.

```
my_db> show tables;
+---------------+--------------------+-------------+------------+
| table_catalog | table_schema       | table_name  | table_type |
+---------------+--------------------+-------------+------------+
| public        | iox                | query_count | BASE TABLE |
| public        | system             | chunks      | BASE TABLE |
| public        | system             | columns     | BASE TABLE |
| public        | system             | operations  | BASE TABLE |
| public        | information_schema | tables      | VIEW       |
| public        | information_schema | columns     | VIEW       |
+---------------+--------------------+-------------+------------+

Query execution complete in 45.06072ms
```
tables in the `iox` schema are those that you have loaded (`query_count`) in this example. Tables which allow you to interrogate IOx's internals are in the `system` schema. The `information_schema` schema contains SQL standard views for accessing table and column metadata.


The `SHOW COLUMNS FROM <tablename>` command will show the columns in a table

```
my_db> show columns from query_count;
+---------------+--------------+-------------+-------------+-----------------------------+-------------+
| table_catalog | table_schema | table_name  | column_name | data_type                   | is_nullable |
+---------------+--------------+-------------+-------------+-----------------------------+-------------+
| public        | iox          | query_count | avg         | Int64                       | YES         |
| public        | iox          | query_count | time        | Timestamp(Nanosecond, None) | NO          |
+---------------+--------------+-------------+-------------+-----------------------------+-------------+

Query execution complete in 62.77733ms
```

A classic trick to explore a table's schema is to select from it with a limit. For example:

```
my_db> select * from query_count limit 2;
+-----+---------------------+
| avg | time                |
+-----+---------------------+
| 461 | 2021-04-28 16:51:00 |
| 402 | 2021-04-28 17:51:00 |
+-----+---------------------+

Query execution complete in 39.046225ms
```



## System Tables

Here are some interesting reports you can run when in `OBSERVER` mode:

### Total storage size taken by each database

```sql
SELECT
  database_name, count(*) as num_chunks,
  sum(estimated_bytes)/1024/1024 as estimated_mb
FROM chunks
GROUP BY database_name
ORDER BY estimated_mb desc
LIMIT 20;
```

### Total estimated storage size by database and storage class
```sql
SELECT
  database_name, storage, count(*) as num_chunks,
  sum(estimated_bytes)/1024/1024 as estimated_mb
FROM chunks
GROUP BY database_name, storage
ORDER BY estimated_mb desc
LIMIT 20;
```

### Total estimated storage size by database, table_name and storage class

```sql
SELECT
  database_name, table_name, storage, count(*) as num_chunks,
  sum(estimated_bytes)/1024/1024 as estimated_mb
FROM chunks
GROUP BY database_name, table_name, storage
ORDER BY estimated_mb desc
LIMIT 20;
```


### Total row count by table

```sql
SELECT database_name, table_name, max(count) as total_rows
FROM columns
GROUP BY database_name, table_name
ORDER BY total_rows DESC
LIMIT 20;
```

### Total row count by partition and table

```sql
SELECT database_name, partition_key, table_name, max(count) as total_rows
FROM columns
GROUP BY database_name, partition_key, table_name
ORDER BY total_rows DESC
LIMIT 20;
```


# SQL Reference

Since IOx uses Apache Arrow's
[DataFusion](https://github.com/apache/arrow-datafusion) query engine, IOx SQL is mostly the same as DataFusion.

In this section, IOx specific SQL tables, commands, and extensions are documented.


## System Tables

In addition to the SQL standard `information_schema`, IOx contains several *system tables* that provide access to IOx specific information. The information in each system table is scoped to that particular database. Cross database queries are not possible due to the design of IOx's security model. Another process, such as the `observer` mode in the IOx SQL client, must be used for queries on information that spans databases.

### `system.chunks`
`system.chunks` contains information about each IOx storage chunk (which holds part of the data for a table).

TODO: document each column, once they have stabilized.

### `system.columns`
`system.columns` contains IOx specific schema information about each column in each table, such as which columns were loaded as tags, fields, and timestamps in the InfluxDB data model.

TODO: document each column, once they have stabilized.
