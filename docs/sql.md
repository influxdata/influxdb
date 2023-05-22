# InfluxDB SQL Repl Tips and Tricks

InfluxDB IOx contains a built in SQL client you can use to interact with the server using SQL.

## Quick Start

```shell
influxdb_iox --host http://localhost:8082  sql

Connected to IOx Server at http://localhost:8082
Ready for commands. (Hint: try 'help;')
>

```

## Remote Mode
In this mode queries are run on the remote server in the context of a namespace. To use this mode, specify a namespace and then run queries as normal

```
> show namespaces;
+--------------+-----------------------------------+
| namespace_id | name                              |
+--------------+-----------------------------------+
| 1            | internal_test                     |
| 2            | 33a8e04942966029_8dce392f44992579 |
| 3            | 810c5937734635d8_dbce66e3a6cbe757 |
+--------------+-----------------------------------+
```

Then run the `USE <your_namespace>` command

```
>  use 810c5937734635d8_dbce66e3a6cbe757;
You are now in remote mode, querying namespace 810c5937734635d8_dbce66e3a6cbe757
810c5937734635d8_dbce66e3a6cbe757>
```

Now, all queries will be run against the specified namespace (`810c5937734635d8_dbce66e3a6cbe757`) in this example
```
810c5937734635d8_dbce66e3a6cbe757> show tables;
+---------------+--------------------+-------------+------------+
| table_catalog | table_schema       | table_name  | table_type |
+---------------+--------------------+-------------+------------+
| public        | iox                | cpu         | BASE TABLE |
| public        | iox                | disk        | BASE TABLE |
| public        | iox                | diskio      | BASE TABLE |
| public        | iox                | mem         | BASE TABLE |
| public        | iox                | net         | BASE TABLE |
| public        | iox                | processes   | BASE TABLE |
| public        | iox                | swap        | BASE TABLE |
| public        | iox                | system      | BASE TABLE |
| public        | system             | queries     | BASE TABLE |
| public        | information_schema | tables      | VIEW       |
| public        | information_schema | views       | VIEW       |
| public        | information_schema | columns     | VIEW       |
| public        | information_schema | df_settings | VIEW       |
+---------------+--------------------+-------------+------------+
Returned 13 rows in 101.973855ms
810c5937734635d8_dbce66e3a6cbe757> select count(*) from cpu;
+-----------------+
| COUNT(UInt8(1)) |
+-----------------+
| 66980           |
+-----------------+
Returned 1 row in 74.022768ms
810c5937734635d8_dbce66e3a6cbe757>
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

# Debug Features
Certain features are hidden behind a "debug mode". This mode opt-in and is enabled when the `iox-debug` HTTP header / gRPC metadata
field is set to `1`. In this case, certain operations are possible that are helpful to IOx developers but that may
confuse end users. Features that are debug features are marked as such.

**IMPORTANT: Debug features MUST NOT contain security-critical information. It is assumed that every user (even our end
users) can always enable the debug features!**


# SQL Reference

Since IOx uses Apache Arrow's
[DataFusion](https://github.com/apache/arrow-datafusion) query engine, IOx SQL is mostly the same as DataFusion.

In this section, IOx specific SQL tables, commands, and extensions are documented.


## System Tables

In addition to the SQL standard `information_schema`, IOx contains several *system tables* that provide access to IOx
specific information. The information in each system table is scoped to that particular namespace. Cross namespace
queries are not possible due to the design of IOx's security model.

### `system.queries`
**This is a debug feature.**

`system.queries` contains information about queries run against this IOx instance. The query log is process local and
NOT shared across instances within the same deployment. While the log size is limited per instance, the view on this log
is scoped to the requesting namespace (i.e. queries are NOT leaked across namespaces.).
