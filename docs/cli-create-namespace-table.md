# CLI Cookbook for `influxdb_iox table create` and `influxdb_iox namespace create`

This file shows instructions on how to use:

1. `influxdb_iox namespace create [OPTIONS] <NAMESPACE>`
1. `influxdb_iox table create [OPTIONS] <DATABASE> <TABLE>`

## Instructions

1. Set up Postgres for Catalog if you havn't yet

   ```shell
   # Install Postgres
   brew install postgresql

   # Creates a new PostgreSQL database cluster
   initdb pg

   # Create a new superuser with username "postgres"
   createuser -s postgres

   # create a database called "iox_test"
   createdb iox_test
   ```

1. In `influxdb_iox` repo, build the most up-to-date IOx locally:

   ```
   cargo build
   ```

1. Then run all-in-one server:

   ```shell
   ./target/debug/influxdb_iox -vv run all-in-one --object-store=file --data-dir=/tmp/iox_data --catalog-dsn=postgres:iox_test --wal-rotation-period-seconds=1

   # Rather than building and running the binary in `target`, you can also compile and run with one command:
   cargo run -- run all-in-one --object-store=file --data-dir=/tmp/iox_data --catalog-dsn=postgres:iox_test --wal-rotation-period-seconds=1
   ```

1. In another shell, run the following to create a namespace and a table:

   ```shell
   # Create a namespace called "ns_test"
   ./target/debug/influxdb_iox -vv namespace create ns_test --host http://localhost:8081

   # Create a table called "t_test"
   ./target/debug/influxdb_iox -vv table create ns_test t_test  --host http://localhost:8081
   ```

1. Check the created table in the IOx SQL REPL

   ```shell
   # Run IOx SQL REPL locally
   $ ./target/debug/influxdb_iox sql

   # Then run the query
   > show namespaces;
   +--------------+---------+
   | namespace_id | name    |
   +--------------+---------+
   | 1            | ns_test |
   +--------------+---------+

   > use namespace ns_test;
   You are now in remote mode, querying namespace ns_test

   ns_test> show tables;
   +---------------+--------------------+-------------+------------+
   | table_catalog | table_schema       | table_name  | table_type |
   +---------------+--------------------+-------------+------------+
   | public        | iox                | t_test      | BASE TABLE |
   | public        | information_schema | tables      | VIEW       |
   | public        | information_schema | views       | VIEW       |
   | public        | information_schema | columns     | VIEW       |
   | public        | information_schema | df_settings | VIEW       |
   +---------------+--------------------+-------------+------------+
   Returned 5 rows in 35.365875ms

   ns_test> exit;

   ```

1. Write data

   ```shell
   ./target/debug/influxdb_iox -vv write ns_test ./test_fixtures/lineproto/air_and_water.lp --host http://localhost:8080

   ```

1. Read data using CLI

   ```shell
   $ ./target/debug/influxdb_iox -vv namespace list
   [
     {
       "id": "1",
       "name": "ns_test",
       "maxTables": 500,
       "maxColumnsPerTable": 200
     }
   ]

   $ ./target/debug/influxdb_iox -vv query ns_test 'show tables'
   +---------------+--------------------+-----------------+------------+
   | table_catalog | table_schema       | table_name      | table_type |
   +---------------+--------------------+-----------------+------------+
   | public        | iox                | air_temperature | BASE TABLE |
   | public        | iox                | h2o_temperature | BASE TABLE |
   | public        | iox                | t_test          | BASE TABLE |
   | public        | information_schema | tables          | VIEW       |
   | public        | information_schema | views           | VIEW       |
   | public        | information_schema | columns         | VIEW       |
   | public        | information_schema | df_settings     | VIEW       |
   +---------------+--------------------+-----------------+------------+

   $ ./target/debug/influxdb_iox -vv query ns_test 'select * from air_temperature'
   +--------------+-------------------+-------+------------------------+--------------------------------+
   | location     | sea_level_degrees | state | tenk_feet_feet_degrees | time                           |
   +--------------+-------------------+-------+------------------------+--------------------------------+
   | coyote_creek | 77.2              | CA    | 40.8                   | 1970-01-01T00:00:01.568756160Z |
   | coyote_creek | 77.1              | CA    | 41.0                   | 1970-01-01T00:00:01.600756160Z |
   | santa_monica | 77.3              | CA    | 40.0                   | 1970-01-01T00:00:01.568756160Z |
   | santa_monica | 77.6              | CA    | 40.9                   | 1970-01-01T00:00:01.600756160Z |
   | puget_sound  | 77.5              | WA    | 41.1                   | 1970-01-01T00:00:01.568756160Z |
   | puget_sound  | 78.0              | WA    | 40.9                   | 1970-01-01T00:00:01.600756160Z |
   +--------------+-------------------+-------+------------------------+--------------------------------+
   ```

1. (Or) Read data from IOx SQL REPL

   ```shell
   # Run IOx SQL CLI locally
   $ ./target/debug/influxdb_iox sql

   # Then run the query
   > show namespaces;
   +--------------+---------+
   | namespace_id | name    |
   +--------------+---------+
   | 1            | ns_test |
   +--------------+---------+

   > use namespace ns_test;
   You are now in remote mode, querying namespace ns_test

   ns_test> show tables;
   +---------------+--------------------+-----------------+------------+
   | table_catalog | table_schema       | table_name      | table_type |
   +---------------+--------------------+-----------------+------------+
   | public        | iox                | air_temperature | BASE TABLE |
   | public        | iox                | h2o_temperature | BASE TABLE |
   | public        | iox                | t_test          | BASE TABLE |
   | public        | information_schema | tables          | VIEW       |
   | public        | information_schema | views           | VIEW       |
   | public        | information_schema | columns         | VIEW       |
   | public        | information_schema | df_settings     | VIEW       |
   +---------------+--------------------+-----------------+------------+
   Returned 7 rows in 23.055041ms

   ns_test> select * from air_temperature;
   +--------------+-------------------+-------+------------------------+--------------------------------+
   | location     | sea_level_degrees | state | tenk_feet_feet_degrees | time                           |
   +--------------+-------------------+-------+------------------------+--------------------------------+
   | coyote_creek | 77.2              | CA    | 40.8                   | 1970-01-01T00:00:01.568756160Z |
   | coyote_creek | 77.1              | CA    | 41.0                   | 1970-01-01T00:00:01.600756160Z |
   | santa_monica | 77.3              | CA    | 40.0                   | 1970-01-01T00:00:01.568756160Z |
   | santa_monica | 77.6              | CA    | 40.9                   | 1970-01-01T00:00:01.600756160Z |
   | puget_sound  | 77.5              | WA    | 41.1                   | 1970-01-01T00:00:01.568756160Z |
   | puget_sound  | 78.0              | WA    | 40.9                   | 1970-01-01T00:00:01.600756160Z |
   +--------------+-------------------+-------+------------------------+--------------------------------+
   Returned 6 rows in 27.051917ms
   ```

## How to use option `-p, --partition-template`

Users can override the [default partition key](https://github.com/influxdata/influxdb_iox/blob/4a18df53c6fa28429d051f904343a78aae041e21/data_types/src/partition_template.rs#L253-L262) (`YYYY-MM-DD`) to a customized format when creating a namespace or a table

- Partition template format: `{"parts": [{"timeFormat": "..."}, {"tagValue": "..."}, {"tagValue": "..."}]}`
- `timeFormat` and `tagValue` can be in any order.
- The number of `timeFormat`s and `tagValue`s are not limited at parsing time. However, server limits the total number of them and will send back error if it exceeds the limit 8.
- The value of `timeFormat` and `tagValue` are strings and can be whatever at parsing time. If they are not in the right format the server expcected, the server will return error.

> [!NOTE]
> "time" is a reserved word and cannot be used in `timeFormat` and `tagValue`.

Examples:

```shell
# Create a namespace with a custom partition template
./target/debug/influxdb_iox -vv namespace create ns_test -p '{"parts": [{"tagValue": "col1"}, {"tagValue": "col2"}, {"timeFormat": "%Y-%m"}]}' --host http://localhost:8081

# Create a table with a custom partition template
./target/debug/influxdb_iox -vv table create ns_test t_test -p '{"parts": [{"timeFormat": "%Y-%m"}, {"tagValue": "col1"}]}' --host http://localhost:8081
```

When using the partition template, the following partition key will be derived:

| Partition template                                      | Data shape                                 | Partition key         |
| ------------------------------------------------------- | ------------------------------------------ | --------------------- |
| {"parts":[{"timeFormat":"%Y-%m"}, {"tagValue":"col1"}]} | time=2023-01-01,col1=bananas,col2=plátanos | 2023-01&#124;bananas  |
| {"parts":[{"timeFormat":"%Y-%m"}, {"tagValue":"col2"}]} | time=2023-01-01,col1=bananas,col2=plátanos | 2023-01&#124;plátanos |
| {"parts":[{"tagValue":"col1"}, {"timeFormat":"%Y-%m"}]} | time=2023-01-01,col1=bananas,col2=plátanos | bananas&#124;2023-01  |

A partitioning template is resolved by evaluating the following (in order of precedence):

1.  Table name override, if specified.
2.  Namespace name override, if specified.
3.  Default partitioning scheme (YYYY-MM-DD).

In other words:

- when a namespace is created _with_ a custom partition template, and a table is created implicitly, i.e. _without_ a partition template, the _namespace's_ partition template will be applied to this table.
- when a namespace is created _with_ a custom partition template, and a table is created _without_ a partition template, the _namespace's_ partition template will be applied to this table
- when a namespace is created _with_ a custom partition template, and a table is created _with_ a partition template, the _table's_ partition template will be applied to this table
