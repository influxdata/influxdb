# InfluxDB CLI cookbook

You can use the `influxdb_iox` command line tool to interact with the server in various ways


## Ports

To connect on a different port, use the `--host` argument:

```shell
# connect to localhost on port 8083 (rather than default of 8082)
$ influxdb_iox --host http://localhost:8083  <command>
```

## List all namespaces

```shell
# Connects to port 8082 (gRPC by default)
$ influxdb_iox debug namespace list
[
  {
    "id": "1",
    "name": "26f7e5a4b7be365b_917b97a92e883afc"
  }
]
```

## List Schema in a Namespace

```shell
$ influxdb_iox debug schema get 26f7e5a4b7be365b_917b97a92e883afc
{
  "id": "1",
  "kafkaTopicId": "1",
  "queryPoolId": "1",
  "tables": {
    "mem": {
      "id": "2",
      "columns": {
        "time": {
          "id": "10",
          "columnType": 6
        },
        "host": {
          "id": "16",
          "columnType": 7
        },
        "available": {
          "id": "17",
          "columnType": 1
        },
        "wired": {
...
```

Alternately you can use `show tables` using SQL (see [sql cookbook](sql.md) for more details):

```shell
$ influxdb_iox query 26f7e5a4b7be365b_917b97a92e883afc 'show tables'
+---------------+--------------------+------------+------------+
| table_catalog | table_schema       | table_name | table_type |
+---------------+--------------------+------------+------------+
| public        | iox                | cpu        | BASE TABLE |
| public        | iox                | disk       | BASE TABLE |
| public        | iox                | diskio     | BASE TABLE |
...
| public        | information_schema | columns    | VIEW       |
+---------------+--------------------+------------+------------+
```

## Run Queries

### SQL

```shell
$ influxdb_iox query 26f7e5a4b7be365b_917b97a92e883afc 'select count(*), cpu as cpu_num from cpu group by cpu'
+-----------------+-----------+
| COUNT(UInt8(1)) | cpu_num   |
+-----------------+-----------+
| 43              | cpu-total |
| 43              | cpu0      |
| 43              | cpu1      |
| 43              | cpu10     |
| 43              | cpu11     |
| 43              | cpu12     |
| 43              | cpu13     |
| 43              | cpu14     |
| 43              | cpu15     |
| 43              | cpu2      |
| 43              | cpu3      |
| 43              | cpu4      |
| 43              | cpu5      |
| 43              | cpu6      |
| 43              | cpu7      |
| 43              | cpu8      |
| 43              | cpu9      |
+-----------------+-----------+
```

### InfluxRPC (used by Flux and InfluxQL)

```shell
TODO
```

### Ingester (used internally to IOx to query unpersisted data)

```shell
# Note you need to connect to the ingester (running on port 8083 in all in one mode)
$ influxdb_iox query-ingester --host http://localhost:8083  26f7e5a4b7be365b_917b97a92e883afc mem available_percent | head
+--------------------+
| available_percent  |
+--------------------+
| 56.58011436462402  |
| 57.43834972381592  |
| 57.46076703071594  |
| 57.482320070266724 |
| 57.447218894958496 |
| 57.420217990875244 |
| 57.361191511154175 |
```

### SQL Repl

```shell
$ influxdb_iox sql
Connected to IOx Server
Set output format format to pretty
Ready for commands. (Hint: try 'help;')
> use 26f7e5a4b7be365b_917b97a92e883afc;
You are now in remote mode, querying database 26f7e5a4b7be365b_917b97a92e883afc
26f7e5a4b7be365b_917b97a92e883afc> select count(*) from cpu;
+-----------------+
| COUNT(UInt8(1)) |
+-----------------+
| 1054            |
+-----------------+
Returned 1 row in 59.410821ms
```
