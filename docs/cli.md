# InfluxDB CLI cookbook

You can use the `influxdb_iox` command line tool to interact with the IOx server in various ways. This document contains a brief tour of highlights and detailed information on each command can be found by passing `--help`.


## Ports

To connect on a different port, use the `--host` argument:

```shell
# connect to localhost on port 8083 (rather than default of 8082)
$ influxdb_iox --host http://localhost:8083  <command>
```

## Getting data in to IOx / Loading

You can load data in parallel using `influxdb_iox write` command by specifing one or more files from the command line.

Currently supported formats are
1. `.lp` (line protocol),
3. `.gz` (gzipped line protocol)
2. `.parquet` (IOx created parquet files)

This command uses the http v2 endpoint, which often runs on port 8080, rather than the default 8082 which handles gRPC:

```shell
influxdb_iox -h http://localhost:8080 write test_db test_fixtures/lineproto/*.lp
```

It is also possible to use the influxdb_iox write command to write data to the InfluxDB cloud service. For example, given the following scenario

* Endpoint: https://us-east-1-1.aws.cloud2.influxdata.com
* Organization: `974db1248c9d6846`
* Bucket: `67c03137977b068c`
* Data in local file: `temperature.lp`
* Token in environment variable: `INFLUX_API_TOKEN`

You can write using this command line:

```shell
$ influxdb_iox write  --token "$INFLUX_API_TOKEN"  -h https://us-east-1-1.aws.cloud2.influxdata.com  974db1248c9d6846_67c03137977b068c temperature.lp
```

## Run Queries

### SQL
You can run an individual SQL query using the `query` command and providing the namespace and the SQL text. See the [sql cookbook](sql.md)for more detailed documentation on SQL.

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

### SQL REPL

IOx comes with its own Read Evaluate Print Loop (REPL) for running SQL interactively. See the [sql cookbook](sql.md)for more detailed documentation.

```shell
$ influxdb_iox sql
Connected to IOx Server
Set output format format to pretty
Ready for commands. (Hint: try 'help;')
> use 26f7e5a4b7be365b_917b97a92e883afc;
You are now in remote mode, querying namespace 26f7e5a4b7be365b_917b97a92e883afc
26f7e5a4b7be365b_917b97a92e883afc> select count(*) from cpu;
+-----------------+
| COUNT(UInt8(1)) |
+-----------------+
| 1054            |
+-----------------+
Returned 1 row in 59.410821ms
```

## Getting data out of IOx

## Fetch the parquet files for a particular table

You can retrieve the parquet files used to store a particular table to a local directory

```shell
$ influxdb_iox remote store get-table 26f7e5a4b7be365b_917b97a92e883afc mem
found 3 Parquet files, downloading...
downloading file 1 of 3 (1ce7e327-7b48-478f-b141-96e8d366ca12.5.parquet)...
downloading file 2 of 3 (fa45a0db-5e9e-4374-b3d3-8294b5e7ade0.5.parquet)...
downloading file 3 of 3 (ad5e47f6-b984-400b-99c2-f562151985d6.5.parquet)...
Done.
```

These are standard parquet files and can be read by any other tool that understands the parquet file format.

## Convert parquet files into line protocol

Parquet files created by IOx can be converted back into the Line Protocol format using metadata stored in the file:

```shell
$ influxdb_iox debug parquet-to-lp mem/1ce7e327-7b48-478f-b141-96e8d366ca12.5.parquet
disk,device=disk1s1s1,fstype=apfs,host=MacBook-Pro-8.local,mode=ro,path=/ free=89205854208i,inodes_free=871150920i,inodes_total=871652968i,inodes_used=502048i,total=1000240963584i,used=911035109376i,used_percent=91.0815635975992 1667300090000000000
disk,device=disk1s1,fstype=apfs,host=MacBook-Pro-8.local,mode=rw,path=/System/Volumes/Update/mnt1 free=89205854208i,inodes_free=871150920i,inodes_total=871652990i,inodes_used=502070i,total=1000240963584i,used=911035109376i,used_percent=91.0815635975992 1667300090000000000
...
```

Note you can also write such parquet files that came from IOx to another IOx instance using the `influxdb_iox write` command as described in `Getting data in to IOx` above.

## Inspect The Catalog


## List all namespaces

```shell
# Connects to port 8082 (gRPC by default)
$ influxdb_iox namespace list
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
  "topicId": "1",
  "queryPoolId": "1",
  "tables": {
    "cpu": {
      "id": "5",
      "columns": {
        "host": {
          "id": "56",
          "columnType": "COLUMN_TYPE_TAG"
        },
        "usage_nice": {
          "id": "51",
          "columnType": "COLUMN_TYPE_F64"
        },
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

## Advanced Querying

These CLI options are most often used for developing and debugging IOx rather than intended for end users.

### InfluxRPC (used by Flux and InfluxQL)

`influxrpc` is the name used to describe the protocol to talk with Flux and InfluxQL services. There is limited CLI support for making such queries. For example, to run `measurement-fields` request,

```shell
$ influxdb_iox storage 26f7e5a4b7be365b_917b97a92e883afc measurement-fields cpu

tag values: 10
+----------------------------------------------+
| values                                       |
+----------------------------------------------+
| key: usage_guest, type: 0, timestamp: 0      |
| key: usage_guest_nice, type: 0, timestamp: 0 |
| key: usage_idle, type: 0, timestamp: 0       |
| key: usage_iowait, type: 0, timestamp: 0     |
| key: usage_irq, type: 0, timestamp: 0        |
| key: usage_nice, type: 0, timestamp: 0       |
| key: usage_softirq, type: 0, timestamp: 0    |
| key: usage_steal, type: 0, timestamp: 0      |
| key: usage_system, type: 0, timestamp: 0     |
| key: usage_user, type: 0, timestamp: 0       |
+----------------------------------------------+
```

### Ingester (used internally to IOx to query unpersisted data)

You can make direct queries to the ingester to see its unpersisted data using the `query-ingester` command. Note you need to connect to the ingester (running on port 8083 in all in one mode)

```shell
$ influxdb_iox query-ingester --host http://localhost:8083  26f7e5a4b7be365b_917b97a92e883afc swap
+------------+---------------------+----+-----+----------------------+------------+------------+-------------------+
| free       | host                | in | out | time                 | total      | used       | used_percent      |
+------------+---------------------+----+-----+----------------------+------------+------------+-------------------+
| 1496055808 | MacBook-Pro-8.local |    |     | 2022-11-01T10:08:40Z | 6442450944 | 4946395136 | 76.77815755208334 |
|            | MacBook-Pro-8.local | 0  | 0   | 2022-11-01T10:08:40Z |            |            |                   |
| 1496055808 | MacBook-Pro-8.local |    |     | 2022-11-01T10:08:40Z | 6442450944 | 4946395136 | 76.77815755208334 |
|            | MacBook-Pro-8.local | 0  | 0   | 2022-11-01T10:08:40Z |            |            |                   |
| 1496055808 | MacBook-Pro-8.local |    |     | 2022-11-01T10:08:50Z | 6442450944 | 4946395136 | 76.77815755208334 |
|            | MacBook-Pro-8.local | 0  | 0   | 2022-11-01T10:08:50Z |            |            |                   |
| 1496055808 | MacBook-Pro-8.local |    |     | 2022-11-01T10:08:50Z | 6442450944 | 4946395136 | 76.77815755208334 |
...
```
