# How to Reproduce and Debug Production Data Locally

Here is a way to reproduce issues using production data locally with all in one mode.

## Summary of steps

Reproduce the error locally by building a local catalog from the output of `influxdb_iox remote store get-table`:

1. Download contents of table_name into a directory named 'table_name'

   ```
   influxdb_iox remote store get-table <namespace> <table_name>
   ```

1. Create a catalog and object_store in /tmp/data_dir

   ```
   influxdb_iox debug build-catalog <table_dir> /tmp/data_dir
   ```

1. Start iox using this data directory (you can now query `table_name` locally):
   ```
   influxdb_iox --data-dir /tmp/data_dir
   ```

## Demonstration

## Setup

Running `influxdb_iox` and getting local telegraph data

```shell
$ influxdb_iox namespace list
[
  {
    "id": "1",
    "name": "26f7e5a4b7be365b_917b97a92e883afc",
    "maxTables": 500,
    "maxColumnsPerTable": 200
  }
]
```

## Export `cpu` table:

```shell
$ influxdb_iox remote store get-table 26f7e5a4b7be365b_917b97a92e883afc cpu
found 11 Parquet files, exporting...
downloading file 1 of 11 (1b8eb36a-7a34-4635-9156-251efcb1c024.4.parquet)...
downloading file 2 of 11 (1819137f-7cb5-4dc8-8051-6fa0b42990cb.4.parquet)...
downloading file 3 of 11 (4931cad7-7aaf-4b41-8f46-2d3be85c492b.4.parquet)...
downloading file 4 of 11 (be75f5fb-a8bc-4646-893a-70d496b13f3d.4.parquet)...
downloading file 5 of 11 (5235b87d-19ee-48ae-830f-b19d81bfe915.4.parquet)...
downloading file 6 of 11 (a8f7be33-42b6-4353-8735-51b245196d39.4.parquet)...
downloading file 7 of 11 (3b43c4ee-7500-47f9-9c0f-76d4f80b480e.4.parquet)...
downloading file 8 of 11 (081da5be-e0f9-4b42-8cd2-45bfebbd934c.4.parquet)...
downloading file 9 of 11 (f29ba3b4-53b1-4c68-9287-4bcea7c4e86b.4.parquet)...
downloading file 10 of 11 (1ce94ce2-1200-4516-950a-64828a7cebba.4.parquet)...
downloading file 11 of 11 (3a2b5525-3be5-41ef-b082-b279edc32acb.4.parquet)...
Done.
$ ls cpu/
081da5be-e0f9-4b42-8cd2-45bfebbd934c.4.parquet       1ce94ce2-1200-4516-950a-64828a7cebba.4.parquet       4931cad7-7aaf-4b41-8f46-2d3be85c492b.4.parquet       be75f5fb-a8bc-4646-893a-70d496b13f3d.4.parquet
081da5be-e0f9-4b42-8cd2-45bfebbd934c.4.parquet.json  1ce94ce2-1200-4516-950a-64828a7cebba.4.parquet.json  4931cad7-7aaf-4b41-8f46-2d3be85c492b.4.parquet.json  be75f5fb-a8bc-4646-893a-70d496b13f3d.4.parquet.json
1819137f-7cb5-4dc8-8051-6fa0b42990cb.4.parquet       3a2b5525-3be5-41ef-b082-b279edc32acb.4.parquet       5235b87d-19ee-48ae-830f-b19d81bfe915.4.parquet       f29ba3b4-53b1-4c68-9287-4bcea7c4e86b.4.parquet
1819137f-7cb5-4dc8-8051-6fa0b42990cb.4.parquet.json  3a2b5525-3be5-41ef-b082-b279edc32acb.4.parquet.json  5235b87d-19ee-48ae-830f-b19d81bfe915.4.parquet.json  f29ba3b4-53b1-4c68-9287-4bcea7c4e86b.4.parquet.json
1b8eb36a-7a34-4635-9156-251efcb1c024.4.parquet       3b43c4ee-7500-47f9-9c0f-76d4f80b480e.4.parquet       a8f7be33-42b6-4353-8735-51b245196d39.4.parquet       partition.4.json
1b8eb36a-7a34-4635-9156-251efcb1c024.4.parquet.json  3b43c4ee-7500-47f9-9c0f-76d4f80b480e.4.parquet.json  a8f7be33-42b6-4353-8735-51b245196d39.4.parquet.json  table.1.json
```

## Build a new `new_data_dir` from export:

```shell
$ influxdb_iox debug build-catalog cpu new_data_dir
Beginning catalog / object_store build from "cpu" in "new_data_dir"....
Done

$ ls new_data_dir/
catalog.sqlite  object_store/
```

## Run `influxdb_iox` with `new_data_dir`:

```shell
$ influxdb_iox --data-dir new_data_dir/
```

And in a separate shell, you can query the data and see it is present:

```shell
$ influxdb_iox query 26f7e5a4b7be365b_917b97a92e883afc 'select * from cpu limit 10';
+-----------+---------------------+----------------------+-------------+------------------+-------------------+--------------+-----------+------------+---------------+-------------+--------------------+--------------------+
| cpu       | host                | time                 | usage_guest | usage_guest_nice | usage_idle        | usage_iowait | usage_irq | usage_nice | usage_softirq | usage_steal | usage_system       | usage_user         |
+-----------+---------------------+----------------------+-------------+------------------+-------------------+--------------+-----------+------------+---------------+-------------+--------------------+--------------------+
| cpu-total | MacBook-Pro-8.local | 2023-07-06T17:13:40Z | 0.0         | 0.0              | 95.6668753914105  | 0.0          | 0.0       | 0.0        | 0.0           | 0.0         | 1.4902943018170824 | 2.8428303068453085 |
| cpu-total | MacBook-Pro-8.local | 2023-07-06T17:13:50Z | 0.0         | 0.0              | 95.9551687433697  | 0.0          | 0.0       | 0.0        | 0.0           | 0.0         | 1.4213261536472683 | 2.6235051029648098 |
| cpu-total | MacBook-Pro-8.local | 2023-07-06T17:14:00Z | 0.0         | 0.0              | 96.52108622167991 | 0.0          | 0.0       | 0.0        | 0.0           | 0.0         | 1.37029157802418   | 2.108622199968126  |
| cpu-total | MacBook-Pro-8.local | 2023-07-06T17:14:10Z | 0.0         | 0.0              | 95.26819803491809 | 0.0          | 0.0       | 0.0        | 0.0           | 0.0         | 1.752519246414341  | 2.979282718922596  |
| cpu-total | MacBook-Pro-8.local | 2023-07-06T17:14:20Z | 0.0         | 0.0              | 95.28402329791422 | 0.0          | 0.0       | 0.0        | 0.0           | 0.0         | 1.6408843239063593 | 3.0750923780335997 |
| cpu-total | MacBook-Pro-8.local | 2023-07-06T17:14:30Z | 0.0         | 0.0              | 93.97484827633119 | 0.0          | 0.0       | 0.0        | 0.0           | 0.0         | 2.0271538509716924 | 3.9979978727699588 |
| cpu-total | MacBook-Pro-8.local | 2023-07-06T17:14:40Z | 0.0         | 0.0              | 95.69219209824692 | 0.0          | 0.0       | 0.0        | 0.0           | 0.0         | 1.458894245831095  | 2.848913656031324  |
| cpu-total | MacBook-Pro-8.local | 2023-07-06T17:14:50Z | 0.0         | 0.0              | 94.78402607970591 | 0.0          | 0.0       | 0.0        | 0.0           | 0.0         | 1.9685286188771443 | 3.2474453011797517 |
| cpu-total | MacBook-Pro-8.local | 2023-07-06T17:15:00Z | 0.0         | 0.0              | 95.85132344665212 | 0.0          | 0.0       | 0.0        | 0.0           | 0.0         | 1.5706151054475623 | 2.5780614479731607 |
| cpu0      | MacBook-Pro-8.local | 2023-07-06T17:13:40Z | 0.0         | 0.0              | 78.65055387717186 | 0.0          | 0.0       | 0.0        | 0.0           | 0.0         | 7.452165156077374  | 13.897280966824042 |
+-----------+---------------------+----------------------+-------------+------------------+-------------------+--------------+-----------+------------+---------------+-------------+--------------------+--------------------+
```
