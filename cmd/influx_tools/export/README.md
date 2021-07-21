`influx-tools export`
=====================

Used with `influx-tools import`, the export tool transforms existing shards to a new shard duration in order to
consolidate into fewer shards. It is also possible to separate into a greater number of shards.


Field type conflicts
--------------------

A field type for a given measurement can be different per shard. This creates the potential for field type conflicts
when exporting new shard durations. If this happens, the field type will be determined by the first shard containing
data for that field in order to fulfil the target shard duration. All conflicting data will be written as line protocol
and gzipped to the path specified by `-conflict-path`, unless the `-no-conflict-path` option is specified.


Range
-----

The optional `-range <range>` option specifies which target shards should be exported, based on their sequence number.
A use case for the `range` option is to parallelize the reshaping of a large data set. A machine with 4 cores could run two
export / import jobs concurrently over a subset of the total target shards.

The sequence number is included in the plan output. For example:

```sh
$ influx-tools export -config config.toml -database foo -rp autogen -duration 24h -format=binary -no-conflict-path -print-only
Source data from: 2018-02-19 00:00:00 +0000 UTC -> 2018-04-09 00:00:00 +0000 UTC

Converting source from 4 shard group(s) to 28 shard groups

Seq #           ID              Start                           End
0               608             2018-02-19 00:00:00 +0000 UTC   2018-02-26 00:00:00 +0000 UTC
1               609             2018-03-19 00:00:00 +0000 UTC   2018-03-26 00:00:00 +0000 UTC
2               610             2018-03-26 00:00:00 +0000 UTC   2018-04-02 00:00:00 +0000 UTC
3               612             2018-04-02 00:00:00 +0000 UTC   2018-04-09 00:00:00 +0000 UTC

Seq #           ID              Start                           End
0               0               2018-02-19 00:00:00 +0000 UTC   2018-02-20 00:00:00 +0000 UTC
1               1               2018-02-20 00:00:00 +0000 UTC   2018-02-21 00:00:00 +0000 UTC
2               2               2018-02-21 00:00:00 +0000 UTC   2018-02-22 00:00:00 +0000 UTC
3               3               2018-02-22 00:00:00 +0000 UTC   2018-02-23 00:00:00 +0000 UTC
4               4               2018-02-23 00:00:00 +0000 UTC   2018-02-24 00:00:00 +0000 UTC
5               5               2018-02-24 00:00:00 +0000 UTC   2018-02-25 00:00:00 +0000 UTC
6               6               2018-02-25 00:00:00 +0000 UTC   2018-02-26 00:00:00 +0000 UTC
7               28              2018-03-19 00:00:00 +0000 UTC   2018-03-20 00:00:00 +0000 UTC
8               29              2018-03-20 00:00:00 +0000 UTC   2018-03-21 00:00:00 +0000 UTC
9               30              2018-03-21 00:00:00 +0000 UTC   2018-03-22 00:00:00 +0000 UTC
...
26              47              2018-04-07 00:00:00 +0000 UTC   2018-04-08 00:00:00 +0000 UTC
27              48              2018-04-08 00:00:00 +0000 UTC   2018-04-09 00:00:00 +0000 UTC
```

Adding `-range 2-4` would return the following plan:

```sh
$ influx-tools export -config config.toml -database foo -rp autogen -duration 24h -format=binary -no-conflict-path -print-only -range=2-4
Source data from: 2018-02-19 00:00:00 +0000 UTC -> 2018-04-09 00:00:00 +0000 UTC

Converting source from 4 shard group(s) to 3 shard groups

Seq #           ID              Start                           End
0               608             2018-02-19 00:00:00 +0000 UTC   2018-02-26 00:00:00 +0000 UTC
1               609             2018-03-19 00:00:00 +0000 UTC   2018-03-26 00:00:00 +0000 UTC
2               610             2018-03-26 00:00:00 +0000 UTC   2018-04-02 00:00:00 +0000 UTC
3               612             2018-04-02 00:00:00 +0000 UTC   2018-04-09 00:00:00 +0000 UTC

Seq #           ID              Start                           End
2               2               2018-02-21 00:00:00 +0000 UTC   2018-02-22 00:00:00 +0000 UTC
3               3               2018-02-22 00:00:00 +0000 UTC   2018-02-23 00:00:00 +0000 UTC
4               4               2018-02-23 00:00:00 +0000 UTC   2018-02-24 00:00:00 +0000 UTC
```

A range can either be a single sequence number or an interval as shown previously.

**Hint**: Include the `-print-only` option to display the plan and exit without exporting any data.

Example usage for Enterprise
----------------------------

Note: This is only for fully replicated clusters. For non-fully-replicated clusters (RF < # of nodes),
the current best method would be exporting in line protocol and re-importing after deleting the old shards.

This example takes many small shards and collapses them to fewer large shards, which can be beneficial
for TSI index performance.

The `influx_tools export` and `influx_tools import` tools run as if they were a full data node - to run either, the normal
data node `influxd` process must be down, and the meta nodes must be running. Ensure any environment
variables and config file settings for the real `influxd` are reflected in the config file provided to
`influx_tools`

#### Create the export

It is recommended to do this step on a running backup of the cluster rather than the real cluster.

The `-duration` must be an *integer multiple* of the shard group duration being
collapsed, to avoid overlapping shards and import problems. For example, 4032h = 24*168h = 24 weeks.

It is recommended to run a dry run with `-print-only` before running the real export.

On one of the nodes (of the running backup), bring down the influxd process and run:

```sh
$ ./influx_tools export -config /root/.influxdb/influxdb.conf -database collapsedb -duration 4032h -format binary -conflict-path /conflicts.tar.gz -print-only
Source data from: 2020-05-25 00:00:00 +0000 UTC -> 2021-07-19 00:00:00 +0000 UTC

Converting source from 60 shard group(s) to 3 shard groups:

Seq #           ID         Start                                End
0               9          2020-05-25 00:00:00 +0000 UTC        2020-06-01 00:00:00 +0000 UTC
1               10         2020-06-01 00:00:00 +0000 UTC        2020-06-08 00:00:00 +0000 UTC
2               11         2020-06-08 00:00:00 +0000 UTC        2020-06-15 00:00:00 +0000 UTC
3               12         2020-06-15 00:00:00 +0000 UTC        2020-06-22 00:00:00 +0000 UTC
4               13         2020-06-22 00:00:00 +0000 UTC        2020-06-29 00:00:00 +0000 UTC
5               14         2020-06-29 00:00:00 +0000 UTC        2020-07-06 00:00:00 +0000 UTC
…
54              63         2021-06-07 00:00:00 +0000 UTC        2021-06-14 00:00:00 +0000 UTC
55              64         2021-06-14 00:00:00 +0000 UTC        2021-06-21 00:00:00 +0000 UTC
56              65         2021-06-21 00:00:00 +0000 UTC        2021-06-28 00:00:00 +0000 UTC
57              66         2021-06-28 00:00:00 +0000 UTC        2021-07-05 00:00:00 +0000 UTC
58              67         2021-07-05 00:00:00 +0000 UTC        2021-07-12 00:00:00 +0000 UTC
59              68         2021-07-12 00:00:00 +0000 UTC        2021-07-19 00:00:00 +0000 UTC

Seq #           ID         Start                                End
0               0          2020-04-06 00:00:00 +0000 UTC        2020-09-21 00:00:00 +0000 UTC
1               1          2020-09-21 00:00:00 +0000 UTC        2021-03-08 00:00:00 +0000 UTC
2               2          2021-03-08 00:00:00 +0000 UTC        2021-08-23 00:00:00 +0000 UTC
```

To run just one of shard collapses, use the `-range` argument, like `-range '0-0'` (the range is inclusive).
The range refers to the output shard sequence numbers (the first column of the second table in the plan output).

Once happy with the plan, run without `-print-only` (make sure to pipe to a file!):

```sh
$ ./influx_tools export -config /root/.influxdb/influxdb.conf -database collapsedb -duration 4032h -format binary -conflict-path /conflicts.tar.gz -range '0-0' > export.binary
Source data from: 2020-05-25 00:00:00 +0000 UTC -> 2021-07-19 00:00:00 +0000 UTC

Converting source from 60 shard group(s) to 1 shard groups:

Seq #           ID              Start                           End
0               9               2020-05-25 00:00:00 +0000 UTC   2020-06-01 00:00:00 +0000 UTC
1               10              2020-06-01 00:00:00 +0000 UTC   2020-06-08 00:00:00 +0000 UTC
2               11              2020-06-08 00:00:00 +0000 UTC   2020-06-15 00:00:00 +0000 UTC
3               12              2020-06-15 00:00:00 +0000 UTC   2020-06-22 00:00:00 +0000 UTC
…
```

#### Import the previously exported data

Importing the data requires a period of data unavailability.

1) For safety, take a full database backup - see https://docs.influxdata.com/enterprise_influxdb/v1.9/administration/backup-and-restore/
2) With all nodes running, delete *all* the shards that would overlap with the newly created shard(s):
```
    i) The output from the export plan (the ‘ID’ column) gives the shard group id’s:

> ./influx_tools export ...
...
Seq #           ID              Start                           End
0               9               2020-05-25 00:00:00 +0000 UTC   2020-06-01 00:00:00 +0000 UTC
                ^
                Shard group ID


    ii) The 'SHOW SHARDS' command gives the shard id's:

> influx -execute 'show shards'
name: _internal
id database  retention_policy shard_group start_time           end_time             expiry_time          owners
-- --------  ---------------- ----------- ----------           --------             -----------          ------
1  _internal monitor          1           2021-07-15T00:00:00Z 2021-07-16T00:00:00Z 2021-07-23T00:00:00Z 5
2  _internal monitor          1           2021-07-15T00:00:00Z 2021-07-16T00:00:00Z 2021-07-23T00:00:00Z 4
14 _internal monitor          8           2021-07-16T00:00:00Z 2021-07-17T00:00:00Z 2021-07-24T00:00:00Z 5
15 _internal monitor          8           2021-07-16T00:00:00Z 2021-07-17T00:00:00Z 2021-07-24T00:00:00Z 4
^                             ^
Shard id                      Shard group id

    iii) The influxql 'DROP SHARD' command must be used to drop each old shard id to be replaced.
> influx -execute 'drop shard 14'
```

3) Take one node down, and use `influx_tools import` on that node to import the data, similar to how `influx_tools export`
   was used on the running backup . Note `-shard-duration` is actually the shard group duration, and `duration`
   should match the existing time to expiry (default is infinite). If importing to an existing retention policy, it must have a matching
   shard group duration, shard duration, and replication factor (otherwise you will get an error `import: retention policy
   autogen already exists with different parameters`). Use `alter retention policy ...` to fix the existing policy if necessary.
```sh
$ ./influx_tools import -config /root/.influxdb/influxdb.conf -database collapsedb -rp autogen -shard-duration 4032h < export.binary
{"level":"info","ts":1626446664.3253446,"caller":"importer/command.go:64","msg":"import starting"}
{"level":"info","ts":1626446664.3271663,"caller":"importer/importer.go:107","msg":"Starting shard group 2020-04-06 00:00:00 +0000 UTC-2020-09-21 00:00:00 +0000 UTC with 0 existing shard groups"}
```

4) Bring the node back up, and use `influxd-ctl copy-shard` to copy the new shard(s) to the other cluster node:
```sh
$ ./influxd-ctl copy-shard latest_data_0_1:8088 latest_data_1_1:8088 71
Copied shard 71 from latest_data_0_1:8088 to latest_data_1_1:8088
```


