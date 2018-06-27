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