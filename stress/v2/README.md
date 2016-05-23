# Influx Stress tool

Blockers to finishing:
* Finalize reporting
  - Decide on how to incorporate TestName (db[difficult], measurement[refactor], tag[easy])
  - Get feedback on reporting syntax
  - Pull addition data from queries
* Documentation is sorely lacking. 
  - Parser behavior and proper `.iql` syntax
  - How the templated query generation works
  - Collection of tested `.iql` files to simulate different loads
  
Commune is potentially blocking writes, look into performance

This stress tool works from list of InfluxQL-esque statements. The language has been extended to allow for some basic templating of fields, tags and measurements in both line protocol and query statements.

By default the test outputs a human readable report to `STDOUT` and records test statistics in an active installation of InfluxDB at `localhost:8086`.

To set state variables for the test such as the address of the Influx node use the following syntax:

```
# Pipe delineated list of addresses. For cluster: [192.168.0.10:8086|192.168.0.2:8086|192.168.0.3:8086]
# Queries currently hit only the first node in a list. Writes are round robin.
SET Addresses [localhost:8086]

# Influx instance to store results
SET ResultsAddress [localhost:8086]

# Database to target for queries and writes. Works like the InfluxCLI USE
SET Database [thing2]

# Precision for the data being written
SET Precision [s]

# Date the first written point will be timestamped
SET StartDate [2016-01-01]

# Size of batches to send to InfluxDB
SET BatchSize [5000]

# Time to wait between sending batches
SET WriteInterval [0s]

# Time to wait between sending queries
SET QueryInterval [0s]

# Number of concurrent writers
SET WriteConcurrency [15]

# Number of concurrent readers
SET QueryConcurrency [5]
```

The values in the example are also the defaults.

Valid line protocol will be forwarded right to the server making setting up your testing environment easy:

```
CREATE DATABASE thing

ALTER RETENTION POLICY default ON thing DURATION 1h REPLICATION 1

SET database [thing]
```

You can write points like this:
```
INSERT mockCpu
cpu,
host=server-[int rand(100) 10000],location=[string rand(8) 1000]
value=[float inc(0) 0]
100000 10s

Explained:

# INSERT keyword kicks off the statement, next to it is the name of the statement for reporting and templated query generation
INSERT mockCpu
# Measurement
cpu,
# Tags - separated by commas. Tag values can be templates, mixed template and fixed values
host=server-[float rand(100) 10000],location=[int inc(0) 1000],fixed=[fix|fid|dor|pom|another_tag_value]
# Fields - separated by commas either templates, mixed template and fixed values
value=[float inc(0) 0]
# 'Timestamp' - Number of points to insert into this measurement and the amount of time between points
100000 10s
```

Each template contains 3 parts: a datatype (`str`, `float`, or `int`) a function which describes how the value changes between points: `inc(0)` is increasing and `rand(n)` is a random number between `0` and `n`. The last number is the number of unique values in the tag or field. `0` is unbounded. To make a tag

To run multiple insert statements at once:
```
GO INSERT devices
devices,
city=[str rand(8) 10],country=[str rand(8) 25],device_id=[str rand(10) 1000]
lat=[float rand(90) 0],lng=[float rand(120) 0],temp=[float rand(40) 0]
10000000 10s

GO INSERT devices2
devices2,
city=[str rand(8) 10],country=[str rand(8) 25],device_id=[str rand(10) 1000]
lat=[float rand(90) 0],lng=[float rand(120) 0],temp=[float rand(40) 0]
10000000 10s

WAIT
```

Fastest point generation and write load requires 3-4 running `GO INSERT` statements at a time.

This tool is still under active development and has rough edges. Work is continuing.

### Output:
Output for config file in this repo:
```
[√] "CREATE DATABASE IF NOT EXISTS thing" -> 1.806785ms
[√] "CREATE DATABASE IF NOT EXISTS thing2" -> 1.492504ms
SET Database = 'thing'
SET Precision = 's'
Go Write Statement:                    mockCpu
  Points/Sec:                          245997
  Resp Time Average:                   173.354445ms
  Resp Time Standard Deviation:        123.80344ms
  95th Percentile Write Response:      381.363503ms
  Average Request Bytes:               276110
  Successful Write Reqs:               20
  Retries:                             0
Go Query Statement:                    mockCpu
  Resp Time Average:                   3.140803ms
  Resp Time Standard Deviation:        2.292328ms
  95th Percentile Read Response:       5.915437ms
  Query Resp Bytes Average:            16 bytes
  Successful Queries:                  10
WAIT -> 406.400059ms
SET DATABASE = 'thing2'
Go Write Statement:                    devices
  Points/Sec:                          163348
  Resp Time Average:                   132.553789ms
  Resp Time Standard Deviation:        149.397972ms
  95th Percentile Write Response:      567.987467ms
  Average Request Bytes:               459999
  Successful Write Reqs:               20
  Retries:                             0
Go Write Statement:                    devices2
  Points/Sec:                          160078
  Resp Time Average:                   133.303097ms
  Resp Time Standard Deviation:        144.352404ms
  95th Percentile Write Response:      560.565066ms
  Average Request Bytes:               464999
  Successful Write Reqs:               20
  Retries:                             0
Go Query Statement:                    fooName
  Resp Time Average:                   1.3307ms
  Resp Time Standard Deviation:        640.249µs
  95th Percentile Read Response:       2.668ms
  Query Resp Bytes Average:            16 bytes
  Successful Queries:                  10
WAIT -> 624.585319ms
[√] "DROP DATABASE thing" -> 991.088464ms
[√] "DROP DATABASE thing2" -> 421.362831ms
```
