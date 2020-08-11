# InfluxDB Input Plugin

The InfluxDB plugin will collect metrics on the given InfluxDB servers.

This plugin can also gather metrics from endpoints that expose
InfluxDB-formatted endpoints. See below for more information.

### Configuration:

```toml
# Read InfluxDB-formatted JSON metrics from one or more HTTP endpoints
[[inputs.influxdb]]
  ## Works with InfluxDB debug endpoints out of the box,
  ## but other services can use this format too.
  ## See the influxdb plugin's README for more details.

  ## Multiple URLs from which to read InfluxDB-formatted JSON
  ## Default is "http://localhost:8086/debug/vars".
  urls = [
    "http://localhost:8086/debug/vars"
  ]

  ## Username and password to send using HTTP Basic Authentication.
  # username = ""
  # password = ""

  ## Optional TLS Config
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = false

  ## http request & header timeout
  timeout = "5s"
```

### Measurements & Fields

**Note:** The measurements and fields included in this plugin are dynamically built from the InfluxDB source, and may vary between versions:

- influxdb
  - n_shards: The total number of shards in the specified database.
- influxdb_database: The database metrics are being collected from.
- influxdb_httpd: The URL to listen for network requests. By default, `http://localhost:8086/debug/var`.
- influxdb_measurement: The measurement that metrics are collected from.
- influxdb_memstats: Statistics about the memory allocator in the specified database.
  - heap_inuse: The number of bytes in in-use spans.
  - heap_released: The number of bytes of physical memory returned to the OS.
  - mspan_inuse: The number of bytes in in-use mspans.
  - total_alloc: The cumulative bytes allocated for heap objects.
  - sys: The total number of bytes of memory obtained from the OS. Measures the virtual address space reserved by the Go runtime for the heap, stacks, and other internal data structures.
  - mallocs: The total number of heap objects allocated. (The total number of live objects are frees.)
  - frees: The cumulative number of freed (live) heap objects.
  - heap_idle: The number of bytes of idle heap objects.
  - pause_total_ns: The total time garbage collection cycles are paused in nanoseconds.
  - lookups: The number of pointer lookups performed by the runtime. Primarily useful for debugging runtime internals.
  - heap_sys: The number of bytes of heap memory obtained from the OS. Measures the amount of virtual address space reserved for the heap.
  - mcache_sys: The bytes of memory obtained from the OS for mcache structures.
  - next_gc: The target heap size of the next garbage collection cycle.
  - gc_cpu_fraction: The fraction of CPU time used by the garbage collection cycle.
  - other_sys: The number of bytes of memory used other than heap_sys, stacks_sys, mspan_sys, mcache_sys, buckhash_sys, and gc_sys.
  - alloc: The currently allocated number of bytes of heap objects.
  - stack_inuse: The number of bytes in in-use stacks.
  - stack_sys: The total number of bytes of memory obtained from the stack in use.
  - buck_hash_sys: The bytes of memory in profiling bucket hash tables.
  - gc_sys: The bytes of memory in garbage collection metadata.
  - num_gc: The number of completed garbage collection cycles.
  - heap_alloc: The size, in bytes, of all heap objects.
  - heap_objects: The number of allocated heap objects.
  - mspan_sys: The bytes of memory obtained from the OS for mspan.
  - mcache_inuse: The bytes of allocated mcache structures.
  - last_gc: Time the last garbage collection finished, as nanoseconds since 1970 (the UNIX epoch).
- influxdb_shard: The shard metrics are collected from.
- influxdb_subscriber: The InfluxDB subscription that metrics are collected from.
- influxdb_tsm1_cache: The TSM cache that metrics are collected from.
- influxdb_tsm1_wal: The TSM Write Ahead Log (WAL) that metrics are collected from.
- influxdb_write: The total writes to the specified database.

### Example Output:

```
telegraf --config ~/ws/telegraf.conf --input-filter influxdb --test
* Plugin: influxdb, Collection 1
> influxdb_database,database=_internal,host=tyrion,url=http://localhost:8086/debug/vars numMeasurements=10,numSeries=29 1463590500247354636
> influxdb_httpd,bind=:8086,host=tyrion,url=http://localhost:8086/debug/vars req=7,reqActive=1,reqDurationNs=14227734 1463590500247354636
> influxdb_measurement,database=_internal,host=tyrion,measurement=database,url=http://localhost:8086/debug/vars numSeries=1 1463590500247354636
> influxdb_measurement,database=_internal,host=tyrion,measurement=httpd,url=http://localhost:8086/debug/vars numSeries=1 1463590500247354636
> influxdb_measurement,database=_internal,host=tyrion,measurement=measurement,url=http://localhost:8086/debug/vars numSeries=10 1463590500247354636
> influxdb_measurement,database=_internal,host=tyrion,measurement=runtime,url=http://localhost:8086/debug/vars numSeries=1 1463590500247354636
> influxdb_measurement,database=_internal,host=tyrion,measurement=shard,url=http://localhost:8086/debug/vars numSeries=4 1463590500247354636
> influxdb_measurement,database=_internal,host=tyrion,measurement=subscriber,url=http://localhost:8086/debug/vars numSeries=1 1463590500247354636
> influxdb_measurement,database=_internal,host=tyrion,measurement=tsm1_cache,url=http://localhost:8086/debug/vars numSeries=4 1463590500247354636
> influxdb_measurement,database=_internal,host=tyrion,measurement=tsm1_filestore,url=http://localhost:8086/debug/vars numSeries=2 1463590500247354636
> influxdb_measurement,database=_internal,host=tyrion,measurement=tsm1_wal,url=http://localhost:8086/debug/vars numSeries=4 1463590500247354636
> influxdb_measurement,database=_internal,host=tyrion,measurement=write,url=http://localhost:8086/debug/vars numSeries=1 1463590500247354636
> influxdb_memstats,host=tyrion,url=http://localhost:8086/debug/vars alloc=7642384i,buck_hash_sys=1463471i,frees=1169558i,gc_sys=653312i,gc_cpu_fraction=0.00003825652361068311,heap_alloc=7642384i,heap_idle=9912320i,heap_inuse=9125888i,heap_objects=48276i,heap_released=0i,heap_sys=19038208i,last_gc=1463590480877651621i,lookups=90i,mallocs=1217834i,mcache_inuse=4800i,mcache_sys=16384i,mspan_inuse=70920i,mspan_sys=81920i,next_gc=11679787i,num_gc=141i,other_sys=1244233i,pause_total_ns=24034027i,stack_inuse=884736i,stack_sys=884736i,sys=23382264i,total_alloc=679012200i 1463590500277918755
> influxdb_shard,database=_internal,engine=tsm1,host=tyrion,id=4,path=/Users/sparrc/.influxdb/data/_internal/monitor/4,retentionPolicy=monitor,url=http://localhost:8086/debug/vars fieldsCreate=65,seriesCreate=26,writePointsOk=7274,writeReq=280 1463590500247354636
> influxdb_subscriber,host=tyrion,url=http://localhost:8086/debug/vars pointsWritten=7274 1463590500247354636
> influxdb_tsm1_cache,database=_internal,host=tyrion,path=/Users/sparrc/.influxdb/data/_internal/monitor/1,retentionPolicy=monitor,url=http://localhost:8086/debug/vars WALCompactionTimeMs=0,cacheAgeMs=2809192,cachedBytes=0,diskBytes=0,memBytes=0,snapshotCount=0 1463590500247354636
> influxdb_tsm1_cache,database=_internal,host=tyrion,path=/Users/sparrc/.influxdb/data/_internal/monitor/2,retentionPolicy=monitor,url=http://localhost:8086/debug/vars WALCompactionTimeMs=0,cacheAgeMs=2809184,cachedBytes=0,diskBytes=0,memBytes=0,snapshotCount=0 1463590500247354636
> influxdb_tsm1_cache,database=_internal,host=tyrion,path=/Users/sparrc/.influxdb/data/_internal/monitor/3,retentionPolicy=monitor,url=http://localhost:8086/debug/vars WALCompactionTimeMs=0,cacheAgeMs=2809180,cachedBytes=0,diskBytes=0,memBytes=42368,snapshotCount=0 1463590500247354636
> influxdb_tsm1_cache,database=_internal,host=tyrion,path=/Users/sparrc/.influxdb/data/_internal/monitor/4,retentionPolicy=monitor,url=http://localhost:8086/debug/vars WALCompactionTimeMs=0,cacheAgeMs=2799155,cachedBytes=0,diskBytes=0,memBytes=331216,snapshotCount=0 1463590500247354636
> influxdb_tsm1_filestore,database=_internal,host=tyrion,path=/Users/sparrc/.influxdb/data/_internal/monitor/1,retentionPolicy=monitor,url=http://localhost:8086/debug/vars diskBytes=37892 1463590500247354636
> influxdb_tsm1_filestore,database=_internal,host=tyrion,path=/Users/sparrc/.influxdb/data/_internal/monitor/2,retentionPolicy=monitor,url=http://localhost:8086/debug/vars diskBytes=52907 1463590500247354636
> influxdb_tsm1_wal,database=_internal,host=tyrion,path=/Users/sparrc/.influxdb/wal/_internal/monitor/1,retentionPolicy=monitor,url=http://localhost:8086/debug/vars currentSegmentDiskBytes=0,oldSegmentsDiskBytes=0 1463590500247354636
> influxdb_tsm1_wal,database=_internal,host=tyrion,path=/Users/sparrc/.influxdb/wal/_internal/monitor/2,retentionPolicy=monitor,url=http://localhost:8086/debug/vars currentSegmentDiskBytes=0,oldSegmentsDiskBytes=0 1463590500247354636
> influxdb_tsm1_wal,database=_internal,host=tyrion,path=/Users/sparrc/.influxdb/wal/_internal/monitor/3,retentionPolicy=monitor,url=http://localhost:8086/debug/vars currentSegmentDiskBytes=0,oldSegmentsDiskBytes=65651 1463590500247354636
> influxdb_tsm1_wal,database=_internal,host=tyrion,path=/Users/sparrc/.influxdb/wal/_internal/monitor/4,retentionPolicy=monitor,url=http://localhost:8086/debug/vars currentSegmentDiskBytes=495687,oldSegmentsDiskBytes=0 1463590500247354636
> influxdb_write,host=tyrion,url=http://localhost:8086/debug/vars pointReq=7274,pointReqLocal=7274,req=280,subWriteOk=280,writeOk=280 1463590500247354636
> influxdb_shard,host=tyrion n_shards=4i 1463590500247354636
```

### InfluxDB-formatted endpoints

The influxdb plugin can collect InfluxDB-formatted data from JSON endpoints.
Whether associated with an Influx database or not.

With a configuration of:

```toml
[[inputs.influxdb]]
  urls = [
    "http://127.0.0.1:8086/debug/vars",
    "http://192.168.2.1:8086/debug/vars"
  ]
```
