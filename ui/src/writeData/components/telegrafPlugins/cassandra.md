
# Telegraf plugin: Cassandra

### **Deprecated in version 1.7**: Please use the [jolokia2](https://github.com/influxdata/telegraf/tree/master/plugins/inputs/jolokia2) plugin with the [cassandra.conf](/plugins/inputs/jolokia2/examples/cassandra.conf) example configuration.

#### Plugin arguments:
- **context** string: Context root used for jolokia url
- **servers** []string: List of servers with the format "<user:passwd@><host>:port"
- **metrics** []string: List of Jmx paths that identify mbeans attributes

#### Description

The Cassandra plugin collects Cassandra 3 / JVM metrics exposed as MBean's attributes through jolokia REST endpoint. All metrics are collected for each server configured.

See: https://jolokia.org/ and [Cassandra Documentation](http://docs.datastax.com/en/cassandra/3.x/cassandra/operations/monitoringCassandraTOC.html)

# Measurements:
Cassandra plugin produces one or more measurements for each metric configured, adding Server's name  as `host` tag. More than one measurement is generated when querying table metrics with a wildcard for the keyspace or table name.

Given a configuration like:

```toml
[[inputs.cassandra]]
  context = "/jolokia/read"
  servers = [":8778"]
  metrics = ["/java.lang:type=Memory/HeapMemoryUsage"]
```

The collected metrics will be:

```
javaMemory,host=myHost,mname=HeapMemoryUsage HeapMemoryUsage_committed=1040187392,HeapMemoryUsage_init=1050673152,HeapMemoryUsage_max=1040187392,HeapMemoryUsage_used=368155000 1459551767230567084
```

# Useful Metrics:

Here is a list of metrics that might be useful to monitor your cassandra cluster. This was put together from multiple sources on the web.

- [How to monitor Cassandra performance metrics](https://www.datadoghq.com/blog/how-to-monitor-cassandra-performance-metrics)
- [Cassandra Documentation](http://docs.datastax.com/en/cassandra/3.x/cassandra/operations/monitoringCassandraTOC.html)

#### measurement = javaGarbageCollector

- /java.lang:type=GarbageCollector,name=ConcurrentMarkSweep/CollectionTime
- /java.lang:type=GarbageCollector,name=ConcurrentMarkSweep/CollectionCount
- /java.lang:type=GarbageCollector,name=ParNew/CollectionTime
- /java.lang:type=GarbageCollector,name=ParNew/CollectionCount

#### measurement = javaMemory

- /java.lang:type=Memory/HeapMemoryUsage
- /java.lang:type=Memory/NonHeapMemoryUsage

#### measurement = cassandraCache

- /org.apache.cassandra.metrics:type=Cache,scope=KeyCache,name=Hits
- /org.apache.cassandra.metrics:type=Cache,scope=KeyCache,name=Requests
- /org.apache.cassandra.metrics:type=Cache,scope=KeyCache,name=Entries
- /org.apache.cassandra.metrics:type=Cache,scope=KeyCache,name=Size
- /org.apache.cassandra.metrics:type=Cache,scope=KeyCache,name=Capacity
- /org.apache.cassandra.metrics:type=Cache,scope=RowCache,name=Hits
- /org.apache.cassandra.metrics:type=Cache,scope=RowCache,name=Requests
- /org.apache.cassandra.metrics:type=Cache,scope=RowCache,name=Entries
- /org.apache.cassandra.metrics:type=Cache,scope=RowCache,name=Size
- /org.apache.cassandra.metrics:type=Cache,scope=RowCache,name=Capacity

#### measurement = cassandraClient

- /org.apache.cassandra.metrics:type=Client,name=connectedNativeClients

#### measurement = cassandraClientRequest

- /org.apache.cassandra.metrics:type=ClientRequest,scope=Read,name=TotalLatency
- /org.apache.cassandra.metrics:type=ClientRequest,scope=Write,name=TotalLatency
- /org.apache.cassandra.metrics:type=ClientRequest,scope=Read,name=Latency
- /org.apache.cassandra.metrics:type=ClientRequest,scope=Write,name=Latency
- /org.apache.cassandra.metrics:type=ClientRequest,scope=Read,name=Timeouts
- /org.apache.cassandra.metrics:type=ClientRequest,scope=Write,name=Timeouts
- /org.apache.cassandra.metrics:type=ClientRequest,scope=Read,name=Unavailables
- /org.apache.cassandra.metrics:type=ClientRequest,scope=Write,name=Unavailables
- /org.apache.cassandra.metrics:type=ClientRequest,scope=Read,name=Failures
- /org.apache.cassandra.metrics:type=ClientRequest,scope=Write,name=Failures

#### measurement = cassandraCommitLog

- /org.apache.cassandra.metrics:type=CommitLog,name=PendingTasks
- /org.apache.cassandra.metrics:type=CommitLog,name=TotalCommitLogSize

#### measurement = cassandraCompaction

- /org.apache.cassandra.metrics:type=Compaction,name=CompletedTasks
- /org.apache.cassandra.metrics:type=Compaction,name=PendingTasks
- /org.apache.cassandra.metrics:type=Compaction,name=TotalCompactionsCompleted
- /org.apache.cassandra.metrics:type=Compaction,name=BytesCompacted

#### measurement = cassandraStorage

- /org.apache.cassandra.metrics:type=Storage,name=Load
- /org.apache.cassandra.metrics:type=Storage,name=Exceptions

#### measurement = cassandraTable
Using wildcards for "keyspace" and "scope" can create a lot of series as metrics will be reported for every table and keyspace including internal system tables. Specify a keyspace name and/or a table name to limit them.

- /org.apache.cassandra.metrics:type=Table,keyspace=\*,scope=\*,name=LiveDiskSpaceUsed
- /org.apache.cassandra.metrics:type=Table,keyspace=\*,scope=\*,name=TotalDiskSpaceUsed
- /org.apache.cassandra.metrics:type=Table,keyspace=\*,scope=\*,name=ReadLatency
- /org.apache.cassandra.metrics:type=Table,keyspace=\*,scope=\*,name=CoordinatorReadLatency
- /org.apache.cassandra.metrics:type=Table,keyspace=\*,scope=\*,name=WriteLatency
- /org.apache.cassandra.metrics:type=Table,keyspace=\*,scope=\*,name=ReadTotalLatency
- /org.apache.cassandra.metrics:type=Table,keyspace=\*,scope=\*,name=WriteTotalLatency


#### measurement = cassandraThreadPools

- /org.apache.cassandra.metrics:type=ThreadPools,path=internal,scope=CompactionExecutor,name=ActiveTasks
- /org.apache.cassandra.metrics:type=ThreadPools,path=internal,scope=AntiEntropyStage,name=ActiveTasks
-  /org.apache.cassandra.metrics:type=ThreadPools,path=request,scope=CounterMutationStage,name=PendingTasks
- /org.apache.cassandra.metrics:type=ThreadPools,path=request,scope=CounterMutationStage,name=CurrentlyBlockedTasks        
- /org.apache.cassandra.metrics:type=ThreadPools,path=request,scope=MutationStage,name=PendingTasks
- /org.apache.cassandra.metrics:type=ThreadPools,path=request,scope=MutationStage,name=CurrentlyBlockedTasks
- /org.apache.cassandra.metrics:type=ThreadPools,path=request,scope=ReadRepairStage,name=PendingTasks
- /org.apache.cassandra.metrics:type=ThreadPools,path=request,scope=ReadRepairStage,name=CurrentlyBlockedTasks
- /org.apache.cassandra.metrics:type=ThreadPools,path=request,scope=ReadStage,name=PendingTasks
- /org.apache.cassandra.metrics:type=ThreadPools,path=request,scope=ReadStage,name=CurrentlyBlockedTasks
-  /org.apache.cassandra.metrics:type=ThreadPools,path=request,scope=RequestResponseStage,name=PendingTasks        
- /org.apache.cassandra.metrics:type=ThreadPools,path=request,scope=RequestResponseStage,name=CurrentlyBlockedTasks


