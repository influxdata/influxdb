# SQL Server Input Plugin
The `sqlserver` plugin provides metrics for your SQL Server instance. It
currently works with SQL Server 2008 SP3 and newer. Recorded metrics are
lightweight and use Dynamic Management Views supplied by SQL Server.

### The SQL Server plugin supports the following editions/versions of SQL Server
- SQL Server
  - 2008 SP3 (with CU3)
  - SQL Server 2008 R2 SP3 and newer versions
- Azure SQL Database (Single)
- Azure SQL Managed Instance

### Additional Setup:

You have to create a login on every SQL Server instance  or Azure SQL Managed instance you want to monitor, with following script:
```sql
USE master;
GO
CREATE LOGIN [telegraf] WITH PASSWORD = N'mystrongpassword';
GO
GRANT VIEW SERVER STATE TO [telegraf];
GO
GRANT VIEW ANY DEFINITION TO [telegraf];
GO
```

For Azure SQL Database, you require the View Database State permission and can create a user with a password directly in the database.
```sql
CREATE USER [telegraf] WITH PASSWORD = N'mystrongpassword';
GO
GRANT VIEW DATABASE STATE TO [telegraf];
GO
```

### Configuration:

```toml
[agent]
  ## Default data collection interval for all inputs, can be changed as per collection interval needs
  interval = "10s"

# Read metrics from Microsoft SQL Server
[[inputs.sqlserver]]
  ## Specify instances to monitor with a list of connection strings.
  ## All connection parameters are optional.
  ## By default, the host is localhost, listening on default port, TCP 1433.
  ##   for Windows, the user is the currently running AD user (SSO).
  ##   See https://github.com/denisenkom/go-mssqldb for detailed connection
  ##   parameters, in particular, tls connections can be created like so:
  ##   "encrypt=true;certificate=<cert>;hostNameInCertificate=<SqlServer host fqdn>"
  # servers = [
  #  "Server=192.168.1.10;Port=1433;User Id=<user>;Password=<pw>;app name=telegraf;log=1;",
  # ]

  ## This enables a specific set of queries depending on the database type. If specified, it replaces azuredb = true/false and query_version = 2
  ## In the config file, the sql server plugin section should be repeated  each with a set of servers for a specific database_type.
  ## Possible values for database_type are  
  ## "AzureSQLDB" 
  ## "SQLServer"
  ## "AzureSQLManagedInstance"
  # database_type = "AzureSQLDB"

  ## Optional parameter, setting this to 2 will use a new version
  ## of the collection queries that break compatibility with the original dashboards.
  ## Version 2 - is compatible from SQL Server 2008 Sp3 and later versions and also for SQL Azure DB
  ## Version 2 is in the process of being deprecated, please consider using database_type.
  # query_version = 2

  ## If you are using AzureDB, setting this to true will gather resource utilization metrics
  # azuredb = false

  ## Possible queries accross different versions of the collectors
  ## Queries enabled by default for specific Database Type
  
  ## database_type =  AzureSQLDB  by default collects the following queries
  ## - AzureSQLDBWaitStats
  ## - AzureSQLDBResourceStats 
  ## - AzureSQLDBResourceGovernance
  ## - AzureSQLDBDatabaseIO
  ## - AzureSQLDBServerProperties
  ## - AzureSQLDBOsWaitstats
  ## - AzureSQLDBMemoryClerks
  ## - AzureSQLDBPerformanceCounters
  ## - AzureSQLDBRequests
  ## - AzureSQLDBSchedulers

   ## database_type =  AzureSQLManagedInstance by default collects the following queries
   ## - AzureSQLMIResourceStats 
   ## - AzureSQLMIResourceGovernance 
   ## - AzureSQLMIDatabaseIO 
   ## - AzureSQLMIServerProperties 
   ## - AzureSQLMIOsWaitstats 
   ## - AzureSQLMIMemoryClerks
   ## - AzureSQLMIPerformanceCounters
   ## - AzureSQLMIRequests
   ## - AzureSQLMISchedulers

   ## database_type =  SQLServer by default collects the following queries
   ## - SQLServerPerformanceCounters 
   ## - SQLServerWaitStatsCategorized 
   ## - SQLServerDatabaseIO 
   ## - SQLServerProperties 
   ## - SQLServerMemoryClerks 
   ## - SQLServerSchedulers
   ## - SQLServerRequests
   ## - SQLServerVolumeSpace
   ## - SQLServerCpu

  ## Version 2 by default collects the following queries
  ## Version 2 is being deprecated, please consider using database_type.
  ## - PerformanceCounters
  ## - WaitStatsCategorized
  ## - DatabaseIO
  ## - ServerProperties
  ## - MemoryClerk
  ## - Schedulers
  ## - SqlRequests
  ## - VolumeSpace
  ## - Cpu

  ## Version 1 by default collects the following queries
  ## Version 1 is deprecated, please consider using database_type.
  ## - PerformanceCounters
  ## - WaitStatsCategorized
  ## - CPUHistory
  ## - DatabaseIO
  ## - DatabaseSize
  ## - DatabaseStats
  ## - DatabaseProperties
  ## - MemoryClerk
  ## - VolumeSpace
  ## - PerformanceMetrics



  ## A list of queries to include. If not specified, all the above listed queries are used.
  # include_query = []

  ## A list of queries to explicitly ignore.
  exclude_query = [ 'Schedulers' , 'SqlRequests' ]



```

### Metrics:
To provide backwards compatibility, this plugin support two versions of metrics queries.

**Note**: Version 2 queries are not backwards compatible with the old queries. Any dashboards or queries based on the old query format will not work with the new format. The version 2 queries only report raw metrics, no math has been done to calculate deltas. To graph this data you must calculate deltas in your dashboarding software.

#### Version 1 (query_version=1): This is Deprecated in 1.6, all future development will be under configuration option database_type.
The original metrics queries provide:
- *Performance counters*: 1000+ metrics from `sys.dm_os_performance_counters`
- *Performance metrics*: special performance and ratio metrics
- *Wait stats*: wait tasks categorized from `sys.dm_os_wait_stats`
- *Memory clerk*: memory breakdown from `sys.dm_os_memory_clerks`
- *Database size*: databases size trend from `sys.dm_io_virtual_file_stats`
- *Database IO*: databases I/O from `sys.dm_io_virtual_file_stats`
- *Database latency*: databases latency from `sys.dm_io_virtual_file_stats`
- *Database properties*: databases properties, state and recovery model, from `sys.databases`
- *OS Volume*: available, used and total space from `sys.dm_os_volume_stats`
- *CPU*: cpu usage from `sys.dm_os_ring_buffers`

If you are using the original queries all stats have the following tags:
- `servername`:  hostname:instance
- `type`: type of stats to easily filter measurements

#### Version 2 (query_version=2): Being deprecated, All future development will be under configuration option database_type.
The new (version 2) metrics provide:
- *Database IO*: IO stats from `sys.dm_io_virtual_file_stats`
- *Memory Clerk*: Memory clerk breakdown from `sys.dm_os_memory_clerks`, most clerks have been given a friendly name.
- *Performance Counters*: A select list of performance counters from `sys.dm_os_performance_counters`. Some of the important metrics included:
  - *Activity*: Transactions/sec/database, Batch requests/sec, blocked processes, + more
  - *Availability Groups*: Bytes sent to replica, Bytes received from replica, Log bytes received, Log send queue, transaction delay, + more
  - *Log activity*: Log bytes flushed/sec, Log flushes/sec, Log Flush Wait Time
  - *Memory*: PLE, Page reads/sec, Page writes/sec, + more
  - *TempDB*: Free space, Version store usage, Active temp tables, temp table creation rate, + more
  - *Resource Governor*: CPU Usage, Requests/sec, Queued Requests, and Blocked tasks per workload group + more
- *Server properties*: Number of databases in all possible states (online, offline, suspect, etc.), cpu count, physical memory, SQL Server service uptime, and SQL Server version. In the case of Azure SQL relevent properties such as Tier, #Vcores, Memory etc.
- *Wait stats*: Wait time in ms, number of waiting tasks, resource wait time, signal wait time, max wait time in ms, wait type, and wait category. The waits are categorized using the same categories used in Query Store.
- *Schedulers* - This captures `sys.dm_os_schedulers`.
- *SqlRequests* - This captures a snapshot of `sys.dm_exec_requests` and `sys.dm_exec_sessions` that gives you running requests as well as wait types and
  blocking sessions.
- *VolumeSpace* - uses `sys.dm_os_volume_stats` to get total, used and occupied space on every disk that contains a data or log file. (Note that even if enabled it won't get any data from Azure SQL Database or SQL Managed Instance). It is pointless to run this with high frequency (ie: every 10s), but it won't cause any problem.
- *Cpu* - uses the buffer ring (`sys.dm_os_ring_buffers`) to get CPU data, the table is updated once per minute. (Note that even if enabled it won't get any data from Azure SQL Database or SQL Managed Instance).

  In order to allow tracking on a per statement basis this query produces a
  unique tag for each query.  Depending on the database workload, this may
  result in a high cardinality series.  Reference the FAQ for tips on
  [managing series cardinality][cardinality].

- *Azure Managed Instances*
  - Stats from `sys.server_resource_stats`
  - Resource governance stats from `sys.dm_instance_resource_governance`
- *Azure SQL Database* in addition to other stats
  - Stats from `sys.dm_db_wait_stats`
  - Resource governance stats from `sys.dm_user_db_resource_governance`
  - Stats from `sys.dm_db_resource_stats`
  


#### database_type = "AzureSQLDB 
These are metrics for Azure SQL Database (single database) and are very similar to version 2 but split out for maintenance reasons, better ability to test,differences in DMVs:
- AzureSQLDBDatabaseIO: IO stats from `sys.dm_io_virtual_file_stats` including resource governance time, RBPEX, IO for Hyperscale.
- AzureSQLDBMemoryClerks: Memory clerk breakdown from `sys.dm_os_memory_clerks`.
= AzureSQLDBResourceGovernance: Relevant properties indicatign resource limits from `sys.dm_user_db_resource_governance`
- AzureSQLDBPerformanceCounters: A select list of performance counters from `sys.dm_os_performance_counters` including cloud specific counters for SQL Hyperscale.
- AzureSQLDBServerProperties: Relevant Azure SQL relevent properties from  such as Tier, #Vcores, Memory etc, storage, etc.
- AzureSQLDBWaitstats: Wait time in ms from `sys.dm_db_wait_stats`, number of waiting tasks, resource wait time, signal wait time, max wait time in ms, wait type, and wait category. The waits are categorized using the same categories used in Query Store. These waits are collected only as of the end of the a statement. and for a specific database only.
- *AzureSQLOsWaitstats*: Wait time in ms from `sys.dm_os_wait_stats`, number of waiting tasks, resource wait time, signal wait time, max wait time in ms, wait type, and wait category. The waits are categorized using the same categories used in Query Store. These waits are collected as they occur and instance wide
- *AzureSQLDBRequests: Requests which are blocked or have a wait type from `sys.dm_exec_sessions` and `sys.dm_exec_requests`
- *AzureSQLDBSchedulers* - This captures `sys.dm_os_schedulers` snapshots.


#### database_type = "AzureSQLManagedInstance 
These are metrics for Azure SQL Managed instance, are very similar to version 2 but split out for maintenance reasons, better ability to test, differences in DMVs:
- AzureSQLMIDatabaseIO: IO stats from `sys.dm_io_virtual_file_stats` including resource governance time, RBPEX, IO for Hyperscale.
- AzureSQLMIMemoryClerks: Memory clerk breakdown from `sys.dm_os_memory_clerks`.
- AzureSQLMIResourceGovernance: Relevant properties indicatign resource limits from `sys.dm_instance_resource_governance`
- AzureSQLMIPerformanceCounters: A select list of performance counters from `sys.dm_os_performance_counters` including cloud specific counters for SQL Hyperscale.
- AzureSQLMIServerProperties: Relevant Azure SQL relevent properties such as Tier, #Vcores, Memory etc, storage, etc.
- AzureSQLMIOsWaitstats: Wait time in ms from `sys.dm_os_wait_stats`, number of waiting tasks, resource wait time, signal wait time, max wait time in ms, wait type, and wait category. The waits are categorized using the same categories used in Query Store. These waits are collected as they occur and instance wide
- AzureSQLMIRequests: Requests which are blocked or have a wait type from `sys.dm_exec_sessions` and `sys.dm_exec_requests`
- AzureSQLMISchedulers - This captures `sys.dm_os_schedulers` snapshots.

#### database_type = "SQLServer 
- SQLServerDatabaseIO: IO stats from `sys.dm_io_virtual_file_stats`
- SQLServerMemoryClerks: Memory clerk breakdown from `sys.dm_os_memory_clerks`, most clerks have been given a friendly name.
- SQLServerPerformanceCounters: A select list of performance counters from `sys.dm_os_performance_counters`. Some of the important metrics included:
  - *Activity*: Transactions/sec/database, Batch requests/sec, blocked processes, + more
  - *Availability Groups*: Bytes sent to replica, Bytes received from replica, Log bytes received, Log send queue, transaction delay, + more
  - *Log activity*: Log bytes flushed/sec, Log flushes/sec, Log Flush Wait Time
  - *Memory*: PLE, Page reads/sec, Page writes/sec, + more
  - *TempDB*: Free space, Version store usage, Active temp tables, temp table creation rate, + more
  - *Resource Governor*: CPU Usage, Requests/sec, Queued Requests, and Blocked tasks per workload group + more
- SQLServerProperties: Number of databases in all possible states (online, offline, suspect, etc.), cpu count, physical memory, SQL Server service uptime, and SQL Server version. In the case of Azure SQL relevent properties such as Tier, #Vcores, Memory etc.
- SQLServerWaitStatsCategorized: Wait time in ms, number of waiting tasks, resource wait time, signal wait time, max wait time in ms, wait type, and wait category. The waits are categorized using the same categories used in Query Store.
- SQLServerSchedulers - This captures `sys.dm_os_schedulers`.
- SQLServerRequests - This captures a snapshot of `sys.dm_exec_requests` and `sys.dm_exec_sessions` that gives you running requests as well as wait types and
  blocking sessions.
- SQLServerVolumeSpace - uses `sys.dm_os_volume_stats` to get total, used and occupied space on every disk that contains a data or log file. (Note that even if enabled it won't get any data from Azure SQL Database or SQL Managed Instance). It is pointless to run this with high frequency (ie: every 10s), but it won't cause any problem.
- SQLServerCpu - uses the buffer ring (`sys.dm_os_ring_buffers`) to get CPU data, the table is updated once per minute. (Note that even if enabled it won't get any data from Azure SQL Database or SQL Managed Instance).


#### Output Measures
The guiding principal is that all data collected from the same primary DMV ends up in the same measure irrespective of database_type.
`sqlserver_database_io` - Used by  AzureSQLDBDatabaseIO, AzureSQLMIDatabaseIO, SQLServerDatabaseIO, DatabaseIO given the data is from `sys.dm_io_virtual_file_stats`
`sqlserver_waitstats` - Used by  WaitStatsCategorized,AzureSQLDBOsWaitstats,AzureSQLMIOsWaitstats
`sqlserver_server_properties` - Used by  SQLServerProperties, AzureSQLDBServerProperties , AzureSQLMIServerProperties,ServerProperties
`sqlserver_memory_clerks` - Used by SQLServerMemoryClerks, AzureSQLDBMemoryClerks, AzureSQLMIMemoryClerks,MemoryClerk
`sqlserver_performance` - Used by  SQLServerPerformanceCounters, AzureSQLDBPerformanceCounters, AzureSQLMIPerformanceCounters,PerformanceCounters
`sys.dm_os_schedulers`  - Used by SQLServerSchedulers,AzureSQLDBServerSchedulers, AzureSQLMIServerSchedulers



The following Performance counter metrics can be used directly, with no delta calculations:
 - SQLServer:Buffer Manager\Buffer cache hit ratio
 - SQLServer:Buffer Manager\Page life expectancy
 - SQLServer:Buffer Node\Page life expectancy
 - SQLServer:Database Replica\Log Apply Pending Queue
 - SQLServer:Database Replica\Log Apply Ready Queue
 - SQLServer:Database Replica\Log Send Queue
 - SQLServer:Database Replica\Recovery Queue
 - SQLServer:Databases\Data File(s) Size (KB)
 - SQLServer:Databases\Log File(s) Size (KB)
 - SQLServer:Databases\Log File(s) Used Size (KB)
 - SQLServer:Databases\XTP Memory Used (KB)
 - SQLServer:General Statistics\Active Temp Tables
 - SQLServer:General Statistics\Processes blocked
 - SQLServer:General Statistics\Temp Tables For Destruction
 - SQLServer:General Statistics\User Connections
 - SQLServer:Memory Broker Clerks\Memory broker clerk size
 - SQLServer:Memory Manager\Memory Grants Pending
 - SQLServer:Memory Manager\Target Server Memory (KB)
 - SQLServer:Memory Manager\Total Server Memory (KB)
 - SQLServer:Resource Pool Stats\Active memory grant amount (KB)
 - SQLServer:Resource Pool Stats\Disk Read Bytes/sec
 - SQLServer:Resource Pool Stats\Disk Read IO Throttled/sec
 - SQLServer:Resource Pool Stats\Disk Read IO/sec
 - SQLServer:Resource Pool Stats\Disk Write Bytes/sec
 - SQLServer:Resource Pool Stats\Disk Write IO Throttled/sec
 - SQLServer:Resource Pool Stats\Disk Write IO/sec
 - SQLServer:Resource Pool Stats\Used memory (KB)
 - SQLServer:Transactions\Free Space in tempdb (KB)
 - SQLServer:Transactions\Version Store Size (KB)
 - SQLServer:User Settable\Query
 - SQLServer:Workload Group Stats\Blocked tasks
 - SQLServer:Workload Group Stats\CPU usage %
 - SQLServer:Workload Group Stats\Queued requests
 - SQLServer:Workload Group Stats\Requests completed/sec

Version 2 queries have the following tags:
- `sql_instance`: Physical host and instance name (hostname:instance)
- `database_name`:  For Azure SQLDB, database_name denotes the name of the Azure SQL Database as server name is a logical construct.

[cardinality]: /docs/FAQ.md#user-content-q-how-can-i-manage-series-cardinality
