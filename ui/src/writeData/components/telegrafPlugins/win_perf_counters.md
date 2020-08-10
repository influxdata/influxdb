# win_perf_counters readme

This document presents the input plugin to read Performance Counters on Windows operating systems.

The configuration is parsed and then tested for validity, such as
whether the Object, Instance and Counter exist on Telegraf startup.

Counter paths are refreshed periodically, see the [CountersRefreshInterval](#countersrefreshinterval)
configuration parameter for more info.

In case of query for all instances `["*"]`, the plugin does not return the instance `_Total`
by default. See [IncludeTotal](#includetotal) for more info.

## Basics

The examples contained in this file have been found on the internet
as counters used when performance monitoring
 Active Directory and IIS in particular.
 There are a lot of other good objects to monitor, if you know what to look for.
 This file is likely to be updated in the future with more examples for
 useful configurations for separate scenarios.

### Plugin wide

Plugin wide entries are underneath `[[inputs.win_perf_counters]]`.

#### PrintValid

Bool, if set to `true` will print out all matching performance objects.

Example:
`PrintValid=true`

#### UseWildcardsExpansion

If `UseWildcardsExpansion` is set to true, wildcards can be used in the
instance name and the counter name.  When using localized Windows, counters
will be also be localized.  Instance indexes will also be returned in the
instance name.

Partial wildcards (e.g. `chrome*`) are supported only in the instance name on Windows Vista and newer.

If disabled, wildcards (not partial) in instance names can still be used, but
instance indexes will not be returned in the instance names.

Example:
`UseWildcardsExpansion=true`

#### CountersRefreshInterval

Configured counters are matched against available counters at the interval
specified by the `CountersRefreshInterval` parameter. The default value is `1m` (1 minute).

If wildcards are used in instance or counter names, they are expanded at this point, if the `UseWildcardsExpansion` param is set to `true`.

Setting the `CountersRefreshInterval` too low (order of seconds) can cause Telegraf to create
a high CPU load.

Set it to `0s` to disable periodic refreshing.

Example:
`CountersRefreshInterval=1m`

#### PreVistaSupport

_Deprecated. Necessary features on Windows Vista and newer are checked dynamically_

Bool, if set to `true`, the plugin will use the localized PerfCounter interface that has been present since before Vista for backwards compatability.

It is recommended NOT to use this on OSes starting with Vista and newer because it requires more configuration to use this than the newer interface present since Vista.

Example for Windows Server 2003, this would be set to true:
`PreVistaSupport=true`

#### UsePerfCounterTime

Bool, if set to `true` will request a timestamp along with the PerfCounter data. 
If se to `false`, current time will be used.

Supported on Windows Vista/Windows Server 2008 and newer
Example:
`UsePerfCounterTime=true`

### Object

See Entry below.

### Entry
A new configuration entry consists of the TOML header starting with,
`[[inputs.win_perf_counters.object]]`.
This must follow before other plugin configurations,
beneath the main win_perf_counters entry, `[[inputs.win_perf_counters]]`.

Following this are 3 required key/value pairs and three optional parameters and their usage.

#### ObjectName
**Required**

ObjectName is the Object to query for, like Processor, DirectoryServices, LogicalDisk or similar.

Example: `ObjectName = "LogicalDisk"`

#### Instances
**Required**

The instances key (this is an array) declares the instances of a counter you would like returned,
it can be one or more values.

Example: `Instances = ["C:","D:","E:"]`

This will return only for the instances
C:, D: and E: where relevant. To get all instances of a Counter, use `["*"]` only.
By default any results containing `_Total` are stripped,
unless this is specified as the wanted instance.
Alternatively see the option `IncludeTotal` below.

It is also possible to set partial wildcards, eg. `["chrome*"]`, if the `UseWildcardsExpansion` param is set to `true`

Some Objects do not have instances to select from at all.
Here only one option is valid if you want data back,
and that is to specify `Instances = ["------"]`.

#### Counters
**Required**

The Counters key (this is an array) declares the counters of the ObjectName
you would like returned, it can also be one or more values.

Example: `Counters = ["% Idle Time", "% Disk Read Time", "% Disk Write Time"]`

This must be specified for every counter you want the results of, or use
`["*"]` for all the counters of the object, if the `UseWildcardsExpansion` param
is set to `true`.

#### Measurement
*Optional*

This key is optional. If it is not set it will be `win_perf_counters`.
In InfluxDB this is the key underneath which the returned data is stored.
So for ordering your data in a good manner,
this is a good key to set with a value when you want your IIS and Disk results stored
separately from Processor results.

Example: `Measurement = "win_disk"``

#### IncludeTotal
*Optional*

This key is optional. It is a simple bool.
If it is not set to true or included it is treated as false.
This key only has effect if the Instances key is set to `["*"]`
and you would also like all instances containing `_Total` to be returned,
like `_Total`, `0,_Total` and so on where applicable
(Processor Information is one example).

#### WarnOnMissing
*Optional*

This key is optional. It is a simple bool.
If it is not set to true or included it is treated as false.
This only has effect on the first execution of the plugin.
It will print out any ObjectName/Instance/Counter combinations
asked for that do not match. Useful when debugging new configurations.

#### FailOnMissing
*Internal*

This key should not be used. It is for testing purposes only.
It is a simple bool. If it is not set to true or included this is treated as false.
If this is set to true, the plugin will abort and end prematurely
if any of the combinations of ObjectName/Instances/Counters are invalid.

## Examples

### Generic Queries
```
[[inputs.win_perf_counters]]
  [[inputs.win_perf_counters.object]]
    # Processor usage, alternative to native, reports on a per core.
    ObjectName = "Processor"
    Instances = ["*"]
    Counters = ["% Idle Time", "% Interrupt Time", "% Privileged Time", "% User Time", "% Processor Time"]
    Measurement = "win_cpu"
    #IncludeTotal=false #Set to true to include _Total instance when querying for all (*).

  [[inputs.win_perf_counters.object]]
    # Disk times and queues
    ObjectName = "LogicalDisk"
    Instances = ["*"]
    Counters = ["% Idle Time", "% Disk Time","% Disk Read Time", "% Disk Write Time", "% User Time", "Current Disk Queue Length"]
    Measurement = "win_disk"
    #IncludeTotal=false #Set to true to include _Total instance when querying for all (*).

  [[inputs.win_perf_counters.object]]
    ObjectName = "System"
    Counters = ["Context Switches/sec","System Calls/sec", "Processor Queue Length"]
    Instances = ["------"]
    Measurement = "win_system"
    #IncludeTotal=false #Set to true to include _Total instance when querying for all (*).

  [[inputs.win_perf_counters.object]]
    # Example query where the Instance portion must be removed to get data back, such as from the Memory object.
    ObjectName = "Memory"
    Counters = ["Available Bytes","Cache Faults/sec","Demand Zero Faults/sec","Page Faults/sec","Pages/sec","Transition Faults/sec","Pool Nonpaged Bytes","Pool Paged Bytes"]
    Instances = ["------"] # Use 6 x - to remove the Instance bit from the query.
    Measurement = "win_mem"
    #IncludeTotal=false #Set to true to include _Total instance when querying for all (*).

  [[inputs.win_perf_counters.object]]
    # more counters for the Network Interface Object can be found at
    # https://msdn.microsoft.com/en-us/library/ms803962.aspx
    ObjectName = "Network Interface"
    Counters = ["Bytes Received/sec","Bytes Sent/sec","Packets Received/sec","Packets Sent/sec"]
    Instances = ["*"] # Use 6 x - to remove the Instance bit from the query.
    Measurement = "win_net"
    #IncludeTotal=false #Set to true to include _Total instance when querying for all (*).
```

### Active Directory Domain Controller
```
[[inputs.win_perf_counters]]
  [inputs.win_perf_counters.tags]
    monitorgroup = "ActiveDirectory"
  [[inputs.win_perf_counters.object]]
    ObjectName = "DirectoryServices"
    Instances = ["*"]
    Counters = ["Base Searches/sec","Database adds/sec","Database deletes/sec","Database modifys/sec","Database recycles/sec","LDAP Client Sessions","LDAP Searches/sec","LDAP Writes/sec"]
    Measurement = "win_ad" # Set an alternative measurement to win_perf_counters if wanted.
    #Instances = [""] # Gathers all instances by default, specify to only gather these
    #IncludeTotal=false #Set to true to include _Total instance when querying for all (*).

  [[inputs.win_perf_counters.object]]
    ObjectName = "Security System-Wide Statistics"
    Instances = ["*"]
    Counters = ["NTLM Authentications","Kerberos Authentications","Digest Authentications"]
    Measurement = "win_ad"
    #IncludeTotal=false #Set to true to include _Total instance when querying for all (*).

  [[inputs.win_perf_counters.object]]
    ObjectName = "Database"
    Instances = ["*"]
    Counters = ["Database Cache % Hit","Database Cache Page Fault Stalls/sec","Database Cache Page Faults/sec","Database Cache Size"]
    Measurement = "win_db"
    #IncludeTotal=false #Set to true to include _Total instance when querying for all (*).
```

### DFS Namespace + Domain Controllers
```
[[inputs.win_perf_counters]]
  [[inputs.win_perf_counters.object]]
    # AD, DFS N, Useful if the server hosts a DFS Namespace or is a Domain Controller
    ObjectName = "DFS Namespace Service Referrals"
    Instances = ["*"]
    Counters = ["Requests Processed","Requests Failed","Avg. Response Time"]
    Measurement = "win_dfsn"
    #IncludeTotal=false #Set to true to include _Total instance when querying for all (*).
    #WarnOnMissing = false # Print out when the performance counter is missing, either of object, counter or instance.
```

### DFS Replication + Domain Controllers
```
[[inputs.win_perf_counters]]
  [[inputs.win_perf_counters.object]]
    # AD, DFS R, Useful if the server hosts a DFS Replication folder or is a Domain Controller
    ObjectName = "DFS Replication Service Volumes"
    Instances = ["*"]
    Counters = ["Data Lookups","Database Commits"]
    Measurement = "win_dfsr"
    #IncludeTotal=false #Set to true to include _Total instance when querying for all (*).
    #WarnOnMissing = false # Print out when the performance counter is missing, either of object, counter or instance.
```

### DNS Server + Domain Controllers
```
[[inputs.win_perf_counters]]
  [[inputs.win_perf_counters.object]]
    ObjectName = "DNS"
    Counters = ["Dynamic Update Received","Dynamic Update Rejected","Recursive Queries","Recursive Queries Failure","Secure Update Failure","Secure Update Received","TCP Query Received","TCP Response Sent","UDP Query Received","UDP Response Sent","Total Query Received","Total Response Sent"]
    Instances = ["------"]
    Measurement = "win_dns"
    #IncludeTotal=false #Set to true to include _Total instance when querying for all (*).
```

### IIS / ASP.NET
```
[[inputs.win_perf_counters]]
  [[inputs.win_perf_counters.object]]
    # HTTP Service request queues in the Kernel before being handed over to User Mode.
    ObjectName = "HTTP Service Request Queues"
    Instances = ["*"]
    Counters = ["CurrentQueueSize","RejectedRequests"]
    Measurement = "win_http_queues"
    #IncludeTotal=false #Set to true to include _Total instance when querying for all (*).

  [[inputs.win_perf_counters.object]]
    # IIS, ASP.NET Applications
    ObjectName = "ASP.NET Applications"
    Counters = ["Cache Total Entries","Cache Total Hit Ratio","Cache Total Turnover Rate","Output Cache Entries","Output Cache Hits","Output Cache Hit Ratio","Output Cache Turnover Rate","Compilations Total","Errors Total/Sec","Pipeline Instance Count","Requests Executing","Requests in Application Queue","Requests/Sec"]
    Instances = ["*"]
    Measurement = "win_aspnet_app"
    #IncludeTotal=false #Set to true to include _Total instance when querying for all (*).

  [[inputs.win_perf_counters.object]]
    # IIS, ASP.NET
    ObjectName = "ASP.NET"
    Counters = ["Application Restarts","Request Wait Time","Requests Current","Requests Queued","Requests Rejected"]
    Instances = ["*"]
    Measurement = "win_aspnet"
    #IncludeTotal=false #Set to true to include _Total instance when querying for all (*).

  [[inputs.win_perf_counters.object]]
    # IIS, Web Service
    ObjectName = "Web Service"
    Counters = ["Get Requests/sec","Post Requests/sec","Connection Attempts/sec","Current Connections","ISAPI Extension Requests/sec"]
    Instances = ["*"]
    Measurement = "win_websvc"
    #IncludeTotal=false #Set to true to include _Total instance when querying for all (*).

  [[inputs.win_perf_counters.object]]
    # Web Service Cache / IIS
    ObjectName = "Web Service Cache"
    Counters = ["URI Cache Hits %","Kernel: URI Cache Hits %","File Cache Hits %"]
    Instances = ["*"]
    Measurement = "win_websvc_cache"
    #IncludeTotal=false #Set to true to include _Total instance when querying for all (*).
```

### Process
```
[[inputs.win_perf_counters]]
  [[inputs.win_perf_counters.object]]
    # Process metrics, in this case for IIS only
    ObjectName = "Process"
    Counters = ["% Processor Time","Handle Count","Private Bytes","Thread Count","Virtual Bytes","Working Set"]
    Instances = ["w3wp"]
    Measurement = "win_proc"
    #IncludeTotal=false #Set to true to include _Total instance when querying for all (*).
```

### .NET Monitoring
```
[[inputs.win_perf_counters]]
  [[inputs.win_perf_counters.object]]
    # .NET CLR Exceptions, in this case for IIS only
    ObjectName = ".NET CLR Exceptions"
    Counters = ["# of Exceps Thrown / sec"]
    Instances = ["w3wp"]
    Measurement = "win_dotnet_exceptions"
    #IncludeTotal=false #Set to true to include _Total instance when querying for all (*).

  [[inputs.win_perf_counters.object]]
    # .NET CLR Jit, in this case for IIS only
    ObjectName = ".NET CLR Jit"
    Counters = ["% Time in Jit","IL Bytes Jitted / sec"]
    Instances = ["w3wp"]
    Measurement = "win_dotnet_jit"
    #IncludeTotal=false #Set to true to include _Total instance when querying for all (*).

  [[inputs.win_perf_counters.object]]
    # .NET CLR Loading, in this case for IIS only
    ObjectName = ".NET CLR Loading"
    Counters = ["% Time Loading"]
    Instances = ["w3wp"]
    Measurement = "win_dotnet_loading"
    #IncludeTotal=false #Set to true to include _Total instance when querying for all (*).

  [[inputs.win_perf_counters.object]]
    # .NET CLR LocksAndThreads, in this case for IIS only
    ObjectName = ".NET CLR LocksAndThreads"
    Counters = ["# of current logical Threads","# of current physical Threads","# of current recognized threads","# of total recognized threads","Queue Length / sec","Total # of Contentions","Current Queue Length"]
    Instances = ["w3wp"]
    Measurement = "win_dotnet_locks"
    #IncludeTotal=false #Set to true to include _Total instance when querying for all (*).

  [[inputs.win_perf_counters.object]]
    # .NET CLR Memory, in this case for IIS only
    ObjectName = ".NET CLR Memory"
    Counters = ["% Time in GC","# Bytes in all Heaps","# Gen 0 Collections","# Gen 1 Collections","# Gen 2 Collections","# Induced GC","Allocated Bytes/sec","Finalization Survivors","Gen 0 heap size","Gen 1 heap size","Gen 2 heap size","Large Object Heap size","# of Pinned Objects"]
    Instances = ["w3wp"]
    Measurement = "win_dotnet_mem"
    #IncludeTotal=false #Set to true to include _Total instance when querying for all (*).

  [[inputs.win_perf_counters.object]]
    # .NET CLR Security, in this case for IIS only
    ObjectName = ".NET CLR Security"
    Counters = ["% Time in RT checks","Stack Walk Depth","Total Runtime Checks"]
    Instances = ["w3wp"]
    Measurement = "win_dotnet_security"
    #IncludeTotal=false #Set to true to include _Total instance when querying for all (*).
```

## Troubleshooting

If you are getting an error about an invalid counter, use the `typeperf` command to check the counter path
on the command line.
E.g. `typeperf "Process(chrome*)\% Processor Time"`

If no metrics are emitted even with the default config, you may need to repair
your performance counters.

1. Launch the Command Prompt as Administrator (right click Runs As Administrator).
1. Drop into the C:\WINDOWS\System32 directory by typing `C:` then `cd \Windows\System32`
1. Rebuild your counter values, which may take a few moments so please be
   patient, by running:
   ```
   lodctr /r
   ```
