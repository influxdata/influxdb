# Intel RDT Input Plugin
The `intel_rdt` plugin collects information provided by monitoring features of 
the Intel Resource Director Technology (Intel(R) RDT). Intel RDT provides the hardware framework to monitor 
and control the utilization of shared resources (ex: last level cache, memory bandwidth). 

### About Intel RDT
Intel’s Resource Director Technology (RDT) framework consists of:  
- Cache Monitoring Technology (CMT) 
- Memory Bandwidth Monitoring (MBM)
- Cache Allocation Technology (CAT) 
- Code and Data Prioritization (CDP) 

As multithreaded and multicore platform architectures emerge, the last level cache and 
memory bandwidth are key resources to manage for running workloads in single-threaded, 
multithreaded, or complex virtual machine environments. Intel introduces CMT, MBM, CAT 
and CDP to manage these workloads across shared resources. 

### Prerequsities - PQoS Tool
To gather Intel RDT metrics, the `intel_rdt` plugin uses _pqos_ cli tool which is a 
part of [Intel(R) RDT Software Package](https://github.com/intel/intel-cmt-cat).
Before using this plugin please be sure _pqos_ is properly installed and configured regarding that the plugin
run _pqos_ to work with `OS Interface` mode. This plugin supports _pqos_ version 4.0.0 and above.
Note: pqos tool needs root privileges to work properly.

Metrics will be constantly reported from the following `pqos` commands within the given interval:

#### In case of cores monitoring:
```
pqos -r --iface-os --mon-file-type=csv --mon-interval=INTERVAL --mon-core=all:[CORES]\;mbt:[CORES]
```
where `CORES` is equal to group of cores provided in config. User can provide many groups.

#### In case of process monitoring:
```
pqos -r --iface-os --mon-file-type=csv --mon-interval=INTERVAL --mon-pid=all:[PIDS]\;mbt:[PIDS]
```
where `PIDS` is group of processes IDs which name are equal to provided process name in a config.
User can provide many process names which lead to create many processes groups.

In both cases `INTERVAL` is equal to sampling_interval from config.

Because PIDs association within system could change in every moment, Intel RDT plugin provides a 
functionality to check on every interval if desired processes change their PIDs association.
If some change is reported, plugin will restart _pqos_ tool with new arguments. If provided by user
process name is not equal to any of available processes, will be omitted and plugin will constantly
check for process availability.

### Useful links
Pqos installation process: https://github.com/intel/intel-cmt-cat/blob/master/INSTALL  
Enabling OS interface: https://github.com/intel/intel-cmt-cat/wiki, https://github.com/intel/intel-cmt-cat/wiki/resctrl  
More about Intel RDT: https://www.intel.com/content/www/us/en/architecture-and-technology/resource-director-technology.html

### Configuration
```toml
# Read Intel RDT metrics
[[inputs.intel_rdt]]
  ## Optionally set sampling interval to Nx100ms. 
  ## This value is propagated to pqos tool. Interval format is defined by pqos itself.
  ## If not provided or provided 0, will be set to 10 = 10x100ms = 1s.
  # sampling_interval = "10"
	
  ## Optionally specify the path to pqos executable. 
  ## If not provided, auto discovery will be performed.
  # pqos_path = "/usr/local/bin/pqos"

  ## Optionally specify if IPC and LLC_Misses metrics shouldn't be propagated.
  ## If not provided, default value is false.
  # shortened_metrics = false

  ## Specify the list of groups of CPU core(s) to be provided as pqos input. 
  ## Mandatory if processes aren't set and forbidden if processes are specified.
  ## e.g. ["0-3", "4,5,6"] or ["1-3,4"]
  # cores = ["0-3"]

  ## Specify the list of processes for which Metrics will be collected.
  ## Mandatory if cores aren't set and forbidden if cores are specified.
  ## e.g. ["qemu", "pmd"]
  # processes = ["process"]
```

### Exposed metrics
| Name          | Full name                                     | Description |
|---------------|-----------------------------------------------|-------------|
| MBL           | Memory Bandwidth on Local NUMA Node  |     Memory bandwidth utilization by the relevant CPU core/process on the local NUMA memory channel        |
| MBR           | Memory Bandwidth on Remote NUMA Node |     Memory bandwidth utilization by the relevant CPU core/process on the remote NUMA memory channel        |
| MBT           | Total Memory Bandwidth               |     Total memory bandwidth utilized by a CPU core/process on local and remote NUMA memory channels        |
| LLC           | L3 Cache Occupancy                   |     Total Last Level Cache occupancy by a CPU core/process         |
| LLC_Misses*    | L3 Cache Misses                      |    Total Last Level Cache misses by a CPU core/process       |
| IPC*           | Instructions Per Cycle               |     Total instructions per cycle executed by a CPU core/process        |

*optional

### Troubleshooting
Pointing to non-existing cores will lead to throwing an error by _pqos_ and the plugin will not work properly.
Be sure to check provided core number exists within desired system.  

Be aware, reading Intel RDT metrics by _pqos_ cannot be done simultaneously on the same resource.
Do not use any other _pqos_ instance that is monitoring the same cores or PIDs within the working system.
It is not possible to monitor same cores or PIDs on different groups.

PIDs associated for the given process could be manually checked by `pidof` command. E.g:
```
pidof PROCESS
```
where `PROCESS` is process name.

### Example Output
```
> rdt_metric,cores=12\,19,host=r2-compute-20,name=IPC,process=top value=0 1598962030000000000
> rdt_metric,cores=12\,19,host=r2-compute-20,name=LLC_Misses,process=top value=0 1598962030000000000
> rdt_metric,cores=12\,19,host=r2-compute-20,name=LLC,process=top value=0 1598962030000000000
> rdt_metric,cores=12\,19,host=r2-compute-20,name=MBL,process=top value=0 1598962030000000000
> rdt_metric,cores=12\,19,host=r2-compute-20,name=MBR,process=top value=0 1598962030000000000
> rdt_metric,cores=12\,19,host=r2-compute-20,name=MBT,process=top value=0 1598962030000000000
```
