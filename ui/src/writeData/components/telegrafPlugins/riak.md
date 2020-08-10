# Riak Plugin

The Riak plugin gathers metrics from one or more riak instances.

### Configuration:

```toml
# Description
[[inputs.riak]]
  # Specify a list of one or more riak http servers
  servers = ["http://localhost:8098"]
```

### Measurements & Fields:

Riak provides one measurement named "riak", with the following fields:

- cpu_avg1
- cpu_avg15
- cpu_avg5
- memory_code
- memory_ets
- memory_processes
- memory_system
- memory_total
- node_get_fsm_objsize_100
- node_get_fsm_objsize_95
- node_get_fsm_objsize_99
- node_get_fsm_objsize_mean
- node_get_fsm_objsize_median
- node_get_fsm_siblings_100
- node_get_fsm_siblings_95
- node_get_fsm_siblings_99
- node_get_fsm_siblings_mean
- node_get_fsm_siblings_median
- node_get_fsm_time_100
- node_get_fsm_time_95
- node_get_fsm_time_99
- node_get_fsm_time_mean
- node_get_fsm_time_median
- node_gets
- node_gets_total
- node_put_fsm_time_100
- node_put_fsm_time_95
- node_put_fsm_time_99
- node_put_fsm_time_mean
- node_put_fsm_time_median
- node_puts
- node_puts_total
- pbc_active
- pbc_connects
- pbc_connects_total
- vnode_gets
- vnode_gets_total
- vnode_index_reads
- vnode_index_reads_total
- vnode_index_writes
- vnode_index_writes_total
- vnode_puts
- vnode_puts_total
- read_repairs
- read_repairs_total

Measurements of time (such as node_get_fsm_time_mean) are measured in nanoseconds.

### Tags:

All measurements have the following tags:

- server (the host:port of the given server address, ex. `127.0.0.1:8087`)
- nodename (the internal node name received, ex. `riak@127.0.0.1`)

### Example Output:

```
$ ./telegraf --config telegraf.conf --input-filter riak --test
> riak,nodename=riak@127.0.0.1,server=localhost:8098 cpu_avg1=31i,cpu_avg15=69i,cpu_avg5=51i,memory_code=11563738i,memory_ets=5925872i,memory_processes=30236069i,memory_system=93074971i,memory_total=123311040i,node_get_fsm_objsize_100=0i,node_get_fsm_objsize_95=0i,node_get_fsm_objsize_99=0i,node_get_fsm_objsize_mean=0i,node_get_fsm_objsize_median=0i,node_get_fsm_siblings_100=0i,node_get_fsm_siblings_95=0i,node_get_fsm_siblings_99=0i,node_get_fsm_siblings_mean=0i,node_get_fsm_siblings_median=0i,node_get_fsm_time_100=0i,node_get_fsm_time_95=0i,node_get_fsm_time_99=0i,node_get_fsm_time_mean=0i,node_get_fsm_time_median=0i,node_gets=0i,node_gets_total=19i,node_put_fsm_time_100=0i,node_put_fsm_time_95=0i,node_put_fsm_time_99=0i,node_put_fsm_time_mean=0i,node_put_fsm_time_median=0i,node_puts=0i,node_puts_total=0i,pbc_active=0i,pbc_connects=0i,pbc_connects_total=20i,vnode_gets=0i,vnode_gets_total=57i,vnode_index_reads=0i,vnode_index_reads_total=0i,vnode_index_writes=0i,vnode_index_writes_total=0i,vnode_puts=0i,vnode_puts_total=0i,read_repair=0i,read_repairs_total=0i 1455913392622482332
```
