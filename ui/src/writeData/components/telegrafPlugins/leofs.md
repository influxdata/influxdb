# LeoFS Input Plugin

The LeoFS plugin gathers metrics of LeoGateway, LeoManager, and LeoStorage using SNMP. See [LeoFS Documentation / System Administration / System Monitoring](https://leo-project.net/leofs/docs/admin/system_admin/monitoring/).

## Configuration:

```toml
# Sample Config:

[[inputs.leofs]]
        servers = ["127.0.0.1:4010"]
```

## Measurements & Fields:
### Statistics specific to the internals of LeoManager
#### Erlang VM

- 1 min Statistics
    - num_of_processes
    - total_memory_usage
    - system_memory_usage
    - processes_memory_usage
    - ets_memory_usage
    - used_allocated_memory
    - allocated_memory
- 5 min Statistics
    - num_of_processes_5min
    - total_memory_usage_5min
    - system_memory_usage_5min
    - processes_memory_usage_5min
    - ets_memory_usage_5min
    - used_allocated_memory_5min
    - allocated_memory_5min

### Statistics specific to the internals of LeoStorage
#### Erlang VM

- 1 min Statistics
    - num_of_processes
    - total_memory_usage
    - system_memory_usage
    - processes_memory_usage
    - ets_memory_usage
    - used_allocated_memory
    - allocated_memory
- 5 min Statistics
    - num_of_processes_5min
    - total_memory_usage_5min
    - system_memory_usage_5min
    - processes_memory_usage_5min
    - ets_memory_usage_5min
    - used_allocated_memory_5min
    - allocated_memory_5min

#### Total Number of Requests

- 1 min Statistics
    - num_of_writes
    - num_of_reads
    - num_of_deletes
- 5 min Statistics
    - num_of_writes_5min
    - num_of_reads_5min
    - num_of_deletes_5min

#### Total Number of Objects and Total Size of Objects

- num_of_active_objects
- total_objects
- total_size_of_active_objects
- total_size

#### Total Number of MQ Messages

- num_of_replication_messages,
- num_of_sync-vnode_messages,
- num_of_rebalance_messages,
- mq_num_of_msg_recovery_node
- mq_num_of_msg_deletion_dir
- mq_num_of_msg_async_deletion_dir
- mq_num_of_msg_req_deletion_dir
- mq_mdcr_num_of_msg_req_comp_metadata
- mq_mdcr_num_of_msg_req_sync_obj

Note: The following items are available since LeoFS v1.4.0:

- mq_num_of_msg_recovery_node
- mq_num_of_msg_deletion_dir
- mq_num_of_msg_async_deletion_dir
- mq_num_of_msg_req_deletion_dir
- mq_mdcr_num_of_msg_req_comp_metadata
- mq_mdcr_num_of_msg_req_sync_obj

#### Data Compaction

- comp_state
- comp_last_start_datetime
- comp_last_end_datetime
- comp_num_of_pending_targets
- comp_num_of_ongoing_targets
- comp_num_of_out_of_targets

Note: The all items are available since LeoFS v1.4.0.

### Statistics specific to the internals of LeoGateway
#### Erlang VM

- 1 min Statistics
    - num_of_processes
    - total_memory_usage
    - system_memory_usage
    - processes_memory_usage
    - ets_memory_usage
    - used_allocated_memory
    - allocated_memory
- 5 min Statistics
    - num_of_processes_5min
    - total_memory_usage_5min
    - system_memory_usage_5min
    - processes_memory_usage_5min
    - ets_memory_usage_5min
    - used_allocated_memory_5min
    - allocated_memory_5min

#### Total Number of Requests

- 1 min Statistics
    - num_of_writes
    - num_of_reads
    - num_of_deletes
- 5 min Statistics
    - num_of_writes_5min
    - num_of_reads_5min
    - num_of_deletes_5min

#### Object Cache

- count_of_cache-hit
- count_of_cache-miss
- total_of_files
- total_cached_size


### Tags:

All measurements have the following tags:

- node


### Example output:

#### LeoManager

```bash
$ ./telegraf --config ./plugins/inputs/leofs/leo_manager.conf --input-filter leofs --test
> leofs, host=manager_0, node=manager_0@127.0.0.1
  allocated_memory=78255445,
  allocated_memory_5min=78159025,
  ets_memory_usage=4611900,
  ets_memory_usage_5min=4632599,
  num_of_processes=223,
  num_of_processes_5min=223,
  processes_memory_usage=20201316,
  processes_memory_usage_5min=20186559,
  system_memory_usage=37172701,
  system_memory_usage_5min=37189213,
  total_memory_usage=57373373,
  total_memory_usage_5min=57374653,
  used_allocated_memory=67,
  used_allocated_memory_5min=67
  1524105758000000000
```

#### LeoStorage

```bash
$ ./telegraf --config ./plugins/inputs/leofs/leo_storage.conf --input-filter leofs --test
> leofs,host=storage_0,node=storage_0@127.0.0.1
  allocated_memory=63504384,
  allocated_memory_5min=0,
  comp_last_end_datetime=0,
  comp_last_start_datetime=0,
  comp_num_of_ongoing_targets=0,
  comp_num_of_out_of_targets=0,
  comp_num_of_pending_targets=8,
  comp_state=0,
  ets_memory_usage=3877824,
  ets_memory_usage_5min=0,
  mq_mdcr_num_of_msg_req_comp_metadata=0,
  mq_mdcr_num_of_msg_req_sync_obj=0,
  mq_num_of_msg_async_deletion_dir=0,
  mq_num_of_msg_deletion_dir=0,
  mq_num_of_msg_recovery_node=0,
  mq_num_of_msg_req_deletion_dir=0,
  num_of_active_objects=70,
  num_of_deletes=0,
  num_of_deletes_5min=0,
  num_of_processes=577,
  num_of_processes_5min=0,
  num_of_reads=1,
  num_of_reads_5min=0,
  num_of_rebalance_messages=0,
  num_of_replication_messages=0,
  num_of_sync-vnode_messages=0,
  num_of_writes=70,
  num_of_writes_5min=0,
  processes_memory_usage=20029464,
  processes_memory_usage_5min=0,
  system_memory_usage=25900472,
  system_memory_usage_5min=0,
  total_memory_usage=45920987,
  total_memory_usage_5min=0,
  total_objects=70,
  total_size=2,
  total_size_of_active_objects=2,
  used_allocated_memory=69,
  used_allocated_memory_5min=0
  1524529826000000000
```

#### LeoGateway

```
$ ./telegraf --config ./plugins/inputs/leofs/leo_gateway.conf --input-filter leofs --test
> leofs, host=gateway_0, node=gateway_0@127.0.0.1
  allocated_memory=87941120,
  allocated_memory_5min=88067672,
  count_of_cache-hit=0,
  count_of_cache-miss=0,
  ets_memory_usage=4843497,
  ets_memory_usage_5min=4841574,
  num_of_deletes=0,
  num_of_deletes_5min=0,
  num_of_processes=555,
  num_of_processes_5min=555,
  num_of_reads=0,
  num_of_reads_5min=0,
  num_of_writes=0,
  num_of_writes_5min=0,
  processes_memory_usage=17388052,
  processes_memory_usage_5min=17413928,
  system_memory_usage=49531263,
  system_memory_usage_5min=49577819,
  total_cached_size=0,
  total_memory_usage=66917393,
  total_memory_usage_5min=66989469,
  total_of_files=0,
  used_allocated_memory=69,
  used_allocated_memory_5min=69 1524105894000000000
```
