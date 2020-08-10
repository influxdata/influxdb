# ClickHouse Input Plugin

This plugin gathers the statistic data from [ClickHouse](https://github.com/ClickHouse/ClickHouse) server.

### Configuration
```toml
# Read metrics from one or many ClickHouse servers
[[inputs.clickhouse]]
  ## Username for authorization on ClickHouse server
  ## example: user = "default"
  username = "default"

  ## Password for authorization on ClickHouse server
  ## example: password = "super_secret"

  ## HTTP(s) timeout while getting metrics values
  ## The timeout includes connection time, any redirects, and reading the response body.
  ##   example: timeout = 1s
  # timeout = 5s

  ## List of servers for metrics scraping
  ## metrics scrape via HTTP(s) clickhouse interface
  ## https://clickhouse.tech/docs/en/interfaces/http/
  ##    example: servers = ["http://127.0.0.1:8123","https://custom-server.mdb.yandexcloud.net"]
  servers         = ["http://127.0.0.1:8123"]

  ## If "auto_discovery"" is "true" plugin tries to connect to all servers available in the cluster
  ## with using same "user:password" described in "user" and "password" parameters
  ## and get this server hostname list from "system.clusters" table
  ## see
  ## - https://clickhouse.tech/docs/en/operations/system_tables/#system-clusters
  ## - https://clickhouse.tech/docs/en/operations/server_settings/settings/#server_settings_remote_servers
  ## - https://clickhouse.tech/docs/en/operations/table_engines/distributed/
  ## - https://clickhouse.tech/docs/en/operations/table_engines/replication/#creating-replicated-tables
  ##    example: auto_discovery = false
  # auto_discovery = true

  ## Filter cluster names in "system.clusters" when "auto_discovery" is "true"
  ## when this filter present then "WHERE cluster IN (...)" filter will apply
  ## please use only full cluster names here, regexp and glob filters is not allowed
  ## for "/etc/clickhouse-server/config.d/remote.xml"
  ## <yandex>
  ##  <remote_servers>
  ##    <my-own-cluster>
  ##        <shard>
  ##          <replica><host>clickhouse-ru-1.local</host><port>9000</port></replica>
  ##          <replica><host>clickhouse-ru-2.local</host><port>9000</port></replica>
  ##        </shard>
  ##        <shard>
  ##          <replica><host>clickhouse-eu-1.local</host><port>9000</port></replica>
  ##          <replica><host>clickhouse-eu-2.local</host><port>9000</port></replica>
  ##        </shard>
  ##    </my-onw-cluster>
  ##  </remote_servers>
  ##
  ## </yandex>
  ##
  ## example: cluster_include = ["my-own-cluster"]
  # cluster_include = []

  ## Filter cluster names in "system.clusters" when "auto_discovery" is "true"
  ## when this filter present then "WHERE cluster NOT IN (...)" filter will apply
  ##    example: cluster_exclude = ["my-internal-not-discovered-cluster"]
  # cluster_exclude = []

  ## Optional TLS Config
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = false
```

### Metrics

- clickhouse_events
  - tags:
    - source (ClickHouse server hostname)
    - cluster (Name of the cluster [optional])
    - shard_num (Shard number in the cluster [optional])
  - fields:
    - all rows from [system.events][]

+ clickhouse_metrics
  - tags:
    - source (ClickHouse server hostname)
    - cluster (Name of the cluster [optional])
    - shard_num (Shard number in the cluster [optional])
  - fields:
    - all rows from [system.metrics][]

- clickhouse_asynchronous_metrics
  - tags:
    - source (ClickHouse server hostname)
    - cluster (Name of the cluster [optional])
    - shard_num (Shard number in the cluster [optional])
  - fields:
    - all rows from [system.asynchronous_metrics][]

+ clickhouse_tables
  - tags:
    - source (ClickHouse server hostname)
    - table
    - database
    - cluster (Name of the cluster [optional])
    - shard_num (Shard number in the cluster [optional])
  - fields:
    - bytes
    - parts
    - rows

### Example Output

```
clickhouse_events,cluster=test_cluster_two_shards_localhost,host=kshvakov,source=localhost,shard_num=1 read_compressed_bytes=212i,arena_alloc_chunks=35i,function_execute=85i,merge_tree_data_writer_rows=3i,rw_lock_acquired_read_locks=421i,file_open=46i,io_buffer_alloc_bytes=86451985i,inserted_bytes=196i,regexp_created=3i,real_time_microseconds=116832i,query=23i,network_receive_elapsed_microseconds=268i,merge_tree_data_writer_compressed_bytes=1080i,arena_alloc_bytes=212992i,disk_write_elapsed_microseconds=556i,inserted_rows=3i,compressed_read_buffer_bytes=81i,read_buffer_from_file_descriptor_read_bytes=148i,write_buffer_from_file_descriptor_write=47i,merge_tree_data_writer_blocks=3i,soft_page_faults=896i,hard_page_faults=7i,select_query=21i,merge_tree_data_writer_uncompressed_bytes=196i,merge_tree_data_writer_blocks_already_sorted=3i,user_time_microseconds=40196i,compressed_read_buffer_blocks=5i,write_buffer_from_file_descriptor_write_bytes=3246i,io_buffer_allocs=296i,created_write_buffer_ordinary=12i,disk_read_elapsed_microseconds=59347044i,network_send_elapsed_microseconds=1538i,context_lock=1040i,insert_query=1i,system_time_microseconds=14582i,read_buffer_from_file_descriptor_read=3i 1569421000000000000
clickhouse_asynchronous_metrics,cluster=test_cluster_two_shards_localhost,host=kshvakov,source=localhost,shard_num=1 jemalloc.metadata_thp=0i,replicas_max_relative_delay=0i,jemalloc.mapped=1803177984i,jemalloc.allocated=1724839256i,jemalloc.background_thread.run_interval=0i,jemalloc.background_thread.num_threads=0i,uncompressed_cache_cells=0i,replicas_max_absolute_delay=0i,mark_cache_bytes=0i,compiled_expression_cache_count=0i,replicas_sum_queue_size=0i,number_of_tables=35i,replicas_max_merges_in_queue=0i,replicas_max_inserts_in_queue=0i,replicas_sum_merges_in_queue=0i,replicas_max_queue_size=0i,mark_cache_files=0i,jemalloc.background_thread.num_runs=0i,jemalloc.active=1726210048i,uptime=158i,jemalloc.retained=380481536i,replicas_sum_inserts_in_queue=0i,uncompressed_cache_bytes=0i,number_of_databases=2i,jemalloc.metadata=9207704i,max_part_count_for_partition=1i,jemalloc.resident=1742442496i 1569421000000000000
clickhouse_metrics,cluster=test_cluster_two_shards_localhost,host=kshvakov,source=localhost,shard_num=1 replicated_send=0i,write=0i,ephemeral_node=0i,zoo_keeper_request=0i,distributed_files_to_insert=0i,replicated_fetch=0i,background_schedule_pool_task=0i,interserver_connection=0i,leader_replica=0i,delayed_inserts=0i,global_thread_active=41i,merge=0i,readonly_replica=0i,memory_tracking_in_background_schedule_pool=0i,memory_tracking_for_merges=0i,zoo_keeper_session=0i,context_lock_wait=0i,storage_buffer_bytes=0i,background_pool_task=0i,send_external_tables=0i,zoo_keeper_watch=0i,part_mutation=0i,disk_space_reserved_for_merge=0i,distributed_send=0i,version_integer=19014003i,local_thread=0i,replicated_checks=0i,memory_tracking=0i,memory_tracking_in_background_processing_pool=0i,leader_election=0i,revision=54425i,open_file_for_read=0i,open_file_for_write=0i,storage_buffer_rows=0i,rw_lock_waiting_readers=0i,rw_lock_waiting_writers=0i,rw_lock_active_writers=0i,local_thread_active=0i,query_preempted=0i,tcp_connection=1i,http_connection=1i,read=2i,query_thread=0i,dict_cache_requests=0i,rw_lock_active_readers=1i,global_thread=43i,query=1i 1569421000000000000
clickhouse_tables,cluster=test_cluster_two_shards_localhost,database=system,host=kshvakov,source=localhost,shard_num=1,table=trace_log bytes=754i,parts=1i,rows=1i 1569421000000000000
clickhouse_tables,cluster=test_cluster_two_shards_localhost,database=default,host=kshvakov,source=localhost,shard_num=1,table=example bytes=326i,parts=2i,rows=2i 1569421000000000000
```

[system.events]: https://clickhouse.tech/docs/en/operations/system_tables/#system_tables-events
[system.metrics]: https://clickhouse.tech/docs/en/operations/system_tables/#system_tables-metrics
[system.asynchronous_metrics]: https://clickhouse.tech/docs/en/operations/system_tables/#system_tables-asynchronous_metrics
