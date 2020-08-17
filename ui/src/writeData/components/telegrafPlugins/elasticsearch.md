# Elasticsearch Input Plugin

The [elasticsearch](https://www.elastic.co/) plugin queries endpoints to obtain
[Node Stats](https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-nodes-stats.html)
and optionally
[Cluster-Health](https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-health.html)
metrics.

In addition, the following optional queries are only made by the master node:
 [Cluster Stats](https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-stats.html)
 [Indices Stats](https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-stats.html)
 [Shard Stats](https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-stats.html)

Specific Elasticsearch endpoints that are queried:
- Node: either /_nodes/stats or /_nodes/_local/stats depending on 'local' configuration setting
- Cluster Heath:  /_cluster/health?level=indices
- Cluster Stats:  /_cluster/stats
- Indices Stats:  /_all/_stats
- Shard Stats:  /_all/_stats?level=shards

Note that specific statistics information can change between Elasticsearch versions. In general, this plugin attempts to stay as version-generic as possible by tagging high-level categories only and using a generic json parser to make unique field names of whatever statistics names are provided at the mid-low level.

### Configuration

```toml
[[inputs.elasticsearch]]
  ## specify a list of one or more Elasticsearch servers
  ## you can add username and password to your url to use basic authentication:
  ## servers = ["http://user:pass@localhost:9200"]
  servers = ["http://localhost:9200"]

  ## Timeout for HTTP requests to the elastic search server(s)
  http_timeout = "5s"

  ## When local is true (the default), the node will read only its own stats.
  ## Set local to false when you want to read the node stats from all nodes
  ## of the cluster.
  local = true

  ## Set cluster_health to true when you want to obtain cluster health stats
  cluster_health = false

  ## Adjust cluster_health_level when you want to obtain detailed health stats
  ## The options are
  ##  - indices (default)
  ##  - cluster
  # cluster_health_level = "indices"

  ## Set cluster_stats to true when you want to obtain cluster stats.
  cluster_stats = false

  ## Only gather cluster_stats from the master node. To work this require local = true
  cluster_stats_only_from_master = true

  ## Indices to collect; can be one or more indices names or _all
  indices_include = ["_all"]

  ## One of "shards", "cluster", "indices"
  ## Currently only "shards" is implemented
  indices_level = "shards"

  ## node_stats is a list of sub-stats that you want to have gathered. Valid options
  ## are "indices", "os", "process", "jvm", "thread_pool", "fs", "transport", "http",
  ## "breaker". Per default, all stats are gathered.
  # node_stats = ["jvm", "http"]

  ## HTTP Basic Authentication username and password.
  # username = ""
  # password = ""

  ## Optional TLS Config
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = false
```

### Metrics

Emitted when `cluster_health = true`:

- elasticsearch_cluster_health
  - tags:
    - name
  - fields:
    - active_primary_shards (integer)
    - active_shards (integer)
    - active_shards_percent_as_number (float)
    - delayed_unassigned_shards (integer)
    - initializing_shards (integer)
    - number_of_data_nodes (integer)
    - number_of_in_flight_fetch (integer)
    - number_of_nodes (integer)
    - number_of_pending_tasks (integer)
    - relocating_shards (integer)
    - status (string, one of green, yellow or red)
    - status_code (integer, green = 1, yellow = 2, red = 3),
    - task_max_waiting_in_queue_millis (integer)
    - timed_out (boolean)
    - unassigned_shards (integer)

Emitted when `cluster_health = true` and `cluster_health_level = "indices"`:

- elasticsearch_cluster_health_indices
  - tags:
    - index
    - name
  - fields:
    - active_primary_shards (integer)
    - active_shards (integer)
    - initializing_shards (integer)
    - number_of_replicas (integer)
    - number_of_shards (integer)
    - relocating_shards (integer)
    - status (string, one of green, yellow or red)
    - status_code (integer, green = 1, yellow = 2, red = 3),
    - unassigned_shards (integer)

Emitted when `cluster_stats = true`:

- elasticsearch_clusterstats_indices
  - tags:
    - cluster_name
    - node_name
    - status
  - fields:
    - completion_size_in_bytes (float)
    - count (float)
    - docs_count (float)
    - docs_deleted (float)
    - fielddata_evictions (float)
    - fielddata_memory_size_in_bytes (float)
    - query_cache_cache_count (float)
    - query_cache_cache_size (float)
    - query_cache_evictions (float)
    - query_cache_hit_count (float)
    - query_cache_memory_size_in_bytes (float)
    - query_cache_miss_count (float)
    - query_cache_total_count (float)
    - segments_count (float)
    - segments_doc_values_memory_in_bytes (float)
    - segments_fixed_bit_set_memory_in_bytes (float)
    - segments_index_writer_memory_in_bytes (float)
    - segments_max_unsafe_auto_id_timestamp (float)
    - segments_memory_in_bytes (float)
    - segments_norms_memory_in_bytes (float)
    - segments_points_memory_in_bytes (float)
    - segments_stored_fields_memory_in_bytes (float)
    - segments_term_vectors_memory_in_bytes (float)
    - segments_terms_memory_in_bytes (float)
    - segments_version_map_memory_in_bytes (float)
    - shards_index_primaries_avg (float)
    - shards_index_primaries_max (float)
    - shards_index_primaries_min (float)
    - shards_index_replication_avg (float)
    - shards_index_replication_max (float)
    - shards_index_replication_min (float)
    - shards_index_shards_avg (float)
    - shards_index_shards_max (float)
    - shards_index_shards_min (float)
    - shards_primaries (float)
    - shards_replication (float)
    - shards_total (float)
    - store_size_in_bytes (float)

+ elasticsearch_clusterstats_nodes
  - tags:
    - cluster_name
    - node_name
    - status
  - fields:
    - count_coordinating_only (float)
    - count_data (float)
    - count_ingest (float)
    - count_master (float)
    - count_total (float)
    - fs_available_in_bytes (float)
    - fs_free_in_bytes (float)
    - fs_total_in_bytes (float)
    - jvm_max_uptime_in_millis (float)
    - jvm_mem_heap_max_in_bytes (float)
    - jvm_mem_heap_used_in_bytes (float)
    - jvm_threads (float)
    - jvm_versions_0_count (float)
    - jvm_versions_0_version (string)
    - jvm_versions_0_vm_name (string)
    - jvm_versions_0_vm_vendor (string)
    - jvm_versions_0_vm_version (string)
    - network_types_http_types_security4 (float)
    - network_types_transport_types_security4 (float)
    - os_allocated_processors (float)
    - os_available_processors (float)
    - os_mem_free_in_bytes (float)
    - os_mem_free_percent (float)
    - os_mem_total_in_bytes (float)
    - os_mem_used_in_bytes (float)
    - os_mem_used_percent (float)
    - os_names_0_count (float)
    - os_names_0_name (string)
    - os_pretty_names_0_count (float)
    - os_pretty_names_0_pretty_name (string)
    - process_cpu_percent (float)
    - process_open_file_descriptors_avg (float)
    - process_open_file_descriptors_max (float)
    - process_open_file_descriptors_min (float)
    - versions_0 (string)

Emitted when the appropriate `node_stats` options are set.

- elasticsearch_transport
  - tags:
    - cluster_name
    - node_attribute_ml.enabled
    - node_attribute_ml.machine_memory
    - node_attribute_ml.max_open_jobs
    - node_attribute_xpack.installed
    - node_host
    - node_id
    - node_name
  - fields:
    - rx_count (float)
    - rx_size_in_bytes (float)
    - server_open (float)
    - tx_count (float)
    - tx_size_in_bytes (float)

+ elasticsearch_breakers
  - tags:
    - cluster_name
    - node_attribute_ml.enabled
    - node_attribute_ml.machine_memory
    - node_attribute_ml.max_open_jobs
    - node_attribute_xpack.installed
    - node_host
    - node_id
    - node_name
  - fields:
    - accounting_estimated_size_in_bytes (float)
    - accounting_limit_size_in_bytes (float)
    - accounting_overhead (float)
    - accounting_tripped (float)
    - fielddata_estimated_size_in_bytes (float)
    - fielddata_limit_size_in_bytes (float)
    - fielddata_overhead (float)
    - fielddata_tripped (float)
    - in_flight_requests_estimated_size_in_bytes (float)
    - in_flight_requests_limit_size_in_bytes (float)
    - in_flight_requests_overhead (float)
    - in_flight_requests_tripped (float)
    - parent_estimated_size_in_bytes (float)
    - parent_limit_size_in_bytes (float)
    - parent_overhead (float)
    - parent_tripped (float)
    - request_estimated_size_in_bytes (float)
    - request_limit_size_in_bytes (float)
    - request_overhead (float)
    - request_tripped (float)

- elasticsearch_fs
  - tags:
    - cluster_name
    - node_attribute_ml.enabled
    - node_attribute_ml.machine_memory
    - node_attribute_ml.max_open_jobs
    - node_attribute_xpack.installed
    - node_host
    - node_id
    - node_name
  - fields:
    - data_0_available_in_bytes (float)
    - data_0_free_in_bytes (float)
    - data_0_total_in_bytes (float)
    - io_stats_devices_0_operations (float)
    - io_stats_devices_0_read_kilobytes (float)
    - io_stats_devices_0_read_operations (float)
    - io_stats_devices_0_write_kilobytes (float)
    - io_stats_devices_0_write_operations (float)
    - io_stats_total_operations (float)
    - io_stats_total_read_kilobytes (float)
    - io_stats_total_read_operations (float)
    - io_stats_total_write_kilobytes (float)
    - io_stats_total_write_operations (float)
    - timestamp (float)
    - total_available_in_bytes (float)
    - total_free_in_bytes (float)
    - total_total_in_bytes (float)

+ elasticsearch_http
  - tags:
    - cluster_name
    - node_attribute_ml.enabled
    - node_attribute_ml.machine_memory
    - node_attribute_ml.max_open_jobs
    - node_attribute_xpack.installed
    - node_host
    - node_id
    - node_name
  - fields:
    - current_open (float)
    - total_opened (float)

- elasticsearch_indices
  - tags:
    - cluster_name
    - node_attribute_ml.enabled
    - node_attribute_ml.machine_memory
    - node_attribute_ml.max_open_jobs
    - node_attribute_xpack.installed
    - node_host
    - node_id
    - node_name
  - fields:
    - completion_size_in_bytes (float)
    - docs_count (float)
    - docs_deleted (float)
    - fielddata_evictions (float)
    - fielddata_memory_size_in_bytes (float)
    - flush_periodic (float)
    - flush_total (float)
    - flush_total_time_in_millis (float)
    - get_current (float)
    - get_exists_time_in_millis (float)
    - get_exists_total (float)
    - get_missing_time_in_millis (float)
    - get_missing_total (float)
    - get_time_in_millis (float)
    - get_total (float)
    - indexing_delete_current (float)
    - indexing_delete_time_in_millis (float)
    - indexing_delete_total (float)
    - indexing_index_current (float)
    - indexing_index_failed (float)
    - indexing_index_time_in_millis (float)
    - indexing_index_total (float)
    - indexing_noop_update_total (float)
    - indexing_throttle_time_in_millis (float)
    - merges_current (float)
    - merges_current_docs (float)
    - merges_current_size_in_bytes (float)
    - merges_total (float)
    - merges_total_auto_throttle_in_bytes (float)
    - merges_total_docs (float)
    - merges_total_size_in_bytes (float)
    - merges_total_stopped_time_in_millis (float)
    - merges_total_throttled_time_in_millis (float)
    - merges_total_time_in_millis (float)
    - query_cache_cache_count (float)
    - query_cache_cache_size (float)
    - query_cache_evictions (float)
    - query_cache_hit_count (float)
    - query_cache_memory_size_in_bytes (float)
    - query_cache_miss_count (float)
    - query_cache_total_count (float)
    - recovery_current_as_source (float)
    - recovery_current_as_target (float)
    - recovery_throttle_time_in_millis (float)
    - refresh_listeners (float)
    - refresh_total (float)
    - refresh_total_time_in_millis (float)
    - request_cache_evictions (float)
    - request_cache_hit_count (float)
    - request_cache_memory_size_in_bytes (float)
    - request_cache_miss_count (float)
    - search_fetch_current (float)
    - search_fetch_time_in_millis (float)
    - search_fetch_total (float)
    - search_open_contexts (float)
    - search_query_current (float)
    - search_query_time_in_millis (float)
    - search_query_total (float)
    - search_scroll_current (float)
    - search_scroll_time_in_millis (float)
    - search_scroll_total (float)
    - search_suggest_current (float)
    - search_suggest_time_in_millis (float)
    - search_suggest_total (float)
    - segments_count (float)
    - segments_doc_values_memory_in_bytes (float)
    - segments_fixed_bit_set_memory_in_bytes (float)
    - segments_index_writer_memory_in_bytes (float)
    - segments_max_unsafe_auto_id_timestamp (float)
    - segments_memory_in_bytes (float)
    - segments_norms_memory_in_bytes (float)
    - segments_points_memory_in_bytes (float)
    - segments_stored_fields_memory_in_bytes (float)
    - segments_term_vectors_memory_in_bytes (float)
    - segments_terms_memory_in_bytes (float)
    - segments_version_map_memory_in_bytes (float)
    - store_size_in_bytes (float)
    - translog_earliest_last_modified_age (float)
    - translog_operations (float)
    - translog_size_in_bytes (float)
    - translog_uncommitted_operations (float)
    - translog_uncommitted_size_in_bytes (float)
    - warmer_current (float)
    - warmer_total (float)
    - warmer_total_time_in_millis (float)

+ elasticsearch_jvm
  - tags:
    - cluster_name
    - node_attribute_ml.enabled
    - node_attribute_ml.machine_memory
    - node_attribute_ml.max_open_jobs
    - node_attribute_xpack.installed
    - node_host
    - node_id
    - node_name
  - fields:
    - buffer_pools_direct_count (float)
    - buffer_pools_direct_total_capacity_in_bytes (float)
    - buffer_pools_direct_used_in_bytes (float)
    - buffer_pools_mapped_count (float)
    - buffer_pools_mapped_total_capacity_in_bytes (float)
    - buffer_pools_mapped_used_in_bytes (float)
    - classes_current_loaded_count (float)
    - classes_total_loaded_count (float)
    - classes_total_unloaded_count (float)
    - gc_collectors_old_collection_count (float)
    - gc_collectors_old_collection_time_in_millis (float)
    - gc_collectors_young_collection_count (float)
    - gc_collectors_young_collection_time_in_millis (float)
    - mem_heap_committed_in_bytes (float)
    - mem_heap_max_in_bytes (float)
    - mem_heap_used_in_bytes (float)
    - mem_heap_used_percent (float)
    - mem_non_heap_committed_in_bytes (float)
    - mem_non_heap_used_in_bytes (float)
    - mem_pools_old_max_in_bytes (float)
    - mem_pools_old_peak_max_in_bytes (float)
    - mem_pools_old_peak_used_in_bytes (float)
    - mem_pools_old_used_in_bytes (float)
    - mem_pools_survivor_max_in_bytes (float)
    - mem_pools_survivor_peak_max_in_bytes (float)
    - mem_pools_survivor_peak_used_in_bytes (float)
    - mem_pools_survivor_used_in_bytes (float)
    - mem_pools_young_max_in_bytes (float)
    - mem_pools_young_peak_max_in_bytes (float)
    - mem_pools_young_peak_used_in_bytes (float)
    - mem_pools_young_used_in_bytes (float)
    - threads_count (float)
    - threads_peak_count (float)
    - timestamp (float)
    - uptime_in_millis (float)

- elasticsearch_os
  - tags:
    - cluster_name
    - node_attribute_ml.enabled
    - node_attribute_ml.machine_memory
    - node_attribute_ml.max_open_jobs
    - node_attribute_xpack.installed
    - node_host
    - node_id
    - node_name
  - fields:
    - cgroup_cpu_cfs_period_micros (float)
    - cgroup_cpu_cfs_quota_micros (float)
    - cgroup_cpu_stat_number_of_elapsed_periods (float)
    - cgroup_cpu_stat_number_of_times_throttled (float)
    - cgroup_cpu_stat_time_throttled_nanos (float)
    - cgroup_cpuacct_usage_nanos (float)
    - cpu_load_average_15m (float)
    - cpu_load_average_1m (float)
    - cpu_load_average_5m (float)
    - cpu_percent (float)
    - mem_free_in_bytes (float)
    - mem_free_percent (float)
    - mem_total_in_bytes (float)
    - mem_used_in_bytes (float)
    - mem_used_percent (float)
    - swap_free_in_bytes (float)
    - swap_total_in_bytes (float)
    - swap_used_in_bytes (float)
    - timestamp (float)

+ elasticsearch_process
  - tags:
    - cluster_name
    - node_attribute_ml.enabled
    - node_attribute_ml.machine_memory
    - node_attribute_ml.max_open_jobs
    - node_attribute_xpack.installed
    - node_host
    - node_id
    - node_name
  - fields:
    - cpu_percent (float)
    - cpu_total_in_millis (float)
    - max_file_descriptors (float)
    - mem_total_virtual_in_bytes (float)
    - open_file_descriptors (float)
    - timestamp (float)

- elasticsearch_thread_pool
  - tags:
    - cluster_name
    - node_attribute_ml.enabled
    - node_attribute_ml.machine_memory
    - node_attribute_ml.max_open_jobs
    - node_attribute_xpack.installed
    - node_host
    - node_id
    - node_name
  - fields:
    - analyze_active (float)
    - analyze_completed (float)
    - analyze_largest (float)
    - analyze_queue (float)
    - analyze_rejected (float)
    - analyze_threads (float)
    - ccr_active (float)
    - ccr_completed (float)
    - ccr_largest (float)
    - ccr_queue (float)
    - ccr_rejected (float)
    - ccr_threads (float)
    - fetch_shard_started_active (float)
    - fetch_shard_started_completed (float)
    - fetch_shard_started_largest (float)
    - fetch_shard_started_queue (float)
    - fetch_shard_started_rejected (float)
    - fetch_shard_started_threads (float)
    - fetch_shard_store_active (float)
    - fetch_shard_store_completed (float)
    - fetch_shard_store_largest (float)
    - fetch_shard_store_queue (float)
    - fetch_shard_store_rejected (float)
    - fetch_shard_store_threads (float)
    - flush_active (float)
    - flush_completed (float)
    - flush_largest (float)
    - flush_queue (float)
    - flush_rejected (float)
    - flush_threads (float)
    - force_merge_active (float)
    - force_merge_completed (float)
    - force_merge_largest (float)
    - force_merge_queue (float)
    - force_merge_rejected (float)
    - force_merge_threads (float)
    - generic_active (float)
    - generic_completed (float)
    - generic_largest (float)
    - generic_queue (float)
    - generic_rejected (float)
    - generic_threads (float)
    - get_active (float)
    - get_completed (float)
    - get_largest (float)
    - get_queue (float)
    - get_rejected (float)
    - get_threads (float)
    - index_active (float)
    - index_completed (float)
    - index_largest (float)
    - index_queue (float)
    - index_rejected (float)
    - index_threads (float)
    - listener_active (float)
    - listener_completed (float)
    - listener_largest (float)
    - listener_queue (float)
    - listener_rejected (float)
    - listener_threads (float)
    - management_active (float)
    - management_completed (float)
    - management_largest (float)
    - management_queue (float)
    - management_rejected (float)
    - management_threads (float)
    - ml_autodetect_active (float)
    - ml_autodetect_completed (float)
    - ml_autodetect_largest (float)
    - ml_autodetect_queue (float)
    - ml_autodetect_rejected (float)
    - ml_autodetect_threads (float)
    - ml_datafeed_active (float)
    - ml_datafeed_completed (float)
    - ml_datafeed_largest (float)
    - ml_datafeed_queue (float)
    - ml_datafeed_rejected (float)
    - ml_datafeed_threads (float)
    - ml_utility_active (float)
    - ml_utility_completed (float)
    - ml_utility_largest (float)
    - ml_utility_queue (float)
    - ml_utility_rejected (float)
    - ml_utility_threads (float)
    - refresh_active (float)
    - refresh_completed (float)
    - refresh_largest (float)
    - refresh_queue (float)
    - refresh_rejected (float)
    - refresh_threads (float)
    - rollup_indexing_active (float)
    - rollup_indexing_completed (float)
    - rollup_indexing_largest (float)
    - rollup_indexing_queue (float)
    - rollup_indexing_rejected (float)
    - rollup_indexing_threads (float)
    - search_active (float)
    - search_completed (float)
    - search_largest (float)
    - search_queue (float)
    - search_rejected (float)
    - search_threads (float)
    - search_throttled_active (float)
    - search_throttled_completed (float)
    - search_throttled_largest (float)
    - search_throttled_queue (float)
    - search_throttled_rejected (float)
    - search_throttled_threads (float)
    - security-token-key_active (float)
    - security-token-key_completed (float)
    - security-token-key_largest (float)
    - security-token-key_queue (float)
    - security-token-key_rejected (float)
    - security-token-key_threads (float)
    - snapshot_active (float)
    - snapshot_completed (float)
    - snapshot_largest (float)
    - snapshot_queue (float)
    - snapshot_rejected (float)
    - snapshot_threads (float)
    - warmer_active (float)
    - warmer_completed (float)
    - warmer_largest (float)
    - warmer_queue (float)
    - warmer_rejected (float)
    - warmer_threads (float)
    - watcher_active (float)
    - watcher_completed (float)
    - watcher_largest (float)
    - watcher_queue (float)
    - watcher_rejected (float)
    - watcher_threads (float)
    - write_active (float)
    - write_completed (float)
    - write_largest (float)
    - write_queue (float)
    - write_rejected (float)
    - write_threads (float)

Emitted when the appropriate `indices_stats` options are set.

- elasticsearch_indices_stats_(primaries|total)
  - tags:
    - index_name
  - fields:
    - completion_size_in_bytes (float)
    - docs_count (float)
    - docs_deleted (float)
    - fielddata_evictions (float)
    - fielddata_memory_size_in_bytes (float)
    - flush_periodic (float)
    - flush_total (float)
    - flush_total_time_in_millis (float)
    - get_current (float)
    - get_exists_time_in_millis (float)
    - get_exists_total (float)
    - get_missing_time_in_millis (float)
    - get_missing_total (float)
    - get_time_in_millis (float)
    - get_total (float)
    - indexing_delete_current (float)
    - indexing_delete_time_in_millis (float)
    - indexing_delete_total (float)
    - indexing_index_current (float)
    - indexing_index_failed (float)
    - indexing_index_time_in_millis (float)
    - indexing_index_total (float)
    - indexing_is_throttled (float)
    - indexing_noop_update_total (float)
    - indexing_throttle_time_in_millis (float)
    - merges_current (float)
    - merges_current_docs (float)
    - merges_current_size_in_bytes (float)
    - merges_total (float)
    - merges_total_auto_throttle_in_bytes (float)
    - merges_total_docs (float)
    - merges_total_size_in_bytes (float)
    - merges_total_stopped_time_in_millis (float)
    - merges_total_throttled_time_in_millis (float)
    - merges_total_time_in_millis (float)
    - query_cache_cache_count (float)
    - query_cache_cache_size (float)
    - query_cache_evictions (float)
    - query_cache_hit_count (float)
    - query_cache_memory_size_in_bytes (float)
    - query_cache_miss_count (float)
    - query_cache_total_count (float)
    - recovery_current_as_source (float)
    - recovery_current_as_target (float)
    - recovery_throttle_time_in_millis (float)
    - refresh_external_total (float)
    - refresh_external_total_time_in_millis (float)
    - refresh_listeners (float)
    - refresh_total (float)
    - refresh_total_time_in_millis (float)
    - request_cache_evictions (float)
    - request_cache_hit_count (float)
    - request_cache_memory_size_in_bytes (float)
    - request_cache_miss_count (float)
    - search_fetch_current (float)
    - search_fetch_time_in_millis (float)
    - search_fetch_total (float)
    - search_open_contexts (float)
    - search_query_current (float)
    - search_query_time_in_millis (float)
    - search_query_total (float)
    - search_scroll_current (float)
    - search_scroll_time_in_millis (float)
    - search_scroll_total (float)
    - search_suggest_current (float)
    - search_suggest_time_in_millis (float)
    - search_suggest_total (float)
    - segments_count (float)
    - segments_doc_values_memory_in_bytes (float)
    - segments_fixed_bit_set_memory_in_bytes (float)
    - segments_index_writer_memory_in_bytes (float)
    - segments_max_unsafe_auto_id_timestamp (float)
    - segments_memory_in_bytes (float)
    - segments_norms_memory_in_bytes (float)
    - segments_points_memory_in_bytes (float)
    - segments_stored_fields_memory_in_bytes (float)
    - segments_term_vectors_memory_in_bytes (float)
    - segments_terms_memory_in_bytes (float)
    - segments_version_map_memory_in_bytes (float)
    - store_size_in_bytes (float)
    - translog_earliest_last_modified_age (float)
    - translog_operations (float)
    - translog_size_in_bytes (float)
    - translog_uncommitted_operations (float)
    - translog_uncommitted_size_in_bytes (float)
    - warmer_current (float)
    - warmer_total (float)
    - warmer_total_time_in_millis (float)

Emitted when the appropriate `shards_stats` options are set.

- elasticsearch_indices_stats_shards_total
  - fields:
    - failed (float)
    - successful (float)
    - total (float)

- elasticsearch_indices_stats_shards
  - tags:
    - index_name
    - node_name
    - shard_name
    - type
  - fields:
    - commit_generation (float)
    - commit_num_docs (float)
    - completion_size_in_bytes (float)
    - docs_count (float)
    - docs_deleted (float)
    - fielddata_evictions (float)
    - fielddata_memory_size_in_bytes (float)
    - flush_periodic (float)
    - flush_total (float)
    - flush_total_time_in_millis (float)
    - get_current (float)
    - get_exists_time_in_millis (float)
    - get_exists_total (float)
    - get_missing_time_in_millis (float)
    - get_missing_total (float)
    - get_time_in_millis (float)
    - get_total (float)
    - indexing_delete_current (float)
    - indexing_delete_time_in_millis (float)
    - indexing_delete_total (float)
    - indexing_index_current (float)
    - indexing_index_failed (float)
    - indexing_index_time_in_millis (float)
    - indexing_index_total (float)
    - indexing_is_throttled (bool)
    - indexing_noop_update_total (float)
    - indexing_throttle_time_in_millis (float)
    - merges_current (float)
    - merges_current_docs (float)
    - merges_current_size_in_bytes (float)
    - merges_total (float)
    - merges_total_auto_throttle_in_bytes (float)
    - merges_total_docs (float)
    - merges_total_size_in_bytes (float)
    - merges_total_stopped_time_in_millis (float)
    - merges_total_throttled_time_in_millis (float)
    - merges_total_time_in_millis (float)
    - query_cache_cache_count (float)
    - query_cache_cache_size (float)
    - query_cache_evictions (float)
    - query_cache_hit_count (float)
    - query_cache_memory_size_in_bytes (float)
    - query_cache_miss_count (float)
    - query_cache_total_count (float)
    - recovery_current_as_source (float)
    - recovery_current_as_target (float)
    - recovery_throttle_time_in_millis (float)
    - refresh_external_total (float)
    - refresh_external_total_time_in_millis (float)
    - refresh_listeners (float)
    - refresh_total (float)
    - refresh_total_time_in_millis (float)
    - request_cache_evictions (float)
    - request_cache_hit_count (float)
    - request_cache_memory_size_in_bytes (float)
    - request_cache_miss_count (float)
    - retention_leases_primary_term (float)
    - retention_leases_version (float)
    - routing_state (int) (UNASSIGNED = 1, INITIALIZING = 2, STARTED = 3, RELOCATING = 4, other = 0)
    - search_fetch_current (float)
    - search_fetch_time_in_millis (float)
    - search_fetch_total (float)
    - search_open_contexts (float)
    - search_query_current (float)
    - search_query_time_in_millis (float)
    - search_query_total (float)
    - search_scroll_current (float)
    - search_scroll_time_in_millis (float)
    - search_scroll_total (float)
    - search_suggest_current (float)
    - search_suggest_time_in_millis (float)
    - search_suggest_total (float)
    - segments_count (float)
    - segments_doc_values_memory_in_bytes (float)
    - segments_fixed_bit_set_memory_in_bytes (float)
    - segments_index_writer_memory_in_bytes (float)
    - segments_max_unsafe_auto_id_timestamp (float)
    - segments_memory_in_bytes (float)
    - segments_norms_memory_in_bytes (float)
    - segments_points_memory_in_bytes (float)
    - segments_stored_fields_memory_in_bytes (float)
    - segments_term_vectors_memory_in_bytes (float)
    - segments_terms_memory_in_bytes (float)
    - segments_version_map_memory_in_bytes (float)
    - seq_no_global_checkpoint (float)
    - seq_no_local_checkpoint (float)
    - seq_no_max_seq_no (float)
    - shard_path_is_custom_data_path (bool)
    - store_size_in_bytes (float)
    - translog_earliest_last_modified_age (float)
    - translog_operations (float)
    - translog_size_in_bytes (float)
    - translog_uncommitted_operations (float)
    - translog_uncommitted_size_in_bytes (float)
    - warmer_current (float)
    - warmer_total (float)
    - warmer_total_time_in_millis (float)
