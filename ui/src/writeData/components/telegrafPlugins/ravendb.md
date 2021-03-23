# RavenDB Input Plugin

Reads metrics from RavenDB servers via monitoring endpoints APIs.

Requires RavenDB Server 5.2+.

### Configuration

The following is an example config for RavenDB. **Note:** The client certificate used should have `Operator` permissions on the cluster.

```toml
[[inputs.ravendb]]
  ## Node URL and port that RavenDB is listening on
  url = "https://localhost:8080"

  ## RavenDB X509 client certificate setup
  tls_cert = "/etc/telegraf/raven.crt"
  tls_key = "/etc/telegraf/raven.key"

  ## Optional request timeout
  ##
  ## Timeout, specifies the amount of time to wait
  ## for a server's response headers after fully writing the request and 
  ## time limit for requests made by this client
  # timeout = "5s"

  ## List of statistics which are collected
  # At least one is required
  # Allowed values: server, databases, indexes, collections
  #  
  # stats_include = ["server", "databases", "indexes", "collections"]

  ## List of db where database stats are collected
  ## If empty, all db are concerned
  # db_stats_dbs = []

  ## List of db where index status are collected
  ## If empty, all indexes from all db are concerned
  # index_stats_dbs = []
  
  ## List of db where collection status are collected
  ## If empty, all collections from all db are concerned
  # collection_stats_dbs = []
```

### Metrics

- ravendb_server
  - tags:
    - url
    - node_tag
    - cluster_id
    - public_server_url (optional)  
  - fields:
    - backup_current_number_of_running_backups
    - backup_max_number_of_concurrent_backups
    - certificate_server_certificate_expiration_left_in_sec (optional)
    - certificate_well_known_admin_certificates (optional, separated by ';')
    - cluster_current_term
    - cluster_index      
    - cluster_node_state
      - 0 -> Passive
      - 1 -> Candidate
      - 2 -> Follower
      - 3 -> LeaderElect
      - 4 -> Leader
    - config_public_tcp_server_urls (optional, separated by ';')
    - config_server_urls
    - config_tcp_server_urls (optional, separated by ';')
    - cpu_assigned_processor_count
    - cpu_machine_usage
    - cpu_machine_io_wait (optional)
    - cpu_process_usage
    - cpu_processor_count
    - cpu_thread_pool_available_worker_threads
    - cpu_thread_pool_available_completion_port_threads
    - databases_loaded_count
    - databases_total_count
    - disk_remaining_storage_space_percentage
    - disk_system_store_used_data_file_size_in_mb
    - disk_system_store_total_data_file_size_in_mb
    - disk_total_free_space_in_mb
    - license_expiration_left_in_sec (optional)
    - license_max_cores
    - license_type
    - license_utilized_cpu_cores
    - memory_allocated_in_mb  
    - memory_installed_in_mb
    - memory_low_memory_severity
      - 0 -> None
      - 1 -> Low
      - 2 -> Extremely Low
    - memory_physical_in_mb
    - memory_total_dirty_in_mb
    - memory_total_swap_size_in_mb
    - memory_total_swap_usage_in_mb
    - memory_working_set_swap_usage_in_mb
    - network_concurrent_requests_count
    - network_last_authorized_non_cluster_admin_request_time_in_sec (optional)
    - network_last_request_time_in_sec (optional)
    - network_requests_per_sec
    - network_tcp_active_connections
    - network_total_requests
    - server_full_version
    - server_process_id
    - server_version
    - uptime_in_sec
  
- ravendb_databases
  - tags:
    - url
    - database_name
    - database_id
    - node_tag
    - public_server_url (optional)
  - fields:
    - counts_alerts
    - counts_attachments
    - counts_documents
    - counts_performance_hints
    - counts_rehabs
    - counts_replication_factor
    - counts_revisions
    - counts_unique_attachments
    - statistics_doc_puts_per_sec
    - statistics_map_index_indexes_per_sec
    - statistics_map_reduce_index_mapped_per_sec
    - statistics_map_reduce_index_reduced_per_sec
    - statistics_request_average_duration_in_ms
    - statistics_requests_count
    - statistics_requests_per_sec
    - indexes_auto_count
    - indexes_count
    - indexes_disabled_count
    - indexes_errors_count
    - indexes_errored_count
    - indexes_idle_count
    - indexes_stale_count
    - indexes_static_count
    - storage_documents_allocated_data_file_in_mb
    - storage_documents_used_data_file_in_mb
    - storage_indexes_allocated_data_file_in_mb
    - storage_indexes_used_data_file_in_mb
    - storage_total_allocated_storage_file_in_mb
    - storage_total_free_space_in_mb
    - time_since_last_backup_in_sec (optional)
    - uptime_in_sec

- ravendb_indexes
  - tags: 
    - database_name
    - index_name
    - node_tag
    - public_server_url (optional)
    - url
  - fields
    - errors
    - is_invalid
    - lock_mode
      - Unlock
      - LockedIgnore
      - LockedError
    - mapped_per_sec
    - priority
      - Low
      - Normal
      - High
    - reduced_per_sec
    - state
      - Normal
      - Disabled
      - Idle
      - Error
    - status
      - Running
      - Paused
      - Disabled
    - time_since_last_indexing_in_sec (optional)
    - time_since_last_query_in_sec (optional)
    - type
      - None
      - AutoMap
      - AutoMapReduce
      - Map
      - MapReduce
      - Faulty
      - JavaScriptMap
      - JavaScriptMapReduce

- ravendb_collections
  - tags:
    - collection_name
    - database_name
    - node_tag
    - public_server_url (optional)
    - url
  - fields
    - documents_count
    - documents_size_in_bytes
    - revisions_size_in_bytes
    - tombstones_size_in_bytes
    - total_size_in_bytes

### Example output

```
> ravendb_server,cluster_id=07aecc42-9194-4181-999c-1c42450692c9,host=DESKTOP-2OISR6D,node_tag=A,url=http://localhost:8080 backup_current_number_of_running_backups=0i,backup_max_number_of_concurrent_backups=4i,certificate_server_certificate_expiration_left_in_sec=-1,cluster_current_term=2i,cluster_index=10i,cluster_node_state=4i,config_server_urls="http://127.0.0.1:8080",cpu_assigned_processor_count=8i,cpu_machine_usage=19.09944089456869,cpu_process_usage=0.16977205323024872,cpu_processor_count=8i,cpu_thread_pool_available_completion_port_threads=1000i,cpu_thread_pool_available_worker_threads=32763i,databases_loaded_count=1i,databases_total_count=1i,disk_remaining_storage_space_percentage=18i,disk_system_store_total_data_file_size_in_mb=35184372088832i,disk_system_store_used_data_file_size_in_mb=31379031064576i,disk_total_free_space_in_mb=42931i,license_expiration_left_in_sec=24079222.8772186,license_max_cores=256i,license_type="Enterprise",license_utilized_cpu_cores=8i,memory_allocated_in_mb=205i,memory_installed_in_mb=16384i,memory_low_memory_severity=0i,memory_physical_in_mb=16250i,memory_total_dirty_in_mb=0i,memory_total_swap_size_in_mb=0i,memory_total_swap_usage_in_mb=0i,memory_working_set_swap_usage_in_mb=0i,network_concurrent_requests_count=1i,network_last_request_time_in_sec=0.0058717,network_requests_per_sec=0.09916543455308825,network_tcp_active_connections=128i,network_total_requests=10i,server_full_version="5.2.0-custom-52",server_process_id=31044i,server_version="5.2",uptime_in_sec=56i 1613027977000000000
> ravendb_databases,database_id=ced0edba-8f80-48b8-8e81-c3d2c6748ec3,database_name=db1,host=DESKTOP-2OISR6D,node_tag=A,url=http://localhost:8080 counts_alerts=0i,counts_attachments=17i,counts_documents=1059i,counts_performance_hints=0i,counts_rehabs=0i,counts_replication_factor=1i,counts_revisions=5475i,counts_unique_attachments=17i,indexes_auto_count=0i,indexes_count=7i,indexes_disabled_count=0i,indexes_errored_count=0i,indexes_errors_count=0i,indexes_idle_count=0i,indexes_stale_count=0i,indexes_static_count=7i,statistics_doc_puts_per_sec=0,statistics_map_index_indexes_per_sec=0,statistics_map_reduce_index_mapped_per_sec=0,statistics_map_reduce_index_reduced_per_sec=0,statistics_request_average_duration_in_ms=0,statistics_requests_count=0i,statistics_requests_per_sec=0,storage_documents_allocated_data_file_in_mb=140737488355328i,storage_documents_used_data_file_in_mb=74741020884992i,storage_indexes_allocated_data_file_in_mb=175921860444160i,storage_indexes_used_data_file_in_mb=120722940755968i,storage_total_allocated_storage_file_in_mb=325455441821696i,storage_total_free_space_in_mb=42931i,uptime_in_sec=54 1613027977000000000
> ravendb_indexes,database_name=db1,host=DESKTOP-2OISR6D,index_name=Orders/Totals,node_tag=A,url=http://localhost:8080 errors=0i,is_invalid=false,lock_mode="Unlock",mapped_per_sec=0,priority="Normal",reduced_per_sec=0,state="Normal",status="Running",time_since_last_indexing_in_sec=45.4256655,time_since_last_query_in_sec=45.4304202,type="Map" 1613027977000000000
> ravendb_collections,collection_name=@hilo,database_name=db1,host=DESKTOP-2OISR6D,node_tag=A,url=http://localhost:8080 documents_count=8i,documents_size_in_bytes=122880i,revisions_size_in_bytes=0i,tombstones_size_in_bytes=122880i,total_size_in_bytes=245760i 1613027977000000000
```

### Contributors

- Marcin Lewandowski (https://github.com/ml054/)
- Casey Barton (https://github.com/bartoncasey)