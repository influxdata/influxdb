# Aerospike Input Plugin

The aerospike plugin queries aerospike server(s) and get node statistics & stats for
all the configured namespaces.

For what the measurements mean, please consult the [Aerospike Metrics Reference Docs](http://www.aerospike.com/docs/reference/metrics).

The metric names, to make it less complicated in querying, have replaced all `-` with `_` as Aerospike metrics come in both forms (no idea why).

All metrics are attempted to be cast to integers, then booleans, then strings.

### Configuration:
```toml
# Read stats from aerospike server(s)
[[inputs.aerospike]]
  ## Aerospike servers to connect to (with port)
  ## This plugin will query all namespaces the aerospike
  ## server has configured and get stats for them.
  servers = ["localhost:3000"]

  # username = "telegraf"
  # password = "pa$$word"

  ## Optional TLS Config
  # enable_tls = false
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"
  ## If false, skip chain & host verification
  # insecure_skip_verify = true
```

### Measurements:

The aerospike metrics are under two measurement names:

***aerospike_node***: These are the aerospike **node** measurements, which are
available from the aerospike `statistics` command.

      ie,
      ```
        telnet localhost 3003
        statistics
        ...
      ```

***aerospike_namespace***: These are aerospike namespace measurements, which
are available from the aerospike `namespace/<namespace_name>` command.

      ie,
      ```
        telnet localhost 3003
        namespaces
        <namespace_1>;<namespace_2>;etc.
        namespace/<namespace_name>
        ...
      ```

### Tags:

All measurements have tags:

- aerospike_host
- node_name

Namespace metrics have tags:

- namespace_name

### Example Output:

```
% telegraf --input-filter aerospike --test
> aerospike_node,aerospike_host=localhost:3000,node_name="BB9020011AC4202" batch_error=0i,batch_index_complete=0i,batch_index_created_buffers=0i,batch_index_destroyed_buffers=0i,batch_index_error=0i,batch_index_huge_buffers=0i,batch_index_initiate=0i,batch_index_queue="0:0,0:0,0:0,0:0",batch_index_timeout=0i,batch_index_unused_buffers=0i,batch_initiate=0i,batch_queue=0i,batch_timeout=0i,client_connections=6i,cluster_integrity=true,cluster_key="8AF422E05281249E",cluster_size=1i,delete_queue=0i,demarshal_error=0i,early_tsvc_batch_sub_error=0i,early_tsvc_client_error=0i,early_tsvc_udf_sub_error=0i,fabric_connections=16i,fabric_msgs_rcvd=0i,fabric_msgs_sent=0i,heartbeat_connections=0i,heartbeat_received_foreign=0i,heartbeat_received_self=0i,info_complete=47i,info_queue=0i,migrate_allowed=true,migrate_partitions_remaining=0i,migrate_progress_recv=0i,migrate_progress_send=0i,objects=0i,paxos_principal="BB9020011AC4202",proxy_in_progress=0i,proxy_retry=0i,query_long_running=0i,query_short_running=0i,reaped_fds=0i,record_refs=0i,rw_in_progress=0i,scans_active=0i,sindex_gc_activity_dur=0i,sindex_gc_garbage_cleaned=0i,sindex_gc_garbage_found=0i,sindex_gc_inactivity_dur=0i,sindex_gc_list_creation_time=0i,sindex_gc_list_deletion_time=0i,sindex_gc_locktimedout=0i,sindex_gc_objects_validated=0i,sindex_ucgarbage_found=0i,sub_objects=0i,system_free_mem_pct=92i,system_swapping=false,tsvc_queue=0i,uptime=1457i 1468923222000000000
> aerospike_namespace,aerospike_host=localhost:3000,namespace=test,node_name="BB9020011AC4202" allow_nonxdr_writes=true,allow_xdr_writes=true,available_bin_names=32768i,batch_sub_proxy_complete=0i,batch_sub_proxy_error=0i,batch_sub_proxy_timeout=0i,batch_sub_read_error=0i,batch_sub_read_not_found=0i,batch_sub_read_success=0i,batch_sub_read_timeout=0i,batch_sub_tsvc_error=0i,batch_sub_tsvc_timeout=0i,client_delete_error=0i,client_delete_not_found=0i,client_delete_success=0i,client_delete_timeout=0i,client_lang_delete_success=0i,client_lang_error=0i,client_lang_read_success=0i,client_lang_write_success=0i,client_proxy_complete=0i,client_proxy_error=0i,client_proxy_timeout=0i,client_read_error=0i,client_read_not_found=0i,client_read_success=0i,client_read_timeout=0i,client_tsvc_error=0i,client_tsvc_timeout=0i,client_udf_complete=0i,client_udf_error=0i,client_udf_timeout=0i,client_write_error=0i,client_write_success=0i,client_write_timeout=0i,cold_start_evict_ttl=4294967295i,conflict_resolution_policy="generation",current_time=206619222i,data_in_index=false,default_ttl=432000i,device_available_pct=99i,device_free_pct=100i,device_total_bytes=4294967296i,device_used_bytes=0i,disallow_null_setname=false,enable_benchmarks_batch_sub=false,enable_benchmarks_read=false,enable_benchmarks_storage=false,enable_benchmarks_udf=false,enable_benchmarks_udf_sub=false,enable_benchmarks_write=false,enable_hist_proxy=false,enable_xdr=false,evict_hist_buckets=10000i,evict_tenths_pct=5i,evict_ttl=0i,evicted_objects=0i,expired_objects=0i,fail_generation=0i,fail_key_busy=0i,fail_record_too_big=0i,fail_xdr_forbidden=0i,geo2dsphere_within.earth_radius_meters=6371000i,geo2dsphere_within.level_mod=1i,geo2dsphere_within.max_cells=12i,geo2dsphere_within.max_level=30i,geo2dsphere_within.min_level=1i,geo2dsphere_within.strict=true,geo_region_query_cells=0i,geo_region_query_falsepos=0i,geo_region_query_points=0i,geo_region_query_reqs=0i,high_water_disk_pct=50i,high_water_memory_pct=60i,hwm_breached=false,ldt_enabled=false,ldt_gc_rate=0i,ldt_page_size=8192i,master_objects=0i,master_sub_objects=0i,max_ttl=315360000i,max_void_time=0i,memory_free_pct=100i,memory_size=1073741824i,memory_used_bytes=0i,memory_used_data_bytes=0i,memory_used_index_bytes=0i,memory_used_sindex_bytes=0i,migrate_order=5i,migrate_record_receives=0i,migrate_record_retransmits=0i,migrate_records_skipped=0i,migrate_records_transmitted=0i,migrate_rx_instances=0i,migrate_rx_partitions_active=0i,migrate_rx_partitions_initial=0i,migrate_rx_partitions_remaining=0i,migrate_sleep=1i,migrate_tx_instances=0i,migrate_tx_partitions_active=0i,migrate_tx_partitions_imbalance=0i,migrate_tx_partitions_initial=0i,migrate_tx_partitions_remaining=0i,non_expirable_objects=0i,ns_forward_xdr_writes=false,nsup_cycle_duration=0i,nsup_cycle_sleep_pct=0i,objects=0i,prole_objects=0i,prole_sub_objects=0i,query_agg=0i,query_agg_abort=0i,query_agg_avg_rec_count=0i,query_agg_error=0i,query_agg_success=0i,query_fail=0i,query_long_queue_full=0i,query_long_reqs=0i,query_lookup_abort=0i,query_lookup_avg_rec_count=0i,query_lookup_error=0i,query_lookup_success=0i,query_lookups=0i,query_reqs=0i,query_short_queue_full=0i,query_short_reqs=0i,query_udf_bg_failure=0i,query_udf_bg_success=0i,read_consistency_level_override="off",repl_factor=1i,scan_aggr_abort=0i,scan_aggr_complete=0i,scan_aggr_error=0i,scan_basic_abort=0i,scan_basic_complete=0i,scan_basic_error=0i,scan_udf_bg_abort=0i,scan_udf_bg_complete=0i,scan_udf_bg_error=0i,set_deleted_objects=0i,sets_enable_xdr=true,sindex.data_max_memory="ULONG_MAX",sindex.num_partitions=32i,single_bin=false,stop_writes=false,stop_writes_pct=90i,storage_engine="device",storage_engine.cold_start_empty=false,storage_engine.data_in_memory=true,storage_engine.defrag_lwm_pct=50i,storage_engine.defrag_queue_min=0i,storage_engine.defrag_sleep=1000i,storage_engine.defrag_startup_minimum=10i,storage_engine.disable_odirect=false,storage_engine.enable_osync=false,storage_engine.file="/opt/aerospike/data/test.dat",storage_engine.filesize=4294967296i,storage_engine.flush_max_ms=1000i,storage_engine.fsync_max_sec=0i,storage_engine.max_write_cache=67108864i,storage_engine.min_avail_pct=5i,storage_engine.post_write_queue=0i,storage_engine.scheduler_mode="null",storage_engine.write_block_size=1048576i,storage_engine.write_threads=1i,sub_objects=0i,udf_sub_lang_delete_success=0i,udf_sub_lang_error=0i,udf_sub_lang_read_success=0i,udf_sub_lang_write_success=0i,udf_sub_tsvc_error=0i,udf_sub_tsvc_timeout=0i,udf_sub_udf_complete=0i,udf_sub_udf_error=0i,udf_sub_udf_timeout=0i,write_commit_level_override="off",xdr_write_error=0i,xdr_write_success=0i,xdr_write_timeout=0i,{test}_query_hist_track_back=300i,{test}_query_hist_track_slice=10i,{test}_query_hist_track_thresholds="1,8,64",{test}_read_hist_track_back=300i,{test}_read_hist_track_slice=10i,{test}_read_hist_track_thresholds="1,8,64",{test}_udf_hist_track_back=300i,{test}_udf_hist_track_slice=10i,{test}_udf_hist_track_thresholds="1,8,64",{test}_write_hist_track_back=300i,{test}_write_hist_track_slice=10i,{test}_write_hist_track_thresholds="1,8,64" 1468923222000000000
```
