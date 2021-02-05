# Aurora Input Plugin

The Aurora Input Plugin gathers metrics from [Apache Aurora](https://aurora.apache.org/) schedulers.

For monitoring recommendations reference [Monitoring your Aurora cluster](https://aurora.apache.org/documentation/latest/operations/monitoring/)

### Configuration:

```toml
[[inputs.aurora]]
  ## Schedulers are the base addresses of your Aurora Schedulers
  schedulers = ["http://127.0.0.1:8081"]

  ## Set of role types to collect metrics from.
  ##
  ## The scheduler roles are checked each interval by contacting the
  ## scheduler nodes; zookeeper is not contacted.
  # roles = ["leader", "follower"]

  ## Timeout is the max time for total network operations.
  # timeout = "5s"

  ## Username and password are sent using HTTP Basic Auth.
  # username = "username"
  # password = "pa$$word"

  ## Optional TLS Config
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = false
```

### Metrics:

- aurora
  - tags:
    - scheduler (URL of scheduler)
    - role (leader or follower)
  - fields:
    - Numeric metrics are collected from the `/vars` endpoint; string fields
      are not gathered.


### Troubleshooting:

Check the Scheduler role, the leader will return a 200 status:
```
curl -v http://127.0.0.1:8081/leaderhealth
```

Get available metrics:
```
curl http://127.0.0.1:8081/vars
```

### Example Output:

The example output below has been trimmed.
```
> aurora,role=leader,scheduler=http://debian-stretch-aurora-coordinator-3.virt:8081 CronBatchWorker_batch_locked_events=0i,CronBatchWorker_batch_locked_events_per_sec=0,CronBatchWorker_batch_locked_nanos_per_event=0,CronBatchWorker_batch_locked_nanos_total=0i,CronBatchWorker_batch_locked_nanos_total_per_sec=0,CronBatchWorker_batch_unlocked_events=0i,CronBatchWorker_batch_unlocked_events_per_sec=0,CronBatchWorker_batch_unlocked_nanos_per_event=0,CronBatchWorker_batch_unlocked_nanos_total=0i,CronBatchWorker_batch_unlocked_nanos_total_per_sec=0,CronBatchWorker_batches_processed=0i,CronBatchWorker_items_processed=0i,CronBatchWorker_last_processed_batch_size=0i,CronBatchWorker_queue_size=0i,TaskEventBatchWorker_batch_locked_events=0i,TaskEventBatchWorker_batch_locked_events_per_sec=0,TaskEventBatchWorker_batch_locked_nanos_per_event=0,TaskEventBatchWorker_batch_locked_nanos_total=0i,TaskEventBatchWorker_batch_locked_nanos_total_per_sec=0,TaskEventBatchWorker_batch_unlocked_events=0i,TaskEventBatchWorker_batch_unlocked_events_per_sec=0,TaskEventBatchWorker_batch_unlocked_nanos_per_event=0,TaskEventBatchWorker_batch_unlocked_nanos_total=0i,TaskEventBatchWorker_batch_unlocked_nanos_total_per_sec=0,TaskEventBatchWorker_batches_processed=0i,TaskEventBatchWorker_items_processed=0i,TaskEventBatchWorker_last_processed_batch_size=0i,TaskEventBatchWorker_queue_size=0i,TaskGroupBatchWorker_batch_locked_events=0i,TaskGroupBatchWorker_batch_locked_events_per_sec=0,TaskGroupBatchWorker_batch_locked_nanos_per_event=0,TaskGroupBatchWorker_batch_locked_nanos_total=0i,TaskGroupBatchWorker_batch_locked_nanos_total_per_sec=0,TaskGroupBatchWorker_batch_unlocked_events=0i,TaskGroupBatchWorker_batch_unlocked_events_per_sec=0,TaskGroupBatchWorker_batch_unlocked_nanos_per_event=0,TaskGroupBatchWorker_batch_unlocked_nanos_total=0i,TaskGroupBatchWorker_batch_unlocked_nanos_total_per_sec=0,TaskGroupBatchWorker_batches_processed=0i,TaskGroupBatchWorker_items_processed=0i,TaskGroupBatchWorker_last_processed_batch_size=0i,TaskGroupBatchWorker_queue_size=0i,assigner_launch_failures=0i,async_executor_uncaught_exceptions=0i,async_tasks_completed=1i,cron_job_collisions=0i,cron_job_concurrent_runs=0i,cron_job_launch_failures=0i,cron_job_misfires=0i,cron_job_parse_failures=0i,cron_job_triggers=0i,cron_jobs_loaded=1i,empty_slots_dedicated_large=0i,empty_slots_dedicated_medium=0i,empty_slots_dedicated_revocable_large=0i,empty_slots_dedicated_revocable_medium=0i,empty_slots_dedicated_revocable_small=0i,empty_slots_dedicated_revocable_xlarge=0i,empty_slots_dedicated_small=0i,empty_slots_dedicated_xlarge=0i,empty_slots_large=0i,empty_slots_medium=0i,empty_slots_revocable_large=0i,empty_slots_revocable_medium=0i,empty_slots_revocable_small=0i,empty_slots_revocable_xlarge=0i,empty_slots_small=0i,empty_slots_xlarge=0i,event_bus_dead_events=0i,event_bus_exceptions=1i,framework_registered=1i,globally_banned_offers_size=0i,http_200_responses_events=55i,http_200_responses_events_per_sec=0,http_200_responses_nanos_per_event=0,http_200_responses_nanos_total=310416694i,http_200_responses_nanos_total_per_sec=0,job_update_delete_errors=0i,job_update_recovery_errors=0i,job_update_state_change_errors=0i,job_update_store_delete_all_events=1i,job_update_store_delete_all_events_per_sec=0,job_update_store_delete_all_nanos_per_event=0,job_update_store_delete_all_nanos_total=1227254i,job_update_store_delete_all_nanos_total_per_sec=0,job_update_store_fetch_details_query_events=74i,job_update_store_fetch_details_query_events_per_sec=0,job_update_store_fetch_details_query_nanos_per_event=0,job_update_store_fetch_details_query_nanos_total=24643149i,job_update_store_fetch_details_query_nanos_total_per_sec=0,job_update_store_prune_history_events=59i,job_update_store_prune_history_events_per_sec=0,job_update_store_prune_history_nanos_per_event=0,job_update_store_prune_history_nanos_total=262868218i,job_update_store_prune_history_nanos_total_per_sec=0,job_updates_pruned=0i,jvm_available_processors=2i,jvm_class_loaded_count=6707i,jvm_class_total_loaded_count=6732i,jvm_class_unloaded_count=25i,jvm_gc_PS_MarkSweep_collection_count=2i,jvm_gc_PS_MarkSweep_collection_time_ms=223i,jvm_gc_PS_Scavenge_collection_count=27i,jvm_gc_PS_Scavenge_collection_time_ms=1691i,jvm_gc_collection_count=29i,jvm_gc_collection_time_ms=1914i,jvm_memory_free_mb=65i,jvm_memory_heap_mb_committed=157i,jvm_memory_heap_mb_max=446i,jvm_memory_heap_mb_used=91i,jvm_memory_max_mb=446i,jvm_memory_mb_total=157i,jvm_memory_non_heap_mb_committed=50i,jvm_memory_non_heap_mb_max=0i,jvm_memory_non_heap_mb_used=49i,jvm_threads_active=47i,jvm_threads_daemon=28i,jvm_threads_peak=48i,jvm_threads_started=62i,jvm_time_ms=1526530686927i,jvm_uptime_secs=79947i,log_entry_serialize_events=16i,log_entry_serialize_events_per_sec=0,log_entry_serialize_nanos_per_event=0,log_entry_serialize_nanos_total=4815321i,log_entry_serialize_nanos_total_per_sec=0,log_manager_append_events=16i,log_manager_append_events_per_sec=0,log_manager_append_nanos_per_event=0,log_manager_append_nanos_total=506453428i,log_manager_append_nanos_total_per_sec=0,log_manager_deflate_events=14i,log_manager_deflate_events_per_sec=0,log_manager_deflate_nanos_per_event=0,log_manager_deflate_nanos_total=21010565i,log_manager_deflate_nanos_total_per_sec=0 1526530687000000000
```
