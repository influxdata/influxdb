# Mesos Input Plugin

This input plugin gathers metrics from Mesos.
For more information, please check the [Mesos Observability Metrics](http://mesos.apache.org/documentation/latest/monitoring/) page.

### Configuration:

```toml
# Telegraf plugin for gathering metrics from N Mesos masters
[[inputs.mesos]]
  ## Timeout, in ms.
  timeout = 100

  ## A list of Mesos masters.
  masters = ["http://localhost:5050"]

  ## Master metrics groups to be collected, by default, all enabled.
  master_collections = [
    "resources",
    "master",
    "system",
    "agents",
    "frameworks",
    "framework_offers",
    "tasks",
    "messages",
    "evqueue",
    "registrar",
    "allocator",
  ]

  ## A list of Mesos slaves, default is []
  # slaves = []

  ## Slave metrics groups to be collected, by default, all enabled.
  # slave_collections = [
  #   "resources",
  #   "agent",
  #   "system",
  #   "executors",
  #   "tasks",
  #   "messages",
  # ]

  ## Optional TLS Config
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = false
```

By default this plugin is not configured to gather metrics from mesos. Since a mesos cluster can be deployed in numerous ways it does not provide any default
values. User needs to specify master/slave nodes this plugin will gather metrics from.

### Measurements & Fields:

Mesos master metric groups

- resources
    - master/cpus_percent
    - master/cpus_used
    - master/cpus_total
    - master/cpus_revocable_percent
    - master/cpus_revocable_total
    - master/cpus_revocable_used
    - master/disk_percent
    - master/disk_used
    - master/disk_total
    - master/disk_revocable_percent
    - master/disk_revocable_total
    - master/disk_revocable_used
    - master/gpus_percent
    - master/gpus_used
    - master/gpus_total
    - master/gpus_revocable_percent
    - master/gpus_revocable_total
    - master/gpus_revocable_used
    - master/mem_percent
    - master/mem_used
    - master/mem_total
    - master/mem_revocable_percent
    - master/mem_revocable_total
    - master/mem_revocable_used

- master
    - master/elected
    - master/uptime_secs

- system
    - system/cpus_total
    - system/load_15min
    - system/load_5min
    - system/load_1min
    - system/mem_free_bytes
    - system/mem_total_bytes

- slaves
    - master/slave_registrations
    - master/slave_removals
    - master/slave_reregistrations
    - master/slave_shutdowns_scheduled
    - master/slave_shutdowns_canceled
    - master/slave_shutdowns_completed
    - master/slaves_active
    - master/slaves_connected
    - master/slaves_disconnected
    - master/slaves_inactive
    - master/slave_unreachable_canceled
    - master/slave_unreachable_completed
    - master/slave_unreachable_scheduled
    - master/slaves_unreachable

- frameworks
    - master/frameworks_active
    - master/frameworks_connected
    - master/frameworks_disconnected
    - master/frameworks_inactive
    - master/outstanding_offers

- framework offers
    - master/frameworks/subscribed
    - master/frameworks/calls_total
    - master/frameworks/calls
    - master/frameworks/events_total
    - master/frameworks/events
    - master/frameworks/operations_total
    - master/frameworks/operations
    - master/frameworks/tasks/active
    - master/frameworks/tasks/terminal
    - master/frameworks/offers/sent
    - master/frameworks/offers/accepted
    - master/frameworks/offers/declined
    - master/frameworks/offers/rescinded
    - master/frameworks/roles/suppressed

- tasks
    - master/tasks_error
    - master/tasks_failed
    - master/tasks_finished
    - master/tasks_killed
    - master/tasks_lost
    - master/tasks_running
    - master/tasks_staging
    - master/tasks_starting
    - master/tasks_dropped
    - master/tasks_gone
    - master/tasks_gone_by_operator
    - master/tasks_killing
    - master/tasks_unreachable

- messages
    - master/invalid_executor_to_framework_messages
    - master/invalid_framework_to_executor_messages
    - master/invalid_status_update_acknowledgements
    - master/invalid_status_updates
    - master/dropped_messages
    - master/messages_authenticate
    - master/messages_deactivate_framework
    - master/messages_decline_offers
    - master/messages_executor_to_framework
    - master/messages_exited_executor
    - master/messages_framework_to_executor
    - master/messages_kill_task
    - master/messages_launch_tasks
    - master/messages_reconcile_tasks
    - master/messages_register_framework
    - master/messages_register_slave
    - master/messages_reregister_framework
    - master/messages_reregister_slave
    - master/messages_resource_request
    - master/messages_revive_offers
    - master/messages_status_update
    - master/messages_status_update_acknowledgement
    - master/messages_unregister_framework
    - master/messages_unregister_slave
    - master/messages_update_slave
    - master/recovery_slave_removals
    - master/slave_removals/reason_registered
    - master/slave_removals/reason_unhealthy
    - master/slave_removals/reason_unregistered
    - master/valid_framework_to_executor_messages
    - master/valid_status_update_acknowledgements
    - master/valid_status_updates
    - master/task_lost/source_master/reason_invalid_offers
    - master/task_lost/source_master/reason_slave_removed
    - master/task_lost/source_slave/reason_executor_terminated
    - master/valid_executor_to_framework_messages
    - master/invalid_operation_status_update_acknowledgements
    - master/messages_operation_status_update_acknowledgement
    - master/messages_reconcile_operations
    - master/messages_suppress_offers
    - master/valid_operation_status_update_acknowledgements

- evqueue
    - master/event_queue_dispatches
    - master/event_queue_http_requests
    - master/event_queue_messages
    - master/operator_event_stream_subscribers

- registrar
    - registrar/state_fetch_ms
    - registrar/state_store_ms
    - registrar/state_store_ms/max
    - registrar/state_store_ms/min
    - registrar/state_store_ms/p50
    - registrar/state_store_ms/p90
    - registrar/state_store_ms/p95
    - registrar/state_store_ms/p99
    - registrar/state_store_ms/p999
    - registrar/state_store_ms/p9999
    - registrar/state_store_ms/count
    - registrar/log/ensemble_size
    - registrar/log/recovered
    - registrar/queued_operations
    - registrar/registry_size_bytes

- allocator
    - allocator/allocation_run_ms
    - allocator/allocation_run_ms/count
    - allocator/allocation_run_ms/max
    - allocator/allocation_run_ms/min
    - allocator/allocation_run_ms/p50
    - allocator/allocation_run_ms/p90
    - allocator/allocation_run_ms/p95
    - allocator/allocation_run_ms/p99
    - allocator/allocation_run_ms/p999
    - allocator/allocation_run_ms/p9999
    - allocator/allocation_runs
    - allocator/allocation_run_latency_ms
    - allocator/allocation_run_latency_ms/count
    - allocator/allocation_run_latency_ms/max
    - allocator/allocation_run_latency_ms/min
    - allocator/allocation_run_latency_ms/p50
    - allocator/allocation_run_latency_ms/p90
    - allocator/allocation_run_latency_ms/p95
    - allocator/allocation_run_latency_ms/p99
    - allocator/allocation_run_latency_ms/p999
    - allocator/allocation_run_latency_ms/p9999
    - allocator/roles/shares/dominant
    - allocator/event_queue_dispatches
    - allocator/offer_filters/roles/active
    - allocator/quota/roles/resources/offered_or_allocated
    - allocator/quota/roles/resources/guarantee
    - allocator/resources/cpus/offered_or_allocated
    - allocator/resources/cpus/total
    - allocator/resources/disk/offered_or_allocated
    - allocator/resources/disk/total
    - allocator/resources/mem/offered_or_allocated
    - allocator/resources/mem/total

Mesos slave metric groups
- resources
    - slave/cpus_percent
    - slave/cpus_used
    - slave/cpus_total
    - slave/cpus_revocable_percent
    - slave/cpus_revocable_total
    - slave/cpus_revocable_used
    - slave/disk_percent
    - slave/disk_used
    - slave/disk_total
    - slave/disk_revocable_percent
    - slave/disk_revocable_total
    - slave/disk_revocable_used
    - slave/gpus_percent
    - slave/gpus_used
    - slave/gpus_total,
    - slave/gpus_revocable_percent
    - slave/gpus_revocable_total
    - slave/gpus_revocable_used
    - slave/mem_percent
    - slave/mem_used
    - slave/mem_total
    - slave/mem_revocable_percent
    - slave/mem_revocable_total
    - slave/mem_revocable_used

- agent
    - slave/registered
    - slave/uptime_secs

- system
    - system/cpus_total
    - system/load_15min
    - system/load_5min
    - system/load_1min
    - system/mem_free_bytes
    - system/mem_total_bytes

- executors
    - containerizer/mesos/container_destroy_errors
    - slave/container_launch_errors
    - slave/executors_preempted
    - slave/frameworks_active
    - slave/executor_directory_max_allowed_age_secs
    - slave/executors_registering
    - slave/executors_running
    - slave/executors_terminated
    - slave/executors_terminating
    - slave/recovery_errors

- tasks
    - slave/tasks_failed
    - slave/tasks_finished
    - slave/tasks_killed
    - slave/tasks_lost
    - slave/tasks_running
    - slave/tasks_staging
    - slave/tasks_starting

- messages
    - slave/invalid_framework_messages
    - slave/invalid_status_updates
    - slave/valid_framework_messages
    - slave/valid_status_updates

### Tags:

- All master/slave measurements have the following tags:
    - server (network location of server: `host:port`)
    - url (URL origin of server: `scheme://host:port`)
    - role (master/slave)

- All master measurements have the extra tags:
	- state (leader/follower)

### Example Output:
```
$ telegraf --config ~/mesos.conf --input-filter mesos --test
* Plugin: mesos, Collection 1
mesos,role=master,state=leader,host=172.17.8.102,server=172.17.8.101
allocator/event_queue_dispatches=0,master/cpus_percent=0,
master/cpus_revocable_percent=0,master/cpus_revocable_total=0,
master/cpus_revocable_used=0,master/cpus_total=2,
master/cpus_used=0,master/disk_percent=0,master/disk_revocable_percent=0,
master/disk_revocable_total=0,master/disk_revocable_used=0,master/disk_total=10823,
master/disk_used=0,master/dropped_messages=2,master/elected=1,
master/event_queue_dispatches=10,master/event_queue_http_requests=0,
master/event_queue_messages=0,master/frameworks_active=2,master/frameworks_connected=2,
master/frameworks_disconnected=0,master/frameworks_inactive=0,
master/invalid_executor_to_framework_messages=0,
master/invalid_framework_to_executor_messages=0,
master/invalid_status_update_acknowledgements=0,master/invalid_status_updates=0,master/mem_percent=0,
master/mem_revocable_percent=0,master/mem_revocable_total=0,
master/mem_revocable_used=0,master/mem_total=1002,
master/mem_used=0,master/messages_authenticate=0,
master/messages_deactivate_framework=0 ...
```

