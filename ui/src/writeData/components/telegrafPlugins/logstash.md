# Logstash Input Plugin

This plugin reads metrics exposed by
[Logstash Monitoring API](https://www.elastic.co/guide/en/logstash/current/monitoring-logstash.html).

Logstash 5 and later is supported.

### Configuration

```toml
[[inputs.logstash]]
  ## The URL of the exposed Logstash API endpoint.
  url = "http://127.0.0.1:9600"

  ## Use Logstash 5 single pipeline API, set to true when monitoring
  ## Logstash 5.
  # single_pipeline = false

  ## Enable optional collection components.  Can contain
  ## "pipelines", "process", and "jvm".
  # collect = ["pipelines", "process", "jvm"]

  ## Timeout for HTTP requests.
  # timeout = "5s"

  ## Optional HTTP Basic Auth credentials.
  # username = "username"
  # password = "pa$$word"

  ## Optional TLS Config.
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"

  ## Use TLS but skip chain & host verification.
  # insecure_skip_verify = false

  ## Optional HTTP headers.
  # [inputs.logstash.headers]
  #   "X-Special-Header" = "Special-Value"
```

### Metrics

- logstash_jvm
  - tags:
    - node_id
    - node_name
    - node_host
    - node_version
  - fields:
    - threads_peak_count
    - mem_pools_survivor_peak_max_in_bytes
    - mem_pools_survivor_max_in_bytes
    - mem_pools_old_peak_used_in_bytes
    - mem_pools_young_used_in_bytes
    - mem_non_heap_committed_in_bytes
    - threads_count
    - mem_pools_old_committed_in_bytes
    - mem_pools_young_peak_max_in_bytes
    - mem_heap_used_percent
    - gc_collectors_young_collection_time_in_millis
    - mem_pools_survivor_peak_used_in_bytes
    - mem_pools_young_committed_in_bytes
    - gc_collectors_old_collection_time_in_millis
    - gc_collectors_old_collection_count
    - mem_pools_survivor_used_in_bytes
    - mem_pools_old_used_in_bytes
    - mem_pools_young_max_in_bytes
    - mem_heap_max_in_bytes
    - mem_non_heap_used_in_bytes
    - mem_pools_survivor_committed_in_bytes
    - mem_pools_old_max_in_bytes
    - mem_heap_committed_in_bytes
    - mem_pools_old_peak_max_in_bytes
    - mem_pools_young_peak_used_in_bytes
    - mem_heap_used_in_bytes
    - gc_collectors_young_collection_count
    - uptime_in_millis

+ logstash_process
  - tags:
    - node_id
    - node_name
    - source
    - node_version
  - fields:
    - open_file_descriptors
    - cpu_load_average_1m
    - cpu_load_average_5m
    - cpu_load_average_15m
    - cpu_total_in_millis
    - cpu_percent
    - peak_open_file_descriptors
    - max_file_descriptors
    - mem_total_virtual_in_bytes
    - mem_total_virtual_in_bytes

- logstash_events
  - tags:
    - node_id
    - node_name
    - source
    - node_version
    - pipeline (for Logstash 6+)
  - fields:
    - queue_push_duration_in_millis
    - duration_in_millis
    - in
    - filtered
    - out

+ logstash_plugins
  - tags:
    - node_id
    - node_name
    - source
    - node_version
    - pipeline (for Logstash 6+)
    - plugin_id
    - plugin_name
    - plugin_type
  - fields:
    - queue_push_duration_in_millis (for input plugins only)
    - duration_in_millis
    - in
    - out

- logstash_queue
  - tags:
    - node_id
    - node_name
    - source
    - node_version
    - pipeline (for Logstash 6+)
    - queue_type
  - fields:
    - events
    - free_space_in_bytes
    - max_queue_size_in_bytes
    - max_unread_events
    - page_capacity_in_bytes
    - queue_size_in_bytes

### Example Output

```
logstash_jvm,node_id=3da53ed0-a946-4a33-9cdb-33013f2273f6,node_name=debian-stretch-logstash6.virt,node_version=6.8.1,source=debian-stretch-logstash6.virt gc_collectors_old_collection_count=2,gc_collectors_old_collection_time_in_millis=100,gc_collectors_young_collection_count=26,gc_collectors_young_collection_time_in_millis=1028,mem_heap_committed_in_bytes=1056309248,mem_heap_max_in_bytes=1056309248,mem_heap_used_in_bytes=207216328,mem_heap_used_percent=19,mem_non_heap_committed_in_bytes=160878592,mem_non_heap_used_in_bytes=140838184,mem_pools_old_committed_in_bytes=899284992,mem_pools_old_max_in_bytes=899284992,mem_pools_old_peak_max_in_bytes=899284992,mem_pools_old_peak_used_in_bytes=189468088,mem_pools_old_used_in_bytes=189468088,mem_pools_survivor_committed_in_bytes=17432576,mem_pools_survivor_max_in_bytes=17432576,mem_pools_survivor_peak_max_in_bytes=17432576,mem_pools_survivor_peak_used_in_bytes=17432576,mem_pools_survivor_used_in_bytes=12572640,mem_pools_young_committed_in_bytes=139591680,mem_pools_young_max_in_bytes=139591680,mem_pools_young_peak_max_in_bytes=139591680,mem_pools_young_peak_used_in_bytes=139591680,mem_pools_young_used_in_bytes=5175600,threads_count=20,threads_peak_count=24,uptime_in_millis=739089 1566425244000000000
logstash_process,node_id=3da53ed0-a946-4a33-9cdb-33013f2273f6,node_name=debian-stretch-logstash6.virt,node_version=6.8.1,source=debian-stretch-logstash6.virt cpu_load_average_15m=0.03,cpu_load_average_1m=0.01,cpu_load_average_5m=0.04,cpu_percent=0,cpu_total_in_millis=83230,max_file_descriptors=16384,mem_total_virtual_in_bytes=3689132032,open_file_descriptors=118,peak_open_file_descriptors=118 1566425244000000000
logstash_events,node_id=3da53ed0-a946-4a33-9cdb-33013f2273f6,node_name=debian-stretch-logstash6.virt,node_version=6.8.1,pipeline=main,source=debian-stretch-logstash6.virt duration_in_millis=0,filtered=0,in=0,out=0,queue_push_duration_in_millis=0 1566425244000000000
logstash_plugins,node_id=3da53ed0-a946-4a33-9cdb-33013f2273f6,node_name=debian-stretch-logstash6.virt,node_version=6.8.1,pipeline=main,plugin_id=2807cb8610ba7854efa9159814fcf44c3dda762b43bd088403b30d42c88e69ab,plugin_name=beats,plugin_type=input,source=debian-stretch-logstash6.virt out=0,queue_push_duration_in_millis=0 1566425244000000000
logstash_plugins,node_id=3da53ed0-a946-4a33-9cdb-33013f2273f6,node_name=debian-stretch-logstash6.virt,node_version=6.8.1,pipeline=main,plugin_id=7a6c973366186a695727c73935634a00bccd52fceedf30d0746983fce572d50c,plugin_name=file,plugin_type=output,source=debian-stretch-logstash6.virt duration_in_millis=0,in=0,out=0 1566425244000000000
logstash_queue,node_id=3da53ed0-a946-4a33-9cdb-33013f2273f6,node_name=debian-stretch-logstash6.virt,node_version=6.8.1,pipeline=main,queue_type=memory,source=debian-stretch-logstash6.virt events=0 1566425244000000000
```
