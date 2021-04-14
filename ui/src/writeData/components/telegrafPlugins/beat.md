# Beat Input Plugin
The Beat plugin will collect metrics from the given Beat instances. It is
known to work with Filebeat and Kafkabeat.
### Configuration:
```toml
  ## An URL from which to read beat-formatted JSON
  ## Default is "http://127.0.0.1:5066".
  url = "http://127.0.0.1:5066"

  ## Enable collection of the listed stats
  ## An empty list means collect all. Available options are currently
  ## "beat", "libbeat", "system" and "filebeat".
  # include = ["beat", "libbeat", "filebeat"]

  ## HTTP method
  # method = "GET"

  ## Optional HTTP headers
  # headers = {"X-Special-Header" = "Special-Value"}

  ## Override HTTP "Host" header
  # host_header = "logstash.example.com"

  ## Timeout for HTTP requests
  # timeout = "5s"

  ## Optional HTTP Basic Auth credentials
  # username = "username"
  # password = "pa$$word"

  ## Optional TLS Config
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = false
```
### Measurements & Fields
- **beat**
  * Fields:
    - cpu_system_ticks
    - cpu_system_time_ms
    - cpu_total_ticks
    - cpu_total_time_ms
    - cpu_total_value
    - cpu_user_ticks
    - cpu_user_time_ms
    - info_uptime_ms
    - memstats_gc_next
    - memstats_memory_alloc
    - memstats_memory_total
    - memstats_rss
  * Tags:
    - beat_beat
    - beat_host
    - beat_id
    - beat_name
    - beat_version

- **beat_filebeat**
  * Fields:
    - events_active
    - events_added
    - events_done
    - harvester_closed
    - harvester_open_files
    - harvester_running
    - harvester_skipped
    - harvester_started
    - input_log_files_renamed
    - input_log_files_truncated
  * Tags:
    - beat_beat
    - beat_host
    - beat_id
    - beat_name
    - beat_version

- **beat_libbeat**
  * Fields:
    - config_module_running
    - config_module_starts
    - config_module_stops
    - config_reloads
    - output_events_acked
    - output_events_active
    - output_events_batches
    - output_events_dropped
    - output_events_duplicates
    - output_events_failed
    - output_events_total
    - output_type
    - output_read_bytes
    - output_read_errors
    - output_write_bytes
    - output_write_errors
    - outputs_kafka_bytes_read
    - outputs_kafka_bytes_write
    - pipeline_clients
    - pipeline_events_active
    - pipeline_events_dropped
    - pipeline_events_failed
    - pipeline_events_filtered
    - pipeline_events_published
    - pipeline_events_retry
    - pipeline_events_total
    - pipeline_queue_acked
  * Tags:
    - beat_beat
    - beat_host
    - beat_id
    - beat_name
    - beat_version

- **beat_system**
  * Field:
    - cpu_cores
    - load_1
    - load_15
    - load_5
    - load_norm_1
    - load_norm_15
    - load_norm_5
  * Tags:
    - beat_beat
    - beat_host
    - beat_id
    - beat_name
    - beat_version

### Example Output:
```
$ telegraf --input-filter beat --test

> beat,beat_beat=filebeat,beat_host=node-6,beat_id=9c1c8697-acb4-4df0-987d-28197814f788,beat_name=node-6-test,beat_version=6.4.2,host=node-6
  cpu_system_ticks=656750,cpu_system_time_ms=656750,cpu_total_ticks=5461190,cpu_total_time_ms=5461198,cpu_total_value=5461190,cpu_user_ticks=4804440,cpu_user_time_ms=4804448,info_uptime_ms=342634196,memstats_gc_next=20199584,memstats_memory_alloc=12547424,memstats_memory_total=486296424792,memstats_rss=72552448 1540316047000000000
> beat_libbeat,beat_beat=filebeat,beat_host=node-6,beat_id=9c1c8697-acb4-4df0-987d-28197814f788,beat_name=node-6-test,beat_version=6.4.2,host=node-6
  config_module_running=0,config_module_starts=0,config_module_stops=0,config_reloads=0,output_events_acked=192404,output_events_active=0,output_events_batches=1607,output_events_dropped=0,output_events_duplicates=0,output_events_failed=0,output_events_total=192404,output_read_bytes=0,output_read_errors=0,output_write_bytes=0,output_write_errors=0,outputs_kafka_bytes_read=1118528,outputs_kafka_bytes_write=48002014,pipeline_clients=1,pipeline_events_active=0,pipeline_events_dropped=0,pipeline_events_failed=0,pipeline_events_filtered=11496,pipeline_events_published=192404,pipeline_events_retry=14,pipeline_events_total=203900,pipeline_queue_acked=192404 1540316047000000000
> beat_system,beat_beat=filebeat,beat_host=node-6,beat_id=9c1c8697-acb4-4df0-987d-28197814f788,beat_name=node-6-test,beat_version=6.4.2,host=node-6
  cpu_cores=32,load_1=46.08,load_15=49.82,load_5=47.88,load_norm_1=1.44,load_norm_15=1.5569,load_norm_5=1.4963 1540316047000000000
> beat_filebeat,beat_beat=filebeat,beat_host=node-6,beat_id=9c1c8697-acb4-4df0-987d-28197814f788,beat_name=node-6-test,beat_version=6.4.2,host=node-6
  events_active=0,events_added=3223,events_done=3223,harvester_closed=0,harvester_open_files=0,harvester_running=0,harvester_skipped=0,harvester_started=0,input_log_files_renamed=0,input_log_files_truncated=0 1540320286000000000
```
