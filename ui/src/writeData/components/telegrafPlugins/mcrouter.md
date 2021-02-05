# Mcrouter Input Plugin

This plugin gathers statistics data from a Mcrouter server.

### Configuration:

```toml
# Read metrics from one or many mcrouter servers.
[[inputs.mcrouter]]
  ## An array of address to gather stats about. Specify an ip or hostname
  ## with port. ie tcp://localhost:11211, tcp://10.0.0.1:11211, etc.
  servers = ["tcp://localhost:11211", "unix:///var/run/mcrouter.sock"]

  ## Timeout for metric collections from all servers.  Minimum timeout is "1s".
  # timeout = "5s"
```

### Measurements & Fields:

The fields from this plugin are gathered in the *mcrouter* measurement.

Description of gathered fields can be found [here](https://github.com/facebook/mcrouter/wiki/Stats-list).

Fields:

* uptime
* num_servers
* num_servers_new
* num_servers_up
* num_servers_down
* num_servers_closed
* num_clients
* num_suspect_servers
* destination_batches_sum
* destination_requests_sum
* outstanding_route_get_reqs_queued
* outstanding_route_update_reqs_queued
* outstanding_route_get_avg_queue_size
* outstanding_route_update_avg_queue_size
* outstanding_route_get_avg_wait_time_sec
* outstanding_route_update_avg_wait_time_sec
* retrans_closed_connections
* destination_pending_reqs
* destination_inflight_reqs
* destination_batch_size
* asynclog_requests
* proxy_reqs_processing
* proxy_reqs_waiting
* client_queue_notify_period
* rusage_system
* rusage_user
* ps_num_minor_faults
* ps_num_major_faults
* ps_user_time_sec
* ps_system_time_sec
* ps_vsize
* ps_rss
* fibers_allocated
* fibers_pool_size
* fibers_stack_high_watermark
* successful_client_connections
* duration_us
* destination_max_pending_reqs
* destination_max_inflight_reqs
* retrans_per_kbyte_max
* cmd_get_count
* cmd_delete_out
* cmd_lease_get
* cmd_set
* cmd_get_out_all
* cmd_get_out
* cmd_lease_set_count
* cmd_other_out_all
* cmd_lease_get_out
* cmd_set_count
* cmd_lease_set_out
* cmd_delete_count
* cmd_other
* cmd_delete
* cmd_get
* cmd_lease_set
* cmd_set_out
* cmd_lease_get_count
* cmd_other_out
* cmd_lease_get_out_all
* cmd_set_out_all
* cmd_other_count
* cmd_delete_out_all
* cmd_lease_set_out_all

### Tags:

* Mcrouter measurements have the following tags:
    - server (the host name from which metrics are gathered)



### Example Output:

```
$ ./telegraf --config telegraf.conf --input-filter mcrouter --test
mcrouter,server=localhost:11211 uptime=166,num_servers=1,num_servers_new=1,num_servers_up=0,num_servers_down=0,num_servers_closed=0,num_clients=1,num_suspect_servers=0,destination_batches_sum=0,destination_requests_sum=0,outstanding_route_get_reqs_queued=0,outstanding_route_update_reqs_queued=0,outstanding_route_get_avg_queue_size=0,outstanding_route_update_avg_queue_size=0,outstanding_route_get_avg_wait_time_sec=0,outstanding_route_update_avg_wait_time_sec=0,retrans_closed_connections=0,destination_pending_reqs=0,destination_inflight_reqs=0,destination_batch_size=0,asynclog_requests=0,proxy_reqs_processing=1,proxy_reqs_waiting=0,client_queue_notify_period=0,rusage_system=0.040966,rusage_user=0.020483,ps_num_minor_faults=2490,ps_num_major_faults=11,ps_user_time_sec=0.02,ps_system_time_sec=0.04,ps_vsize=697741312,ps_rss=10563584,fibers_allocated=0,fibers_pool_size=0,fibers_stack_high_watermark=0,successful_client_connections=18,duration_us=0,destination_max_pending_reqs=0,destination_max_inflight_reqs=0,retrans_per_kbyte_max=0,cmd_get_count=0,cmd_delete_out=0,cmd_lease_get=0,cmd_set=0,cmd_get_out_all=0,cmd_get_out=0,cmd_lease_set_count=0,cmd_other_out_all=0,cmd_lease_get_out=0,cmd_set_count=0,cmd_lease_set_out=0,cmd_delete_count=0,cmd_other=0,cmd_delete=0,cmd_get=0,cmd_lease_set=0,cmd_set_out=0,cmd_lease_get_count=0,cmd_other_out=0,cmd_lease_get_out_all=0,cmd_set_out_all=0,cmd_other_count=0,cmd_delete_out_all=0,cmd_lease_set_out_all=0 1453831884664956455
```
