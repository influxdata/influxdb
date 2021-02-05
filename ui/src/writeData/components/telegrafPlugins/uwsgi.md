# uWSGI Input Plugin

The uWSGI input plugin gathers metrics about uWSGI using its [Stats Server](https://uwsgi-docs.readthedocs.io/en/latest/StatsServer.html).

### Configuration

```toml
[[inputs.uwsgi]]
  ## List with urls of uWSGI Stats servers. Url must match pattern:
  ## scheme://address[:port]
  ##
  ## For example:
  ## servers = ["tcp://localhost:5050", "http://localhost:1717", "unix:///tmp/statsock"]
  servers = ["tcp://127.0.0.1:1717"]

  ## General connection timeout
  # timeout = "5s"
```


### Metrics:

 - uwsgi_overview
  - tags:
    - source
    - uid
    - gid
    - version
  - fields:
    - listen_queue
    - listen_queue_errors
    - signal_queue
    - load
    - pid

+ uwsgi_workers
  - tags:
    - worker_id
    - source
  - fields:
    - requests
    - accepting
    - delta_request
    - exceptions
    - harakiri_count
    - pid
    - signals
    - signal_queue
    - status
    - rss
    - vsz
    - running_time
    - last_spawn
    - respawn_count
    - tx
    - avg_rt

- uwsgi_apps
  - tags:
    - app_id
    - worker_id
    - source
  - fields:
    - modifier1
    - requests
    - startup_time
    - exceptions

+ uwsgi_cores
  - tags:
    - core_id
    - worker_id
    - source
  - fields:
    - requests
    - static_requests
    - routed_requests
    - offloaded_requests
    - write_errors
    - read_errors
    - in_request 


### Example Output:

```
uwsgi_overview,gid=0,uid=0,source=172.17.0.2,version=2.0.18 listen_queue=0i,listen_queue_errors=0i,load=0i,pid=1i,signal_queue=0i 1564441407000000000
uwsgi_workers,source=172.17.0.2,worker_id=1 accepting=1i,avg_rt=0i,delta_request=0i,exceptions=0i,harakiri_count=0i,last_spawn=1564441202i,pid=6i,requests=0i,respawn_count=1i,rss=0i,running_time=0i,signal_queue=0i,signals=0i,status="idle",tx=0i,vsz=0i 1564441407000000000
uwsgi_apps,app_id=0,worker_id=1,source=172.17.0.2 exceptions=0i,modifier1=0i,requests=0i,startup_time=0i 1564441407000000000
uwsgi_cores,core_id=0,worker_id=1,source=172.17.0.2 in_request=0i,offloaded_requests=0i,read_errors=0i,requests=0i,routed_requests=0i,static_requests=0i,write_errors=0i 1564441407000000000
```

