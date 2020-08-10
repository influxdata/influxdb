# Telegraf Plugin: nginx_plus_api

Nginx Plus is a commercial version of the open source web server Nginx. The use this plugin you will need a license. For more information about the differences between Nginx (F/OSS) and Nginx Plus, [click here](https://www.nginx.com/blog/whats-difference-nginx-foss-nginx-plus/).

### Configuration:

```
# Read Nginx Plus API advanced status information
[[inputs.nginx_plus_api]]
  ## An array of Nginx API URIs to gather stats.
  urls = ["http://localhost/api"]
  # Nginx API version, default: 3
  # api_version = 3
```

### Migration from Nginx Plus (Status) input plugin

| Nginx Plus                      | Nginx Plus API                       |
|---------------------------------|--------------------------------------|
| nginx_plus_processes            | nginx_plus_api_processes             |
| nginx_plus_connections          | nginx_plus_api_connections           |
| nginx_plus_ssl                  | nginx_plus_api_ssl                   |
| nginx_plus_requests             | nginx_plus_api_http_requests         |
| nginx_plus_zone                 | nginx_plus_api_http_server_zones     |
| nginx_plus_upstream             | nginx_plus_api_http_upstreams        |
| nginx_plus_upstream_peer        | nginx_plus_api_http_upstream_peers   |
| nginx_plus_cache                | nginx_plus_api_http_caches           |
| nginx_plus_stream_upstream      | nginx_plus_api_stream_upstreams      |
| nginx_plus_stream_upstream_peer | nginx_plus_api_stream_upstream_peers |
| nginx.stream.zone               | nginx_plus_api_stream_server_zones   |

### Measurements by API version

| Measurement                          | API version (api_version) |
|--------------------------------------|---------------------------|
| nginx_plus_api_processes             | >= 3                      |
| nginx_plus_api_connections           | >= 3                      |
| nginx_plus_api_ssl                   | >= 3                      |
| nginx_plus_api_http_requests         | >= 3                      |
| nginx_plus_api_http_server_zones     | >= 3                      |
| nginx_plus_api_http_upstreams        | >= 3                      |
| nginx_plus_api_http_upstream_peers   | >= 3                      |
| nginx_plus_api_http_caches           | >= 3                      |
| nginx_plus_api_stream_upstreams      | >= 3                      |
| nginx_plus_api_stream_upstream_peers | >= 3                      |
| nginx_plus_api_stream_server_zones   | >= 3                      |
| nginx_plus_api_http_location_zones   | >= 5                      |
| nginx_plus_api_resolver_zones        | >= 5                      |

### Measurements & Fields:

- nginx_plus_api_processes
  - respawned
- nginx_plus_api_connections
  - accepted
  - dropped
  - active
  - idle
- nginx_plus_api_ssl
  - handshakes
  - handshakes_failed
  - session_reuses
- nginx_plus_api_http_requests
  - total
  - current
- nginx_plus_api_http_server_zones
  - processing
  - requests
  - responses_1xx
  - responses_2xx
  - responses_3xx
  - responses_4xx
  - responses_5xx
  - responses_total
  - received
  - sent
  - discarded
- nginx_plus_api_http_upstreams
  - keepalive
  - zombies
- nginx_plus_api_http_upstream_peers
  - requests
  - unavail
  - healthchecks_checks
  - header_time
  - state
  - response_time
  - active
  - healthchecks_last_passed
  - weight
  - responses_1xx
  - responses_2xx
  - responses_3xx
  - responses_4xx
  - responses_5xx
  - received
  - healthchecks_fails
  - healthchecks_unhealthy
  - backup
  - responses_total
  - sent
  - fails
  - downtime
- nginx_plus_api_http_caches
  - size
  - max_size
  - cold
  - hit_responses
  - hit_bytes
  - stale_responses
  - stale_bytes
  - updating_responses
  - updating_bytes
  - revalidated_responses
  - revalidated_bytes
  - miss_responses
  - miss_bytes
  - miss_responses_written
  - miss_bytes_written
  - expired_responses
  - expired_bytes
  - expired_responses_written
  - expired_bytes_written
  - bypass_responses
  - bypass_bytes
  - bypass_responses_written
  - bypass_bytes_written
- nginx_plus_api_stream_upstreams
  - zombies
- nginx_plus_api_stream_upstream_peers
  - unavail
  - healthchecks_checks
  - healthchecks_fails
  - healthchecks_unhealthy
  - healthchecks_last_passed
  - response_time
  - state
  - active
  - weight
  - received
  - backup
  - sent
  - fails
  - downtime
- nginx_plus_api_stream_server_zones
  - processing
  - connections
  - received
  - sent
- nginx_plus_api_location_zones
  - requests
  - responses_1xx
  - responses_2xx
  - responses_3xx
  - responses_4xx
  - responses_5xx
  - responses_total
  - received
  - sent
  - discarded
- nginx_plus_api_resolver_zones
  - name
  - srv
  - addr
  - noerror
  - formerr
  - servfail
  - nxdomain
  - notimp
  - refused
  - timedout
  - unknown

### Tags:

- nginx_plus_api_processes, nginx_plus_api_connections, nginx_plus_api_ssl, nginx_plus_api_http_requests
  - source
  - port

- nginx_plus_api_http_upstreams, nginx_plus_api_stream_upstreams
  - upstream
  - source
  - port

- nginx_plus_api_http_server_zones, nginx_plus_api_upstream_server_zones, nginx_plus_api_http_location_zones, nginx_plus_api_resolver_zones
  - source
  - port
  - zone

- nginx_plus_api_upstream_peers, nginx_plus_api_stream_upstream_peers
  - id
  - upstream
  - source
  - port
  - upstream_address

- nginx_plus_api_http_caches
  - source
  - port

### Example Output:

Using this configuration:
```
[[inputs.nginx_plus_api]]
  ## An array of Nginx Plus API URIs to gather stats.
  urls = ["http://localhost/api"]
```

When run with:
```
./telegraf -config telegraf.conf -input-filter nginx_plus_api -test
```

It produces:
```
> nginx_plus_api_processes,port=80,source=demo.nginx.com respawned=0i 1570696321000000000
> nginx_plus_api_connections,port=80,source=demo.nginx.com accepted=68998606i,active=7i,dropped=0i,idle=57i 1570696322000000000
> nginx_plus_api_ssl,port=80,source=demo.nginx.com handshakes=9398978i,handshakes_failed=289353i,session_reuses=1004389i 1570696322000000000
> nginx_plus_api_http_requests,port=80,source=demo.nginx.com current=51i,total=264649353i 1570696322000000000
> nginx_plus_api_http_server_zones,port=80,source=demo.nginx.com,zone=hg.nginx.org discarded=5i,processing=0i,received=24123604i,requests=60138i,responses_1xx=0i,responses_2xx=59353i,responses_3xx=531i,responses_4xx=249i,responses_5xx=0i,responses_total=60133i,sent=830165221i 1570696322000000000
> nginx_plus_api_http_server_zones,port=80,source=demo.nginx.com,zone=trac.nginx.org discarded=250i,processing=0i,received=2184618i,requests=12404i,responses_1xx=0i,responses_2xx=8579i,responses_3xx=2513i,responses_4xx=583i,responses_5xx=479i,responses_total=12154i,sent=139384159i 1570696322000000000
> nginx_plus_api_http_server_zones,port=80,source=demo.nginx.com,zone=lxr.nginx.org discarded=1i,processing=0i,received=1011701i,requests=4523i,responses_1xx=0i,responses_2xx=4332i,responses_3xx=28i,responses_4xx=39i,responses_5xx=123i,responses_total=4522i,sent=72631354i 1570696322000000000
> nginx_plus_api_http_upstreams,port=80,source=demo.nginx.com,upstream=trac-backend keepalive=0i,zombies=0i 1570696322000000000
> nginx_plus_api_http_upstream_peers,id=0,port=80,source=demo.nginx.com,upstream=trac-backend,upstream_address=10.0.0.1:8080 active=0i,backup=false,downtime=0i,fails=0i,header_time=235i,healthchecks_checks=0i,healthchecks_fails=0i,healthchecks_unhealthy=0i,received=88581178i,requests=3180i,response_time=235i,responses_1xx=0i,responses_2xx=3168i,responses_3xx=5i,responses_4xx=6i,responses_5xx=0i,responses_total=3179i,sent=1321720i,state="up",unavail=0i,weight=1i 1570696322000000000
> nginx_plus_api_http_upstream_peers,id=1,port=80,source=demo.nginx.com,upstream=trac-backend,upstream_address=10.0.0.1:8081 active=0i,backup=true,downtime=0i,fails=0i,healthchecks_checks=0i,healthchecks_fails=0i,healthchecks_unhealthy=0i,received=0i,requests=0i,responses_1xx=0i,responses_2xx=0i,responses_3xx=0i,responses_4xx=0i,responses_5xx=0i,responses_total=0i,sent=0i,state="up",unavail=0i,weight=1i 1570696322000000000
> nginx_plus_api_http_upstreams,port=80,source=demo.nginx.com,upstream=hg-backend keepalive=0i,zombies=0i 1570696322000000000
> nginx_plus_api_http_upstream_peers,id=0,port=80,source=demo.nginx.com,upstream=hg-backend,upstream_address=10.0.0.1:8088 active=0i,backup=false,downtime=0i,fails=0i,header_time=22i,healthchecks_checks=0i,healthchecks_fails=0i,healthchecks_unhealthy=0i,received=909402572i,requests=18514i,response_time=88i,responses_1xx=0i,responses_2xx=17799i,responses_3xx=531i,responses_4xx=179i,responses_5xx=0i,responses_total=18509i,sent=10608107i,state="up",unavail=0i,weight=5i 1570696322000000000
> nginx_plus_api_http_upstream_peers,id=1,port=80,source=demo.nginx.com,upstream=hg-backend,upstream_address=10.0.0.1:8089 active=0i,backup=true,downtime=0i,fails=0i,healthchecks_checks=0i,healthchecks_fails=0i,healthchecks_unhealthy=0i,received=0i,requests=0i,responses_1xx=0i,responses_2xx=0i,responses_3xx=0i,responses_4xx=0i,responses_5xx=0i,responses_total=0i,sent=0i,state="up",unavail=0i,weight=1i 1570696322000000000
> nginx_plus_api_http_upstreams,port=80,source=demo.nginx.com,upstream=lxr-backend keepalive=0i,zombies=0i 1570696322000000000
> nginx_plus_api_http_upstream_peers,id=0,port=80,source=demo.nginx.com,upstream=lxr-backend,upstream_address=unix:/tmp/cgi.sock active=0i,backup=false,downtime=0i,fails=123i,header_time=91i,healthchecks_checks=0i,healthchecks_fails=0i,healthchecks_unhealthy=0i,received=71782888i,requests=4354i,response_time=91i,responses_1xx=0i,responses_2xx=4230i,responses_3xx=0i,responses_4xx=0i,responses_5xx=0i,responses_total=4230i,sent=3088656i,state="up",unavail=0i,weight=1i 1570696322000000000
> nginx_plus_api_http_upstream_peers,id=1,port=80,source=demo.nginx.com,upstream=lxr-backend,upstream_address=unix:/tmp/cgib.sock active=0i,backup=true,downtime=0i,fails=0i,healthchecks_checks=0i,healthchecks_fails=0i,healthchecks_unhealthy=0i,max_conns=42i,received=0i,requests=0i,responses_1xx=0i,responses_2xx=0i,responses_3xx=0i,responses_4xx=0i,responses_5xx=0i,responses_total=0i,sent=0i,state="up",unavail=0i,weight=1i 1570696322000000000
> nginx_plus_api_http_upstreams,port=80,source=demo.nginx.com,upstream=demo-backend keepalive=0i,zombies=0i 1570696322000000000
> nginx_plus_api_http_upstream_peers,id=0,port=80,source=demo.nginx.com,upstream=demo-backend,upstream_address=10.0.0.2:15431 active=0i,backup=false,downtime=0i,fails=0i,healthchecks_checks=0i,healthchecks_fails=0i,healthchecks_unhealthy=0i,received=0i,requests=0i,responses_1xx=0i,responses_2xx=0i,responses_3xx=0i,responses_4xx=0i,responses_5xx=0i,responses_total=0i,sent=0i,state="up",unavail=0i,weight=1i 1570696322000000000
> nginx_plus_api_http_caches,cache=http_cache,port=80,source=demo.nginx.com bypass_bytes=0i,bypass_bytes_written=0i,bypass_responses=0i,bypass_responses_written=0i,cold=false,expired_bytes=381518640i,expired_bytes_written=363449785i,expired_responses=42114i,expired_responses_written=39954i,hit_bytes=6321885979i,hit_responses=596730i,max_size=536870912i,miss_bytes=48512185i,miss_bytes_written=155600i,miss_responses=6052i,miss_responses_written=136i,revalidated_bytes=0i,revalidated_responses=0i,size=765952i,stale_bytes=0i,stale_responses=0i,updating_bytes=0i,updating_responses=0i 1570696323000000000
> nginx_plus_api_stream_server_zones,port=80,source=demo.nginx.com,zone=postgresql_loadbalancer connections=0i,processing=0i,received=0i,sent=0i 1570696323000000000
> nginx_plus_api_stream_server_zones,port=80,source=demo.nginx.com,zone=dns_loadbalancer connections=0i,processing=0i,received=0i,sent=0i 1570696323000000000
> nginx_plus_api_stream_upstreams,port=80,source=demo.nginx.com,upstream=postgresql_backends zombies=0i 1570696323000000000
> nginx_plus_api_stream_upstream_peers,id=0,port=80,source=demo.nginx.com,upstream=postgresql_backends,upstream_address=10.0.0.2:15432 active=0i,backup=false,connections=0i,downtime=0i,fails=0i,healthchecks_checks=0i,healthchecks_fails=0i,healthchecks_unhealthy=0i,received=0i,sent=0i,state="up",unavail=0i,weight=1i 1570696323000000000
> nginx_plus_api_stream_upstream_peers,id=1,port=80,source=demo.nginx.com,upstream=postgresql_backends,upstream_address=10.0.0.2:15433 active=0i,backup=false,connections=0i,downtime=0i,fails=0i,healthchecks_checks=0i,healthchecks_fails=0i,healthchecks_unhealthy=0i,received=0i,sent=0i,state="up",unavail=0i,weight=1i 1570696323000000000
> nginx_plus_api_stream_upstream_peers,id=2,port=80,source=demo.nginx.com,upstream=postgresql_backends,upstream_address=10.0.0.2:15434 active=0i,backup=false,connections=0i,downtime=0i,fails=0i,healthchecks_checks=0i,healthchecks_fails=0i,healthchecks_unhealthy=0i,received=0i,sent=0i,state="up",unavail=0i,weight=1i 1570696323000000000
> nginx_plus_api_stream_upstream_peers,id=3,port=80,source=demo.nginx.com,upstream=postgresql_backends,upstream_address=10.0.0.2:15435 active=0i,backup=false,connections=0i,downtime=0i,fails=0i,healthchecks_checks=0i,healthchecks_fails=0i,healthchecks_unhealthy=0i,received=0i,sent=0i,state="down",unavail=0i,weight=1i 1570696323000000000
> nginx_plus_api_stream_upstreams,port=80,source=demo.nginx.com,upstream=dns_udp_backends zombies=0i 1570696323000000000
> nginx_plus_api_stream_upstream_peers,id=0,port=80,source=demo.nginx.com,upstream=dns_udp_backends,upstream_address=10.0.0.5:53 active=0i,backup=false,connections=0i,downtime=0i,fails=0i,healthchecks_checks=0i,healthchecks_fails=0i,healthchecks_unhealthy=0i,received=0i,sent=0i,state="up",unavail=0i,weight=2i 1570696323000000000
> nginx_plus_api_stream_upstream_peers,id=1,port=80,source=demo.nginx.com,upstream=dns_udp_backends,upstream_address=10.0.0.2:53 active=0i,backup=false,connections=0i,downtime=0i,fails=0i,healthchecks_checks=0i,healthchecks_fails=0i,healthchecks_unhealthy=0i,received=0i,sent=0i,state="up",unavail=0i,weight=1i 1570696323000000000
> nginx_plus_api_stream_upstream_peers,id=2,port=80,source=demo.nginx.com,upstream=dns_udp_backends,upstream_address=10.0.0.7:53 active=0i,backup=false,connections=0i,downtime=0i,fails=0i,healthchecks_checks=0i,healthchecks_fails=0i,healthchecks_unhealthy=0i,received=0i,sent=0i,state="down",unavail=0i,weight=1i 1570696323000000000
> nginx_plus_api_stream_upstreams,port=80,source=demo.nginx.com,upstream=unused_tcp_backends zombies=0i 1570696323000000000
> nginx_plus_api_http_location_zones,port=80,source=demo.nginx.com,zone=swagger discarded=0i,received=1622i,requests=8i,responses_1xx=0i,responses_2xx=7i,responses_3xx=0i,responses_4xx=1i,responses_5xx=0i,responses_total=8i,sent=638333i 1570696323000000000
> nginx_plus_api_http_location_zones,port=80,source=demo.nginx.com,zone=api-calls discarded=64i,received=337530181i,requests=1726513i,responses_1xx=0i,responses_2xx=1726428i,responses_3xx=0i,responses_4xx=21i,responses_5xx=0i,responses_total=1726449i,sent=1902577668i 1570696323000000000
> nginx_plus_api_resolver_zones,port=80,source=demo.nginx.com,zone=resolver1 addr=0i,formerr=0i,name=0i,noerror=0i,notimp=0i,nxdomain=0i,refused=0i,servfail=0i,srv=0i,timedout=0i,unknown=0i 1570696324000000000
```

### Reference material

[api documentation](http://demo.nginx.com/swagger-ui/#/)
